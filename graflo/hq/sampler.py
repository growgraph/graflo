"""Connector-driven resource sampling.

Pulls a bounded number of documents from each resource behind a set of
connectors and returns them as a :class:`~graflo.architecture.onto_sample.SourceSample`
— pure JSON, with the originating connector recorded on every sample.

This is the input stage shared by every schema inferencer, for every connector
kind: a sample reads through bindings and a connection provider, exactly as
ingestion does, so what was sampled is what a model built from it will read.

Sampling is deliberately *not* profiling: this module fetches documents and does
not describe them. See :func:`~graflo.architecture.onto_sample.profile_sample`
for the derived typed view.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from graflo.architecture.contract.bindings.connectors import (
    APIConnector,
    FileConnector,
    KafkaConnector,
    PushConnector,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.architecture.contract.bindings.core import Bindings
from graflo.architecture.onto_sample import (
    DEFAULT_MAX_DOCS,
    ForeignKeyHint,
    ResourceSample,
    SourceSample,
)
from graflo.connections.onto import PostgresConfig
from graflo.connections.provider import ConnectionProvider, connector_config
from graflo.connections.sources import KafkaConnConfig
from graflo.data_source.base import AbstractDataSource
from graflo.hq.registry_builder import RegistryBuilder

logger = logging.getLogger(__name__)


#: Values longer than this are clipped and the sample marked ``truncated``.
DEFAULT_MAX_CELL_CHARS = 512

#: Longest a Kafka sample waits for messages, in milliseconds.
KAFKA_SAMPLE_WAIT_MS = 10_000


def _jsonable(value: Any, *, max_cell_chars: int) -> tuple[Any, bool]:
    """Coerce a fetched value to something JSON-serialisable.

    Database and columnar readers hand back ``datetime``, ``Decimal``,
    ``memoryview`` and numpy scalars. ``dict[str, Any]`` accepts them but only
    serialises them best-effort, so normalise at sample time rather than leaving
    it to whatever writes the JSON.

    Returns the coerced value and whether it was clipped.
    """
    if value is None or isinstance(value, (bool, int)):
        return value, False
    if isinstance(value, float):
        return value, False
    if isinstance(value, str):
        if len(value) > max_cell_chars:
            return value[:max_cell_chars], True
        return value, False
    if isinstance(value, (datetime, date, time)):
        return value.isoformat(), False
    if isinstance(value, Decimal):
        return float(value), False
    if isinstance(value, UUID):
        return str(value), False
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<{len(bytes(value))} bytes>", True
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        clipped = False
        for key, item in value.items():
            out[str(key)], item_clipped = _jsonable(item, max_cell_chars=max_cell_chars)
            clipped = clipped or item_clipped
        return out, clipped
    if isinstance(value, (list, tuple, set, frozenset)):
        items: list[Any] = []
        clipped = False
        for item in value:
            coerced, item_clipped = _jsonable(item, max_cell_chars=max_cell_chars)
            items.append(coerced)
            clipped = clipped or item_clipped
        return items, clipped
    # numpy scalars and anything else with a scalar view
    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return _jsonable(item_method(), max_cell_chars=max_cell_chars)
        except Exception:
            pass
    return str(value), False


def _declared_table_facts(
    conn: Any, table_name: str, schema_name: str | None
) -> dict[str, Any]:
    """What PostgreSQL declares about one table, as :class:`ResourceSample` fields.

    The table and column comments, the primary and foreign keys and the planner's
    row estimate -- the same facts :meth:`ResourceSampler.sample_postgres` reads
    by introspection, for a single table.
    """
    comment = next(
        (
            str(row.get("description") or "")
            for row in conn.get_tables(schema_name)
            if row.get("table_name") == table_name
        ),
        "",
    )
    return {
        "description": comment or None,
        "field_descriptions": {
            str(column["name"]): str(column["description"])
            for column in conn.get_table_columns(table_name, schema_name)
            if column.get("description")
        },
        "primary_key": list(conn.get_primary_keys(table_name, schema_name)),
        "foreign_keys": [
            ForeignKeyHint(
                field=str(fk["column"]),
                references_resource=str(fk["references_table"]),
                references_field=fk.get("references_column"),
            )
            for fk in conn.get_foreign_keys(table_name, schema_name)
        ],
        "total_estimate": conn.get_table_row_count_estimate(table_name, schema_name),
    }


class ResourceSampler:
    """Fetch bounded document samples from connectors.

    Args:
        max_docs: Cap on documents fetched per resource.
        max_cell_chars: Cap on the length of any single string value.
    """

    def __init__(
        self,
        *,
        max_docs: int = DEFAULT_MAX_DOCS,
        max_cell_chars: int = DEFAULT_MAX_CELL_CHARS,
    ) -> None:
        if max_docs < 1:
            raise ValueError("max_docs must be at least 1")
        self.max_docs = max_docs
        self.max_cell_chars = max_cell_chars

    # ------------------------------------------------------------------
    # Normalisation
    # ------------------------------------------------------------------

    def _normalize_docs(
        self, docs: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], bool]:
        normalized: list[dict[str, Any]] = []
        truncated = False
        for doc in docs[: self.max_docs]:
            coerced, clipped = _jsonable(doc, max_cell_chars=self.max_cell_chars)
            normalized.append(coerced)
            truncated = truncated or clipped
        if len(docs) > self.max_docs:
            truncated = True
        return normalized, truncated

    def _read_data_source(self, data_source: Any) -> tuple[list[dict[str, Any]], bool]:
        # Read one document past the cap so ``_normalize_docs`` can tell a source
        # that happened to hold exactly ``max_docs`` from one that was truncated.
        probe = self.max_docs + 1
        docs: list[dict[str, Any]] = []
        try:
            for batch in data_source.iter_batches(batch_size=probe, limit=probe):
                docs.extend(batch)
                if len(docs) >= probe:
                    break
        finally:
            # Closed without acknowledging: a sample must not move the position
            # an ingest reads from.
            data_source.close()
        return self._normalize_docs(docs)

    # ------------------------------------------------------------------
    # Connectors
    # ------------------------------------------------------------------

    def sample_connector(
        self,
        connector: ResourceConnector,
        *,
        resource_name: str,
        bindings: Bindings,
        connection_provider: ConnectionProvider,
    ) -> ResourceSample:
        """Sample one resource through *connector*.

        The connection comes from the connector's ``conn_proxy`` in *bindings*,
        resolved by *connection_provider*, as for ingestion. A file connector
        without one reads its local ``sub_path``; a SPARQL connector without
        one, its own ``endpoint_url`` or ``rdf_file``.
        """
        connector_name = connector.name or resource_name

        if isinstance(connector, TableConnector):
            config = connector_config(
                connector, bindings, connection_provider, PostgresConfig
            )
            from graflo.db.postgres.conn import PostgresConnection

            with PostgresConnection(config) as conn:
                rows = conn.get_table_sample_rows(
                    connector.table_name,
                    schema_name=connector.schema_name,
                    limit=self.max_docs,
                )
                declared = _declared_table_facts(
                    conn, connector.table_name, connector.schema_name
                )
            docs, truncated = self._normalize_docs(rows)
            return ResourceSample(
                resource_name=resource_name,
                connector=connector_name,
                docs=docs,
                truncated=truncated,
                **declared,
            )

        if isinstance(connector, PushConnector):
            raise ValueError(
                f"Resource '{resource_name}' takes records pushed, not fetched; "
                "there is nothing to sample"
            )

        source = self._source_of(
            connector,
            resource_name=resource_name,
            bindings=bindings,
            connection_provider=connection_provider,
        )
        docs, truncated = self._read_data_source(source)
        return ResourceSample(
            resource_name=resource_name,
            connector=connector_name,
            docs=docs,
            truncated=truncated,
        )

    def _source_of(
        self,
        connector: ResourceConnector,
        *,
        resource_name: str,
        bindings: Bindings,
        connection_provider: ConnectionProvider,
    ) -> AbstractDataSource:
        """The data source a sample of *connector* reads."""
        if isinstance(connector, FileConnector):
            sources = RegistryBuilder.file_sources(
                connector, bindings, connection_provider, limit_files=1
            )
            if not sources:
                raise ValueError(
                    f"FileConnector for resource '{resource_name}' matched no files "
                    f"under '{connector.sub_path}'"
                )
            return sources[0]

        if isinstance(connector, SparqlConnector):
            return RegistryBuilder.sparql_source(
                connector,
                bindings,
                resource_name=resource_name,
                page_size=self.max_docs + 1,
                connection_provider=connection_provider,
            )

        if isinstance(connector, APIConnector):
            try:
                return RegistryBuilder.api_source(
                    connector, bindings, connection_provider
                )
            except ValueError as exc:
                raise ValueError(
                    f"Sampling APIConnector for resource '{resource_name}' requires "
                    f"a REST API connection: {exc}"
                ) from exc

        if isinstance(connector, KafkaConnector):
            from graflo.data_source.kafka import KafkaDataSource

            kafka = connector_config(
                connector, bindings, connection_provider, KafkaConnConfig
            )
            config = connector.build_kafka_config(conn=kafka)
            # A consumer group of its own, so a sample never takes partitions
            # from a running consumer; nothing is acknowledged, so nothing commits.
            wait = min(config.max_wait_ms or KAFKA_SAMPLE_WAIT_MS, KAFKA_SAMPLE_WAIT_MS)
            return KafkaDataSource(
                config=config.model_copy(
                    update={
                        "group_id": f"graflo-sample-{uuid4().hex}",
                        "auto_offset_reset": "earliest",
                        "max_wait_ms": wait,
                    }
                )
            )

        raise ValueError(
            f"Sampling is not implemented for {type(connector).__name__} "
            f"(resource '{resource_name}')"
        )

    def sample_bindings(
        self,
        bindings: Bindings,
        *,
        connection_provider: ConnectionProvider,
        resources: list[str] | None = None,
        source_name: str = "bindings",
    ) -> SourceSample:
        """Sample every resource wired up in *bindings*.

        The ``resource_connector`` mapping is the authority on which connector
        feeds which resource, so the provenance recorded on each sample is the
        same relation ingestion will later use.

        Args:
            bindings: The connectors, their resource mapping and their
                ``conn_proxy`` wiring.
            connection_provider: Resolves each ``conn_proxy`` to its connection,
                the way ingestion does.
            resources: Restrict sampling to these resource names.
            source_name: Logical name recorded on the sample.

        Raises:
            ValueError: When no resource could be sampled; the message names
                each skipped resource and why.
        """
        connectors_by_ref: dict[str, ResourceConnector] = {}
        for connector in bindings.connectors:
            if connector.name:
                connectors_by_ref[connector.name] = connector
            connectors_by_ref[connector.hash] = connector

        samples: list[ResourceSample] = []
        skipped: list[str] = []
        for mapping in bindings.resource_connector:
            resource = (
                mapping.get("resource")
                if isinstance(mapping, dict)
                else mapping.resource
            )
            connector_ref = (
                mapping.get("connector")
                if isinstance(mapping, dict)
                else mapping.connector
            )
            if resources is not None and resource not in resources:
                continue
            connector = connectors_by_ref.get(str(connector_ref))
            if connector is None:
                reason = f"connector '{connector_ref}' not found in bindings"
                logger.warning("Skipping resource '%s': %s", resource, reason)
                skipped.append(f"{resource}: {reason}")
                continue
            try:
                samples.append(
                    self.sample_connector(
                        connector,
                        resource_name=str(resource),
                        bindings=bindings,
                        connection_provider=connection_provider,
                    )
                )
            except (ValueError, OSError) as exc:
                logger.warning("Skipping resource '%s': %s", resource, exc)
                skipped.append(f"{resource}: {exc}")

        if not samples:
            detail = "; ".join(skipped) or "the bindings map no resources"
            raise ValueError(
                f"No resources could be sampled from the given bindings ({detail})"
            )

        return SourceSample(source_name=source_name, samples=samples)


__all__ = ["DEFAULT_MAX_CELL_CHARS", "ResourceSampler"]
