"""Connector-driven resource sampling.

Pulls a bounded number of documents from each resource behind a set of
connectors and returns them as a :class:`~graflo.architecture.onto_sample.SourceSample`
— pure JSON, with the originating connector recorded on every sample.

This is the input stage shared by every schema inferencer. ``infer_manifest``
performs it privately for PostgreSQL; exposing it lets an agentic or algorithmic
inferencer work from the same material, for any connector kind.

Sampling is deliberately *not* profiling: this module fetches documents and does
not describe them. See :func:`~graflo.architecture.onto_sample.profile_sample`
for the derived typed view.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from graflo.architecture.contract.bindings.connectors import (
    FileConnector,
    ResourceConnector,
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
from graflo.data_source.factory import DataSourceFactory
from graflo.hq.registry_builder import RegistryBuilder

logger = logging.getLogger(__name__)

#: Values longer than this are clipped and the sample marked ``truncated``.
DEFAULT_MAX_CELL_CHARS = 512


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
        for batch in data_source.iter_batches(batch_size=probe, limit=probe):
            docs.extend(batch)
            if len(docs) >= probe:
                break
        return self._normalize_docs(docs)

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------

    def sample_file(
        self,
        path: Path | str,
        *,
        resource_name: str | None = None,
        connector_name: str | None = None,
    ) -> ResourceSample:
        """Sample a single file, recording the connector that would read it."""
        path = Path(path)
        connector = FileConnector(
            name=connector_name or path.stem,
            regex=f"^{re.escape(path.name)}$",
            sub_path=path.parent,
        )
        data_source = DataSourceFactory.create_file_data_source(path=path)
        docs, truncated = self._read_data_source(data_source)
        return ResourceSample(
            resource_name=resource_name or path.stem,
            connector=connector.name,
            docs=docs,
            truncated=truncated,
            description=f"Sampled from file {path.name}",
        )

    def sample_files(
        self,
        paths_or_dir: Path | str | list[Path | str],
        *,
        source_name: str | None = None,
    ) -> SourceSample:
        """Sample every readable file in a directory, or an explicit file list.

        Files that cannot be read, or that yield no documents, are skipped with a
        warning rather than failing the whole sample — a source directory
        routinely holds READMEs and notes beside its data.
        """
        if isinstance(paths_or_dir, (str, Path)):
            root = Path(paths_or_dir)
            if root.is_dir():
                paths = sorted(p for p in root.iterdir() if p.is_file())
                default_name = root.name
            else:
                paths = [root]
                default_name = root.stem
        else:
            paths = [Path(p) for p in paths_or_dir]
            default_name = paths[0].parent.name if paths else "source"

        samples: list[ResourceSample] = []
        for path in paths:
            try:
                sample = self.sample_file(path)
            except (ValueError, OSError) as exc:
                logger.warning("Skipping unsampleable file '%s': %s", path, exc)
                continue
            if not sample.docs:
                logger.warning("Skipping file '%s': yielded no documents", path)
                continue
            samples.append(sample)

        if not samples:
            raise ValueError(f"No sampleable files found in {paths_or_dir}")

        return SourceSample(source_name=source_name or default_name, samples=samples)

    # ------------------------------------------------------------------
    # PostgreSQL
    # ------------------------------------------------------------------

    def sample_postgres(
        self,
        config: PostgresConfig,
        *,
        schema_name: str | None = None,
        tables: list[str] | None = None,
        source_name: str | None = None,
    ) -> SourceSample:
        """Sample PostgreSQL tables, carrying declared keys through.

        Primary and foreign keys come from schema introspection, not from column
        naming — they are ground truth for edge inference, so an inferencer never
        has to guess at ``*_id`` suffixes when a real constraint exists.
        """
        from graflo.db.postgres.conn import PostgresConnection
        from graflo.hq.sql_inferencer import SQLInferenceManager

        effective_schema = schema_name or config.schema_name or "public"
        samples: list[ResourceSample] = []

        with PostgresConnection(config) as conn:
            inferencer = SQLInferenceManager(
                conn=conn, target_db_flavor=self.postgres_target_flavor()
            )
            introspection = inferencer.introspect(
                schema_name=effective_schema, include_raw_tables=True
            )
            for table in introspection.raw_tables:
                if tables is not None and table.name not in tables:
                    continue
                connector = TableConnector(
                    name=table.name,
                    table_name=table.name,
                    schema_name=table.schema_name,
                )
                rows = conn.get_table_sample_rows(
                    table.name, schema_name=table.schema_name, limit=self.max_docs
                )
                docs, truncated = self._normalize_docs(rows)
                samples.append(
                    ResourceSample(
                        resource_name=table.name,
                        connector=connector.name,
                        docs=docs,
                        primary_key=list(table.primary_key),
                        foreign_keys=[
                            ForeignKeyHint(
                                field=fk.column,
                                references_resource=fk.references_table,
                                references_field=fk.references_column,
                            )
                            for fk in table.foreign_keys
                        ],
                        truncated=truncated,
                        total_estimate=table.row_count_estimate,
                        description=f"Sampled from table {table.schema_name}.{table.name}",
                    )
                )

        if not samples:
            raise ValueError(
                f"No tables found to sample in schema '{effective_schema}'"
            )

        return SourceSample(
            source_name=source_name or config.database or effective_schema,
            samples=samples,
        )

    @staticmethod
    def postgres_target_flavor() -> Any:
        """Target flavour used for introspection type mapping."""
        from graflo.connections.onto import DBType

        return DBType.ARANGO

    # ------------------------------------------------------------------
    # Bindings
    # ------------------------------------------------------------------

    def sample_connector(
        self,
        connector: ResourceConnector,
        *,
        resource_name: str,
        config: PostgresConfig | None = None,
    ) -> ResourceSample:
        """Sample one resource through *connector*."""
        if isinstance(connector, FileConnector):
            files = RegistryBuilder.discover_files(
                connector.sub_path.expanduser(), connector=connector, limit_files=1
            )
            if not files:
                raise ValueError(
                    f"FileConnector for resource '{resource_name}' matched no files "
                    f"under '{connector.sub_path}'"
                )
            sample = self.sample_file(
                files[0],
                resource_name=resource_name,
                connector_name=connector.name or resource_name,
            )
            return sample

        if isinstance(connector, TableConnector):
            if config is None:
                raise ValueError(
                    f"Sampling TableConnector for resource '{resource_name}' requires "
                    "a PostgresConfig"
                )
            from graflo.db.postgres.conn import PostgresConnection

            with PostgresConnection(config) as conn:
                rows = conn.get_table_sample_rows(
                    connector.table_name,
                    schema_name=connector.schema_name,
                    limit=self.max_docs,
                )
            docs, truncated = self._normalize_docs(rows)
            return ResourceSample(
                resource_name=resource_name,
                connector=connector.name or resource_name,
                docs=docs,
                truncated=truncated,
            )

        raise ValueError(
            f"Sampling is not implemented for {type(connector).__name__} "
            f"(resource '{resource_name}')"
        )

    def sample_bindings(
        self,
        bindings: Bindings,
        *,
        resources: list[str] | None = None,
        config: PostgresConfig | None = None,
        source_name: str = "bindings",
    ) -> SourceSample:
        """Sample every resource wired up in *bindings*.

        The ``resource_connector`` mapping is the authority on which connector
        feeds which resource, so the provenance recorded on each sample is the
        same relation ingestion will later use.
        """
        connectors_by_ref: dict[str, ResourceConnector] = {}
        for connector in bindings.connectors:
            if connector.name:
                connectors_by_ref[connector.name] = connector
            connectors_by_ref[connector.hash] = connector

        samples: list[ResourceSample] = []
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
                logger.warning(
                    "Skipping resource '%s': connector '%s' not found in bindings",
                    resource,
                    connector_ref,
                )
                continue
            try:
                samples.append(
                    self.sample_connector(
                        connector, resource_name=str(resource), config=config
                    )
                )
            except ValueError as exc:
                logger.warning("Skipping resource '%s': %s", resource, exc)

        if not samples:
            raise ValueError("No resources could be sampled from the given bindings")

        return SourceSample(source_name=source_name, samples=samples)


__all__ = ["DEFAULT_MAX_CELL_CHARS", "ResourceSampler"]
