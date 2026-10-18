"""Build a :class:`DataSourceRegistry` from :class:`Bindings` and schema models.

Handles file discovery, SQL table source creation (with auto-JOIN
enrichment and datetime filtering), and connector dispatch by bound source kind.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from graflo.architecture.contract.bindings import (
    APIConnector,
    BoundSourceKind,
    FileConnector,
    KafkaConnector,
    PushConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.schema import Schema
from graflo.connections.provider import (
    ApiGeneralizedConnConfig,
    ConnectionProvider,
    EmptyConnectionProvider,
    KafkaGeneralizedConnConfig,
    PostgresGeneralizedConnConfig,
    SparqlGeneralizedConnConfig,
)
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.sql import datetime_range_filter

if TYPE_CHECKING:
    from graflo.architecture.contract.bindings import Bindings
    from graflo.connections.sources import RestApiConnConfig
    from graflo.data_source.api import APIConfig, APIDataSource, DependentAPIDataSource
    from graflo.hq.caster import IngestionParams

logger = logging.getLogger(__name__)


class RegistryBuilder:
    """Create a :class:`DataSourceRegistry` from :class:`Bindings`.

    Attributes:
        schema: Schema providing the resource definitions and vertex/edge config.
    """

    def __init__(self, schema: Schema, ingestion_model: IngestionModel):
        self.schema = schema
        self.ingestion_model = ingestion_model

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(
        self,
        bindings: Bindings,
        ingestion_params: IngestionParams,
        connection_provider: ConnectionProvider | None = None,
        *,
        strict: bool = False,
        pushed: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    ) -> DataSourceRegistry:
        """Return a populated :class:`DataSourceRegistry`.

        For each ingestion resource, registers every bound connector (same
        resource may have multiple physical sources).

        A source that cannot be built -- no connector bound, no connection
        configuration registered, a registration error -- is logged and
        skipped; with *strict* the build raises once every resource has been
        tried, listing them all.

        *pushed* holds the records for push connectors, keyed by connector
        name. A push connector with nothing pushed is a source that cannot be
        built.
        """
        registry = DataSourceRegistry()
        provider = connection_provider or EmptyConnectionProvider()
        failures: list[str] = []

        resources_filter: set[str] | None = None
        if ingestion_params.resources is not None:
            resources_filter = set(ingestion_params.resources)

        connectors_filter: set[str] | None = None
        if ingestion_params.connectors is not None:
            connectors_filter = bindings.resolve_connector_refs_to_hashes(
                ingestion_params.connectors
            )

        for resource in self.ingestion_model.resources:
            resource_name = resource.name
            if resources_filter is not None and resource_name not in resources_filter:
                continue
            connectors = bindings.get_connectors_for_resource(resource_name)
            if not connectors:
                msg = f"No connectors bound for resource '{resource_name}'"
                logger.warning("%s, skipping", msg)
                failures.append(msg)
                continue

            for connector in connectors:
                if (
                    connectors_filter is not None
                    and connector.hash not in connectors_filter
                ):
                    continue
                cref = connector.name or connector.hash
                kind = connector.bound_source_kind()

                if kind == BoundSourceKind.FILE:
                    if not isinstance(connector, FileConnector):
                        msg = (
                            f"Connector '{cref}' for resource '{resource_name}' "
                            f"is not a FileConnector"
                        )
                        logger.warning("%s, skipping", msg)
                        failures.append(msg)
                        continue
                    try:
                        self._register_file_sources(
                            registry, resource_name, connector, ingestion_params
                        )
                    except Exception as e:
                        msg = (
                            f"Failed to register FILE source for resource "
                            f"'{resource_name}' (connector '{cref}'): {e}"
                        )
                        if not strict:
                            logger.warning("%s, skipping", msg)
                        failures.append(msg)

                elif kind == BoundSourceKind.SQL_TABLE:
                    if not isinstance(connector, TableConnector):
                        msg = (
                            f"Connector '{cref}' for resource '{resource_name}' "
                            f"is not a TableConnector"
                        )
                        logger.warning("%s, skipping", msg)
                        failures.append(msg)
                        continue
                    try:
                        self._register_sql_table_sources(
                            registry,
                            resource_name,
                            connector,
                            bindings,
                            ingestion_params,
                            provider,
                        )
                    except Exception as e:
                        msg = (
                            f"Failed to register SQL source for resource "
                            f"'{resource_name}' (connector '{cref}'): {e}"
                        )
                        if not strict:
                            logger.warning("%s, skipping", msg)
                        failures.append(msg)

                elif kind == BoundSourceKind.SPARQL:
                    if not isinstance(connector, SparqlConnector):
                        msg = (
                            f"Connector '{cref}' for resource '{resource_name}' "
                            f"is not a SparqlConnector"
                        )
                        logger.warning("%s, skipping", msg)
                        failures.append(msg)
                        continue
                    try:
                        self._register_sparql_sources(
                            registry,
                            resource_name,
                            connector,
                            bindings,
                            ingestion_params,
                            provider,
                        )
                    except Exception as e:
                        msg = (
                            f"Failed to register SPARQL source for resource "
                            f"'{resource_name}' (connector '{cref}'): {e}"
                        )
                        if not strict:
                            logger.warning("%s, skipping", msg)
                        failures.append(msg)

                elif kind == BoundSourceKind.API:
                    if not isinstance(connector, APIConnector):
                        msg = (
                            f"Connector '{cref}' for resource '{resource_name}' "
                            f"is not an APIConnector"
                        )
                        logger.warning("%s, skipping", msg)
                        failures.append(msg)
                        continue
                    try:
                        self._register_api_sources(
                            registry,
                            resource_name,
                            connector,
                            bindings,
                            ingestion_params,
                            provider,
                        )
                    except Exception as e:
                        msg = (
                            f"Failed to register API source for resource "
                            f"'{resource_name}' (connector '{cref}'): {e}"
                        )
                        if not strict:
                            logger.warning("%s, skipping", msg)
                        failures.append(msg)

                elif kind == BoundSourceKind.KAFKA:
                    if not isinstance(connector, KafkaConnector):
                        msg = (
                            f"Connector '{cref}' for resource '{resource_name}' "
                            f"is not a KafkaConnector"
                        )
                        logger.warning("%s, skipping", msg)
                        failures.append(msg)
                        continue
                    try:
                        self._register_kafka_sources(
                            registry,
                            resource_name,
                            connector,
                            provider,
                        )
                    except Exception as e:
                        msg = (
                            f"Failed to register Kafka source for resource "
                            f"'{resource_name}' (connector '{cref}'): {e}"
                        )
                        if not strict:
                            logger.warning("%s, skipping", msg)
                        failures.append(msg)

                elif kind == BoundSourceKind.PUSH:
                    if not isinstance(connector, PushConnector):
                        msg = (
                            f"Connector '{cref}' for resource '{resource_name}' "
                            f"is not a PushConnector"
                        )
                        logger.warning("%s, skipping", msg)
                        failures.append(msg)
                        continue
                    records = (pushed or {}).get(connector.name)
                    if not records:
                        msg = (
                            f"Push connector '{cref}' for resource "
                            f"'{resource_name}': nothing was pushed"
                        )
                        if not strict:
                            logger.warning("%s, skipping", msg)
                        failures.append(msg)
                        continue
                    self._register_push_source(
                        registry, resource_name, connector, records
                    )

                else:
                    msg = (
                        f"Unsupported bound source kind '{kind}' "
                        f"for resource '{resource_name}' (connector '{cref}')"
                    )
                    logger.warning("%s, skipping", msg)
                    failures.append(msg)

        if strict and failures:
            details = "\n".join(f"- {item}" for item in failures)
            raise ValueError(f"Registry build failed in strict mode:\n{details}")

        return registry

    # ------------------------------------------------------------------
    # File sources
    # ------------------------------------------------------------------

    @staticmethod
    def discover_files(
        fpath: Path | str, connector: FileConnector, limit_files: int | None = None
    ) -> list[Path]:
        """Discover files matching *connector* in a directory.

        Args:
            fpath: Directory to search in.
            connector: Connector used to match files.
            limit_files: Optional cap on the number of files returned.

        Returns:
            Matching file paths.
        """
        if connector.sub_path is None:
            raise ValueError("connector.sub_path is required")
        path = Path(fpath) if isinstance(fpath, str) else fpath

        files = [
            f
            for f in path.iterdir()
            if f.is_file()
            and (
                True
                if connector.regex is None
                else re.search(connector.regex, f.name) is not None
            )
        ]

        if limit_files is not None:
            files = files[:limit_files]

        return files

    def _register_file_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        connector: FileConnector,
        ingestion_params: IngestionParams,
    ) -> None:
        if connector.sub_path is None:
            raise ValueError(
                f"FileConnector for resource '{resource_name}' has no sub_path"
            )

        path_obj = connector.sub_path.expanduser()
        files = self.discover_files(
            path_obj, limit_files=ingestion_params.limit_files, connector=connector
        )
        logger.info(f"For resource name {resource_name} {len(files)} files were found")

        encoding = self.ingestion_model.fetch_resource_config(resource_name).encoding
        for file_path in files:
            file_source = DataSourceFactory.create_file_data_source(
                path=file_path, encoding=encoding
            )
            registry.register(file_source, resource_name=resource_name)

    # ------------------------------------------------------------------
    # SQL / table sources
    # ------------------------------------------------------------------

    def _register_sql_table_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        connector: TableConnector,
        bindings: Bindings,
        ingestion_params: IngestionParams,
        connection_provider: ConnectionProvider,
    ) -> None:
        """Register SQL table data sources for a resource.

        Uses SQLDataSource with batch processing (cursors) instead of loading
        all data into memory.

        When the matching Resource has edge actors with ``match_source`` /
        ``match_target`` and the source/target vertex types have known
        table connectors, JoinClauses and IS_NOT_NULL filters are auto-generated
        on the connector before building the SQL query.
        """
        from graflo.hq.auto_join import enrich_edge_connector_with_joins

        generalized = (
            connection_provider.get_generalized_conn_config(connector)
            if hasattr(connection_provider, "get_generalized_conn_config")
            else None
        )
        postgres_config = (
            generalized.config
            if isinstance(generalized, PostgresGeneralizedConnConfig)
            else None
        )
        if postgres_config is None:
            # Legacy fallback: allow older ConnectionProvider implementations.
            postgres_config = connection_provider.get_postgres_config(
                resource_name, connector
            )
        if postgres_config is None:
            raise ValueError(
                f"no PostgreSQL connection configuration for table "
                f"'{connector.table_name}'"
            )

        table_name = connector.table_name
        schema_name = connector.schema_name
        effective_schema = schema_name or postgres_config.schema_name or "public"

        try:
            resource = self.ingestion_model.fetch_resource(resource_name)
            if connector.view is None and not connector.joins:
                enrich_edge_connector_with_joins(
                    resource=resource,
                    connector=connector,
                    bindings=bindings,
                    vertex_config=self.schema.core_schema.vertex_config,
                )

            # The run's date range is one more condition on the connector's
            # query: it applies whether the column comes from the connector or
            # from the run, beside any window the connector declares itself.
            date_column = connector.date_field or ingestion_params.datetime_column
            run_filters = []
            if ingestion_params.datetime_after or ingestion_params.datetime_before:
                if date_column:
                    date_range = datetime_range_filter(
                        ingestion_params.datetime_after,
                        ingestion_params.datetime_before,
                        date_column,
                    )
                    if date_range is not None:
                        run_filters.append(date_range)
                else:
                    logger.warning(
                        "datetime_after/datetime_before set but no date column: "
                        "set TableConnector.date_field or IngestionParams.datetime_column for resource %s",
                        resource_name,
                    )

            query = connector.build_query(effective_schema, extra_filters=run_filters)

            connection_string = postgres_config.to_sqlalchemy_connection_string()

            sql_config = SQLConfig(
                connection_string=connection_string,
                query=query,
            )
            sql_source = SQLDataSource(config=sql_config)

            registry.register(sql_source, resource_name=resource_name)

            logger.info(
                f"Created SQL data source for table '{effective_schema}.{table_name}' "
                f"mapped to resource '{resource_name}' "
                f"(will process in batches of {ingestion_params.batch_size})"
            )
        except Exception as e:
            logger.error(
                f"Failed to create data source for PostgreSQL table '{resource_name}': {e}",
                exc_info=True,
            )
            raise

    # ------------------------------------------------------------------
    # SPARQL / RDF sources
    # ------------------------------------------------------------------

    def _register_sparql_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        connector: SparqlConnector,
        bindings: Bindings,
        ingestion_params: IngestionParams,
        connection_provider: ConnectionProvider,
    ) -> None:
        """Register SPARQL data sources for a resource.

        Handles two modes:

        * **Endpoint mode** (``connector.endpoint_url`` is set): creates a
          :class:`SparqlEndpointDataSource` that queries the remote SPARQL
          endpoint.
        * **File mode** (``connector.rdf_file`` is set): creates an
          :class:`RdfFileDataSource` that parses a local RDF file.
        """
        if not connector.endpoint_url and not connector.rdf_file:
            raise ValueError("SparqlConnector has neither endpoint_url nor rdf_file")

        try:
            if connector.endpoint_url:
                from graflo.data_source.rdf import (
                    SparqlEndpointDataSource,
                    SparqlSourceConfig,
                )

                generalized = (
                    connection_provider.get_generalized_conn_config(connector)
                    if hasattr(connection_provider, "get_generalized_conn_config")
                    else None
                )
                if isinstance(generalized, SparqlGeneralizedConnConfig):
                    cfg = generalized.config
                    username = cfg.username
                    password = cfg.password
                else:
                    # Legacy fallback: allow older ConnectionProvider implementations.
                    sparql_auth = connection_provider.get_sparql_auth(
                        resource_name, connector
                    )
                    username = sparql_auth.username if sparql_auth else None
                    password = sparql_auth.password if sparql_auth else None

                source_config = SparqlSourceConfig(
                    endpoint_url=connector.endpoint_url,
                    rdf_class=connector.rdf_class,
                    graph_uri=connector.graph_uri,
                    sparql_query=connector.sparql_query,
                    username=username,
                    password=password,
                    page_size=ingestion_params.batch_size,
                )
                sparql_source = SparqlEndpointDataSource(
                    config=source_config,
                    same_as=connector.same_as,
                    typed_objects=connector.typed_objects,
                )
                registry.register(sparql_source, resource_name=resource_name)

                logger.info(
                    "Created SPARQL endpoint data source for class <%s> at '%s' "
                    "mapped to resource '%s'",
                    connector.rdf_class,
                    connector.endpoint_url,
                    resource_name,
                )

            elif connector.rdf_file:
                from graflo.data_source.rdf import RdfFileDataSource

                rdf_source = RdfFileDataSource(
                    path=connector.rdf_file,
                    rdf_class=connector.rdf_class,
                    same_as=connector.same_as,
                    typed_objects=connector.typed_objects,
                )
                registry.register(rdf_source, resource_name=resource_name)

                logger.info(
                    "Created RDF file data source for class <%s> from '%s' "
                    "mapped to resource '%s'",
                    connector.rdf_class,
                    connector.rdf_file,
                    resource_name,
                )

        except Exception as e:
            logger.error(
                "Failed to create data source for SPARQL resource '%s': %s",
                resource_name,
                e,
                exc_info=True,
            )
            raise

    @staticmethod
    def api_config(
        connector: APIConnector,
        connection_provider: ConnectionProvider,
        *,
        page_size_override: int | None = None,
    ) -> APIConfig:
        """Resolve *connector*'s connection and return its runtime request config.

        Raises:
            ValueError: No REST API connection is registered for the connector.
        """
        generalized = (
            connection_provider.get_generalized_conn_config(connector)
            if hasattr(connection_provider, "get_generalized_conn_config")
            else None
        )
        if not isinstance(generalized, ApiGeneralizedConnConfig):
            raise ValueError(
                "no REST API connection configuration is registered for the "
                "connector's conn_proxy"
            )
        runtime = generalized.config
        return connector.build_api_config(
            base_url=runtime.base_url,
            auth=runtime.auth,
            default_headers=runtime.default_headers,
            page_size_override=page_size_override,
        )

    #: Request settings a child connector takes from its parent unless it sets them.
    _INHERITED_REQUEST_SETTINGS = (
        "headers",
        "timeout",
        "retries",
        "retry_backoff_factor",
        "retry_status_forcelist",
        "verify",
    )

    @classmethod
    def api_source(
        cls,
        connector: APIConnector,
        bindings: Bindings,
        connection_provider: ConnectionProvider,
        *,
        page_size_override: int | None = None,
    ) -> APIDataSource | DependentAPIDataSource:
        """Return the data source that fetches *connector*'s records.

        A connector with a ``parent`` becomes one request per parent record. It
        uses its own ``conn_proxy`` connection when it has one, else its
        parent's, and takes the parent's request settings it does not set.

        Raises:
            ValueError: No REST API connection is registered for the connector
                or any of its parents.
        """
        from graflo.data_source.api import APIDataSource, DependentAPIDataSource

        parent = bindings.get_parent_connector(connector)
        if parent is None or connector.parent is None:
            return APIDataSource(
                config=cls.api_config(
                    connector,
                    connection_provider,
                    page_size_override=page_size_override,
                )
            )
        unset = [
            name
            for name in cls._INHERITED_REQUEST_SETTINGS
            if name not in connector.model_fields_set
        ]
        effective = connector.model_copy(
            update={name: getattr(parent, name) for name in unset}
        )
        runtime = cls._runtime_connection(connector, bindings, connection_provider)
        config = effective.build_api_config(
            base_url=runtime.base_url,
            auth=runtime.auth,
            default_headers=runtime.default_headers,
            page_size_override=page_size_override,
        )
        return DependentAPIDataSource(
            parent=cls.api_source(parent, bindings, connection_provider),
            config=config,
            fields=dict(connector.parent.fields),
        )

    @staticmethod
    def _runtime_connection(
        connector: APIConnector,
        bindings: Bindings,
        connection_provider: ConnectionProvider,
    ) -> RestApiConnConfig:
        """The REST connection of *connector*, else of its nearest parent with one."""
        current: APIConnector | None = connector
        while current is not None:
            generalized = (
                connection_provider.get_generalized_conn_config(current)
                if hasattr(connection_provider, "get_generalized_conn_config")
                else None
            )
            if isinstance(generalized, ApiGeneralizedConnConfig):
                return generalized.config
            current = bindings.get_parent_connector(current)
        raise ValueError(
            "no REST API connection configuration is registered for the "
            "connector's conn_proxy or any of its parents'"
        )

    def _register_api_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        connector: APIConnector,
        bindings: Bindings,
        ingestion_params: IngestionParams,
        connection_provider: ConnectionProvider,
    ) -> None:
        """Register REST API data sources for a resource."""
        # The connector's declared page size is what the endpoint accepts; the
        # global batch size replaces it only when the caller set one explicitly.
        page_size_override = (
            ingestion_params.batch_size
            if connector.pagination is not None
            and "batch_size" in ingestion_params.model_fields_set
            else None
        )
        api_source = self.api_source(
            connector,
            bindings,
            connection_provider,
            page_size_override=page_size_override,
        )
        registry.register(api_source, resource_name=resource_name)

        logger.info(
            "Created API data source for path '%s' at '%s' mapped to resource '%s'",
            connector.path,
            api_source.config.url,
            resource_name,
        )

    @staticmethod
    def _register_push_source(
        registry: DataSourceRegistry,
        resource_name: str,
        connector: PushConnector,
        records: Sequence[Mapping[str, Any]],
    ) -> None:
        """Register pushed records as an in-memory source for a resource."""
        from graflo.data_source.api_response import get_at_path
        from graflo.data_source.memory import InMemoryDataSource

        rows: list[dict] = []
        for record in records:
            if connector.records_path is None:
                items: list[Any] = [record]
            else:
                found = get_at_path(record, connector.records_path)
                items = found if isinstance(found, list) else []
            rows.extend(
                {**connector.row_annotations, **item}
                for item in items
                if isinstance(item, Mapping)
            )
        registry.register(InMemoryDataSource(data=rows), resource_name)

    def _register_kafka_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        connector: KafkaConnector,
        connection_provider: ConnectionProvider,
    ) -> None:
        """Register Kafka topic data sources for a resource."""
        generalized = (
            connection_provider.get_generalized_conn_config(connector)
            if hasattr(connection_provider, "get_generalized_conn_config")
            else None
        )
        if not isinstance(generalized, KafkaGeneralizedConnConfig):
            raise ValueError(
                "no Kafka connection configuration is registered for the "
                "connector's conn_proxy"
            )

        from graflo.data_source.kafka import KafkaDataSource

        kafka_config = connector.build_kafka_config(conn=generalized.config)
        kafka_source = KafkaDataSource(config=kafka_config)
        registry.register(kafka_source, resource_name=resource_name)

        logger.info(
            "Created Kafka data source for topics %s (group=%s) mapped to resource '%s'",
            connector.topics,
            connector.group_id,
            resource_name,
        )
