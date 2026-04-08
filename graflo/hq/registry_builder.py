"""Build a :class:`DataSourceRegistry` from :class:`Bindings` and schema models.

Handles file discovery, SQL table source creation (with auto-JOIN
enrichment and datetime filtering), and connector dispatch by bound source kind.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.schema import Schema
from graflo.data_source import DataSourceFactory, DataSourceRegistry
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.sql import datetime_range_where_sql
from graflo.hq.connection_provider import (
    ConnectionProvider,
    EmptyConnectionProvider,
    PostgresGeneralizedConnConfig,
    SparqlGeneralizedConnConfig,
)
from graflo.architecture.contract.bindings import (
    BoundSourceKind,
    FileConnector,
    SparqlConnector,
    TableConnector,
)

if TYPE_CHECKING:
    from graflo.hq.caster import IngestionParams
    from graflo.architecture.contract.bindings import Bindings

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
    ) -> DataSourceRegistry:
        """Return a populated :class:`DataSourceRegistry`.

        For each ingestion resource, registers every bound connector (same
        resource may have multiple physical sources).
        """
        registry = DataSourceRegistry()
        provider = connection_provider or EmptyConnectionProvider()
        failures: list[str] = []

        resources_filter: set[str] | None = None
        if ingestion_params.resources is not None:
            resources_filter = set(ingestion_params.resources)

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
                        failures.append(msg)
                        if strict:
                            continue

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
                        failures.append(msg)
                        if strict:
                            continue

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
                        failures.append(msg)
                        if strict:
                            continue

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

        for file_path in files:
            file_source = DataSourceFactory.create_file_data_source(path=file_path)
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
            logger.warning(
                f"PostgreSQL table '{resource_name}' has no connection config, skipping"
            )
            return

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

            date_column = connector.date_field or ingestion_params.datetime_column
            if (
                ingestion_params.datetime_after or ingestion_params.datetime_before
            ) and date_column:
                # Handled below via build_query + appended WHERE.
                pass
            elif ingestion_params.datetime_after or ingestion_params.datetime_before:
                logger.warning(
                    "datetime_after/datetime_before set but no date column: "
                    "set TableConnector.date_field or IngestionParams.datetime_column for resource %s",
                    resource_name,
                )

            query = connector.build_query(effective_schema)

            if date_column and date_column != connector.date_field:
                dt_where = datetime_range_where_sql(
                    ingestion_params.datetime_after,
                    ingestion_params.datetime_before,
                    date_column,
                )
                if dt_where:
                    if " WHERE " in query:
                        query += f" AND {dt_where}"
                    else:
                        query += f" WHERE {dt_where}"

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
        bindings: "Bindings",
        ingestion_params: "IngestionParams",
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
                sparql_source = SparqlEndpointDataSource(config=source_config)
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
                )
                registry.register(rdf_source, resource_name=resource_name)

                logger.info(
                    "Created RDF file data source for class <%s> from '%s' "
                    "mapped to resource '%s'",
                    connector.rdf_class,
                    connector.rdf_file,
                    resource_name,
                )

            else:
                logger.warning(
                    "SparqlConnector for resource '%s' has neither endpoint_url nor "
                    "rdf_file set, skipping",
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
