"""Build a :class:`DataSourceRegistry` from :class:`Patterns` and :class:`Schema`.

Handles file discovery, SQL table source creation (with auto-JOIN
enrichment and datetime filtering), and pattern dispatch by resource type.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from graflo.architecture.schema import Schema
from graflo.data_source import DataSourceFactory, DataSourceRegistry
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.sql import datetime_range_where_sql
from graflo.util.onto import FilePattern, ResourceType, SparqlPattern, TablePattern

if TYPE_CHECKING:
    from graflo.hq.caster import IngestionParams
    from graflo.util.onto import Patterns

logger = logging.getLogger(__name__)


class RegistryBuilder:
    """Create a :class:`DataSourceRegistry` from :class:`Patterns`.

    Attributes:
        schema: Schema providing the resource definitions and vertex/edge config.
    """

    def __init__(self, schema: Schema):
        self.schema = schema

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(
        self,
        patterns: Patterns,
        ingestion_params: IngestionParams,
    ) -> DataSourceRegistry:
        """Return a populated :class:`DataSourceRegistry`.

        Iterates over every resource in the schema, looks up its pattern and
        resource type, then delegates to the appropriate registration helper.
        """
        registry = DataSourceRegistry()

        for resource in self.schema.resources:
            resource_name = resource.name
            resource_type = patterns.get_resource_type(resource_name)

            if resource_type is None:
                logger.warning(
                    f"No resource type found for resource '{resource_name}', skipping"
                )
                continue

            pattern = patterns.patterns.get(resource_name)
            if pattern is None:
                logger.warning(
                    f"No pattern found for resource '{resource_name}', skipping"
                )
                continue

            if resource_type == ResourceType.FILE:
                if not isinstance(pattern, FilePattern):
                    logger.warning(
                        f"Pattern for resource '{resource_name}' is not a FilePattern, skipping"
                    )
                    continue
                self._register_file_sources(
                    registry, resource_name, pattern, ingestion_params
                )

            elif resource_type == ResourceType.SQL_TABLE:
                if not isinstance(pattern, TablePattern):
                    logger.warning(
                        f"Pattern for resource '{resource_name}' is not a TablePattern, skipping"
                    )
                    continue
                self._register_sql_table_sources(
                    registry, resource_name, pattern, patterns, ingestion_params
                )

            elif resource_type == ResourceType.SPARQL:
                if not isinstance(pattern, SparqlPattern):
                    logger.warning(
                        f"Pattern for resource '{resource_name}' is not a SparqlPattern, skipping"
                    )
                    continue
                self._register_sparql_sources(
                    registry, resource_name, pattern, patterns, ingestion_params
                )

            else:
                logger.warning(
                    f"Unsupported resource type '{resource_type}' for resource '{resource_name}', skipping"
                )

        return registry

    # ------------------------------------------------------------------
    # File sources
    # ------------------------------------------------------------------

    @staticmethod
    def discover_files(
        fpath: Path | str, pattern: FilePattern, limit_files: int | None = None
    ) -> list[Path]:
        """Discover files matching *pattern* in a directory.

        Args:
            fpath: Directory to search in.
            pattern: Pattern to match files against.
            limit_files: Optional cap on the number of files returned.

        Returns:
            Matching file paths.
        """
        assert pattern.sub_path is not None
        path = Path(fpath) if isinstance(fpath, str) else fpath

        files = [
            f
            for f in path.iterdir()
            if f.is_file()
            and (
                True
                if pattern.regex is None
                else re.search(pattern.regex, f.name) is not None
            )
        ]

        if limit_files is not None:
            files = files[:limit_files]

        return files

    def _register_file_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        pattern: FilePattern,
        ingestion_params: IngestionParams,
    ) -> None:
        if pattern.sub_path is None:
            logger.warning(
                f"FilePattern for resource '{resource_name}' has no sub_path, skipping"
            )
            return

        path_obj = pattern.sub_path.expanduser()
        files = self.discover_files(
            path_obj, limit_files=ingestion_params.limit_files, pattern=pattern
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
        pattern: TablePattern,
        patterns: Patterns,
        ingestion_params: IngestionParams,
    ) -> None:
        """Register SQL table data sources for a resource.

        Uses SQLDataSource with batch processing (cursors) instead of loading
        all data into memory.

        When the matching Resource has edge actors with ``match_source`` /
        ``match_target`` and the source/target vertex types have known
        TablePatterns, JoinClauses and IS_NOT_NULL filters are auto-generated
        on the pattern before building the SQL query.
        """
        from graflo.hq.auto_join import enrich_edge_pattern_with_joins

        postgres_config = patterns.get_postgres_config(resource_name)
        if postgres_config is None:
            logger.warning(
                f"PostgreSQL table '{resource_name}' has no connection config, skipping"
            )
            return

        table_info = patterns.get_table_info(resource_name)
        if table_info is None:
            logger.warning(
                f"Could not get table info for resource '{resource_name}', skipping"
            )
            return

        table_name, schema_name = table_info
        effective_schema = schema_name or postgres_config.schema_name or "public"

        try:
            resource = self.schema.fetch_resource(resource_name)
            if not pattern.joins:
                enrich_edge_pattern_with_joins(
                    resource=resource,
                    pattern=pattern,
                    patterns=patterns,
                    vertex_config=self.schema.vertex_config,
                )

            date_column = pattern.date_field or ingestion_params.datetime_column
            if (
                ingestion_params.datetime_after or ingestion_params.datetime_before
            ) and date_column:
                # Handled below via build_query + appended WHERE.
                pass
            elif ingestion_params.datetime_after or ingestion_params.datetime_before:
                logger.warning(
                    "datetime_after/datetime_before set but no date column: "
                    "set TablePattern.date_field or IngestionParams.datetime_column for resource %s",
                    resource_name,
                )

            query = pattern.build_query(effective_schema)

            if date_column and date_column != pattern.date_field:
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
                pagination=True,
                page_size=ingestion_params.batch_size,
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

    # ------------------------------------------------------------------
    # SPARQL / RDF sources
    # ------------------------------------------------------------------

    def _register_sparql_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        pattern: SparqlPattern,
        patterns: "Patterns",
        ingestion_params: "IngestionParams",
    ) -> None:
        """Register SPARQL data sources for a resource.

        Handles two modes:

        * **Endpoint mode** (``pattern.endpoint_url`` is set): creates a
          :class:`SparqlEndpointDataSource` that queries the remote SPARQL
          endpoint.
        * **File mode** (``pattern.rdf_file`` is set): creates an
          :class:`RdfFileDataSource` that parses a local RDF file.
        """
        try:
            if pattern.endpoint_url:
                from graflo.data_source.rdf import (
                    SparqlEndpointDataSource,
                    SparqlSourceConfig,
                )

                sparql_config = patterns.get_sparql_config(resource_name)
                username = (
                    getattr(sparql_config, "username", None) if sparql_config else None
                )
                password = (
                    getattr(sparql_config, "password", None) if sparql_config else None
                )

                source_config = SparqlSourceConfig(
                    endpoint_url=pattern.endpoint_url,
                    rdf_class=pattern.rdf_class,
                    graph_uri=pattern.graph_uri,
                    sparql_query=pattern.sparql_query,
                    username=username,
                    password=password,
                    page_size=ingestion_params.batch_size,
                )
                sparql_source = SparqlEndpointDataSource(config=source_config)
                registry.register(sparql_source, resource_name=resource_name)

                logger.info(
                    "Created SPARQL endpoint data source for class <%s> at '%s' "
                    "mapped to resource '%s'",
                    pattern.rdf_class,
                    pattern.endpoint_url,
                    resource_name,
                )

            elif pattern.rdf_file:
                from graflo.data_source.rdf import RdfFileDataSource

                rdf_source = RdfFileDataSource(
                    path=pattern.rdf_file,
                    rdf_class=pattern.rdf_class,
                )
                registry.register(rdf_source, resource_name=resource_name)

                logger.info(
                    "Created RDF file data source for class <%s> from '%s' "
                    "mapped to resource '%s'",
                    pattern.rdf_class,
                    pattern.rdf_file,
                    resource_name,
                )

            else:
                logger.warning(
                    "SparqlPattern for resource '%s' has neither endpoint_url nor "
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
