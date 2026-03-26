"""Resource mapper for creating Bindings from different data sources.

This module provides functionality to create Bindings from various data sources
(PostgreSQL, files, etc.) that can be used for graph ingestion.
"""

import logging

from graflo.db import PostgresConnection
from graflo.filter.select import SelectSpec
from graflo.architecture.contract.bindings import Bindings, TableConnector
from graflo.hq.connection_provider import (
    InMemoryConnectionProvider,
    PostgresGeneralizedConnConfig,
)

logger = logging.getLogger(__name__)


class ResourceMapper:
    """Maps different data sources to Bindings for graph ingestion.

    This class provides methods to create Bindings from various data sources,
    enabling a unified interface for connector creation regardless of the source type.
    """

    def create_bindings_from_postgres(
        self,
        conn: PostgresConnection,
        schema_name: str | None = None,
        datetime_columns: dict[str, str] | None = None,
        type_lookup_overrides: dict[str, dict] | None = None,
        include_raw_tables: bool = False,
    ) -> Bindings:
        bindings, _ = self.create_bindings_with_provider_from_postgres(
            conn=conn,
            schema_name=schema_name,
            datetime_columns=datetime_columns,
            type_lookup_overrides=type_lookup_overrides,
            include_raw_tables=include_raw_tables,
        )
        return bindings

    def create_bindings_with_provider_from_postgres(
        self,
        conn: PostgresConnection,
        schema_name: str | None = None,
        datetime_columns: dict[str, str] | None = None,
        type_lookup_overrides: dict[str, dict] | None = None,
        include_raw_tables: bool = False,
    ) -> tuple[Bindings, InMemoryConnectionProvider]:
        """Create Bindings from PostgreSQL tables.

        Args:
            conn: PostgresConnection instance
            schema_name: Schema name to introspect
            datetime_columns: Optional mapping of resource/table name to datetime
                column name for date-range filtering (sets date_field on each
                TableConnector). Used with IngestionParams.datetime_after /
                datetime_before.
            type_lookup_overrides: Optional mapping of table name to type_lookup
                spec for edge tables where source/target types come from a lookup
                table. Each value is a dict with: table, identity, type_column,
                source, target, relation (optional).

        Returns:
            Tuple of:
                - Bindings object with TableConnector instances for all tables
                - InMemoryConnectionProvider containing connector->PostgresConfig mappings
        """
        # Introspect the schema
        introspection_result = conn.introspect_schema(
            schema_name=schema_name,
            include_raw_tables=include_raw_tables,
        )

        # Create bindings
        bindings = Bindings()

        # Get schema name
        effective_schema = schema_name or introspection_result.schema_name

        provider = InMemoryConnectionProvider()
        conn_proxy = "postgres_source"
        provider.register_generalized_config(
            conn_proxy=conn_proxy,
            config=PostgresGeneralizedConnConfig(config=conn.config),
        )

        date_cols = datetime_columns or {}
        type_lookup = type_lookup_overrides or {}

        # Add bindings for vertex tables
        for table_info in introspection_result.vertex_tables:
            table_name = table_info.name
            table_connector = TableConnector(
                table_name=table_name,
                schema_name=effective_schema,
                date_field=date_cols.get(table_name),
            )
            bindings.add_connector(table_connector)
            bindings.bind_resource(table_name, table_connector)
            bindings.bind_connector_to_conn_proxy(table_connector, conn_proxy)
            provider.bind_connector_to_conn_proxy(
                connector=table_connector, conn_proxy=conn_proxy
            )
            provider.postgres_by_resource[table_name] = conn.config

        # Add bindings for edge tables
        for table_info in introspection_result.edge_tables:
            table_name = table_info.name
            tl_spec = type_lookup.get(table_name)
            view = None
            if tl_spec:
                view = SelectSpec.from_dict({"kind": "type_lookup", **tl_spec})
            table_connector = TableConnector(
                table_name=table_name,
                schema_name=effective_schema,
                date_field=date_cols.get(table_name),
                view=view,
            )
            bindings.add_connector(table_connector)
            bindings.bind_resource(table_name, table_connector)
            bindings.bind_connector_to_conn_proxy(table_connector, conn_proxy)
            provider.bind_connector_to_conn_proxy(
                connector=table_connector, conn_proxy=conn_proxy
            )
            provider.postgres_by_resource[table_name] = conn.config

        return bindings, provider

    # Future methods can be added here for other resource types:
    # def create_bindings_from_files(...) -> Bindings:
    #     """Create Bindings from file sources."""
    #     ...
