"""Resource mapper for creating Bindings from different data sources.

This module provides functionality to create Bindings from various data sources
(PostgreSQL, files, etc.) that can be used for graph ingestion.
"""

import logging

from graflo.db import PostgresConnection
from graflo.filter.select import SelectSpec
from graflo.architecture.contract.bindings import Bindings, TableConnector

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
    ) -> Bindings:
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
            Bindings: Bindings object with TableConnector instances for all tables
        """
        # Introspect the schema
        introspection_result = conn.introspect_schema(schema_name=schema_name)

        # Create bindings
        bindings = Bindings()

        # Get schema name
        effective_schema = schema_name or introspection_result.schema_name

        # Store the connection config
        config_key = "default"
        bindings.postgres_configs[(config_key, effective_schema)] = conn.config

        date_cols = datetime_columns or {}
        type_lookup = type_lookup_overrides or {}

        # Add bindings for vertex tables
        for table_info in introspection_result.vertex_tables:
            table_name = table_info.name
            table_connector = TableConnector(
                table_name=table_name,
                schema_name=effective_schema,
                resource_name=table_name,
                date_field=date_cols.get(table_name),
            )
            bindings.table_connectors[table_name] = table_connector
            bindings.postgres_table_configs[table_name] = (
                config_key,
                effective_schema,
                table_name,
            )

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
                resource_name=table_name,
                date_field=date_cols.get(table_name),
                view=view,
            )
            bindings.table_connectors[table_name] = table_connector
            bindings.postgres_table_configs[table_name] = (
                config_key,
                effective_schema,
                table_name,
            )

        return bindings

    # Future methods can be added here for other resource types:
    # def create_bindings_from_files(...) -> Bindings:
    #     """Create Bindings from file sources."""
    #     ...
