import logging

from graflo.util.onto import Patterns, TablePattern
from graflo.db.postgres.conn import (
    PostgresConnection,
)
from graflo.db.postgres.resource_mapping import PostgresResourceMapper
from graflo.db.postgres.schema_inference import PostgresSchemaInferencer

logger = logging.getLogger(__name__)


def create_patterns_from_postgres(
    conn: PostgresConnection, schema_name: str | None = None
) -> Patterns:
    """Create Patterns from PostgreSQL tables.

    Args:
        conn: PostgresConnection instance
        schema_name: Schema name to introspect

    Returns:
        Patterns: Patterns object with TablePattern instances for all tables
    """

    # Introspect the schema
    introspection_result = conn.introspect_schema(schema_name=schema_name)

    # Create patterns
    patterns = Patterns()

    # Get schema name
    effective_schema = schema_name or introspection_result.schema_name

    # Store the connection config
    config_key = "default"
    patterns.postgres_configs[(config_key, effective_schema)] = conn.config

    # Add patterns for vertex tables
    for table_info in introspection_result.vertex_tables:
        table_name = table_info.name
        table_pattern = TablePattern(
            table_name=table_name,
            schema_name=effective_schema,
            resource_name=table_name,
        )
        patterns.patterns[table_name] = table_pattern
        patterns.postgres_table_configs[table_name] = (
            config_key,
            effective_schema,
            table_name,
        )

    # Add patterns for edge tables
    for table_info in introspection_result.edge_tables:
        table_name = table_info.name
        table_pattern = TablePattern(
            table_name=table_name,
            schema_name=effective_schema,
            resource_name=table_name,
        )
        patterns.patterns[table_name] = table_pattern
        patterns.postgres_table_configs[table_name] = (
            config_key,
            effective_schema,
            table_name,
        )

    return patterns


def create_resources_from_postgres(
    conn: PostgresConnection, schema, schema_name: str | None = None
):
    """Create Resources from PostgreSQL tables for an existing schema.

    Args:
        conn: PostgresConnection instance
        schema: Existing Schema object
        schema_name: Schema name to introspect

    Returns:
        list[Resource]: List of Resources for PostgreSQL tables
    """
    # Introspect the schema
    introspection_result = conn.introspect_schema(schema_name=schema_name)

    # Map tables to resources
    mapper = PostgresResourceMapper()
    resources = mapper.map_tables_to_resources(
        introspection_result, schema.vertex_config, schema.edge_config
    )

    return resources


def infer_schema_from_postgres(
    conn: PostgresConnection, schema_name: str | None = None, db_flavor=None
):
    """Convenience function to infer a graflo Schema from PostgreSQL database.

    Args:
        conn: PostgresConnection instance
        schema_name: Schema name to introspect (defaults to config schema_name or 'public')
        db_flavor: Target database flavor (defaults to ARANGO)

    Returns:
        Schema: Inferred schema with vertices, edges, and resources
    """
    from graflo.onto import DBFlavor

    if db_flavor is None:
        db_flavor = DBFlavor.ARANGO

    # Introspect the schema
    introspection_result = conn.introspect_schema(schema_name=schema_name)

    # Infer schema (pass connection for type sampling)
    inferencer = PostgresSchemaInferencer(db_flavor=db_flavor, conn=conn)
    schema = inferencer.infer_schema(introspection_result, schema_name=schema_name)

    # Create and add resources
    mapper = PostgresResourceMapper()
    resources = mapper.map_tables_to_resources(
        introspection_result, schema.vertex_config, schema.edge_config
    )

    # Update schema with resources
    schema.resources = resources
    # Re-initialize to set up resource mappings
    schema.__post_init__()

    return schema
