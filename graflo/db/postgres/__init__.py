"""PostgreSQL database implementation.

This package provides PostgreSQL-specific implementations for schema introspection
and connection management. It focuses on reading and analyzing 3NF schemas to identify
vertex-like and edge-like tables, and inferring graflo Schema objects.

Key Components:
    - PostgresConnection: PostgreSQL connection and schema introspection implementation
    - PostgresSchemaInferencer: Infers graflo Schema from PostgreSQL schemas
    - PostgresResourceMapper: Maps PostgreSQL tables to graflo Resources

Example:
    >>> from graflo.db.postgres import PostgresConnection, infer_schema_from_postgres
    >>> from graflo.db.connection.onto import PostgresConfig
    >>> config = PostgresConfig.from_docker_env()
    >>> conn = PostgresConnection(config)
    >>> schema = infer_schema_from_postgres(conn, schema_name="public")
    >>> conn.close()
"""

from .conn import PostgresConnection
from .resource_mapping import PostgresResourceMapper
from .schema_inference import PostgresSchemaInferencer

__all__ = [
    "PostgresConnection",
    "PostgresSchemaInferencer",
    "PostgresResourceMapper",
    "infer_schema_from_postgres",
    "create_resources_from_postgres",
    "create_patterns_from_postgres",
]


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

    # Infer schema
    inferencer = PostgresSchemaInferencer(db_flavor=db_flavor)
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


def create_patterns_from_postgres(
    conn: PostgresConnection, schema_name: str | None = None
):
    """Create Patterns from PostgreSQL tables.

    Args:
        conn: PostgresConnection instance
        schema_name: Schema name to introspect

    Returns:
        Patterns: Patterns object with TablePattern instances for all tables
    """
    from graflo.util.onto import Patterns, TablePattern

    # Introspect the schema
    introspection_result = conn.introspect_schema(schema_name=schema_name)

    # Create patterns
    patterns = Patterns()

    # Get schema name
    effective_schema = schema_name or introspection_result.get("schema_name", "public")

    # Store the connection config
    config_key = "default"
    patterns.postgres_configs[(config_key, effective_schema)] = conn.config

    # Add patterns for vertex tables
    for table_info in introspection_result.get("vertex_tables", []):
        table_name = table_info["name"]
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
    for table_info in introspection_result.get("edge_tables", []):
        table_name = table_info["name"]
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
