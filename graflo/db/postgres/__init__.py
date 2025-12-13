"""PostgreSQL database implementation.

This package provides PostgreSQL-specific implementations for schema introspection
and connection management. It focuses on reading and analyzing 3NF schemas to identify
vertex-like and edge-like tables, and inferring graflo Schema objects.

Key Components:
    - PostgresConnection: PostgreSQL connection and schema introspection implementation
    - PostgresSchemaInferencer: Infers graflo Schema from PostgreSQL schemas
    - PostgresResourceMapper: Maps PostgreSQL tables to graflo Resources

Example:
    >>> from graflo.db.postgres.heuristics import infer_schema_from_postgres    >>> from graflo.db.postgres import PostgresConnection
    >>> from graflo.db.connection.onto import PostgresConfig
    >>> config = PostgresConfig.from_docker_env()
    >>> conn = PostgresConnection(config)
    >>> schema = infer_schema_from_postgres(conn, schema_name="public")
    >>> conn.close()
"""

from .conn import PostgresConnection
from .heuristics import (
    create_patterns_from_postgres,
    create_resources_from_postgres,
    infer_schema_from_postgres,
)
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
