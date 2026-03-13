"""PostgreSQL database implementation.

This package provides PostgreSQL-specific implementations for schema introspection
and connection management. It focuses on reading and analyzing 3NF schemas to identify
vertex-like and edge-like tables, and inferring graflo Schema objects.

Key Components:
    - PostgresConnection: PostgreSQL connection and schema introspection implementation
    - PostgresSchemaInferencer: Infers graflo Schema from PostgreSQL schemas
    - PostgresResourceMapper: Maps PostgreSQL tables to graflo Resources

Example:
    >>> from graflo.hq import GraphEngine
    >>> from graflo.db import PostgresConfig
    >>> config = PostgresConfig.from_docker_env()
    >>> engine = GraphEngine()
    >>> manifest = engine.infer_manifest(config, schema_name="public")
"""

from .conn import PostgresConnection
from .resource_mapping import PostgresResourceMapper
from .schema_inference import PostgresSchemaInferencer

__all__ = [
    "PostgresConnection",
    "PostgresSchemaInferencer",
    "PostgresResourceMapper",
]
