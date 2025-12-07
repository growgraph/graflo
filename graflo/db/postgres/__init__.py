"""PostgreSQL database implementation.

This package provides PostgreSQL-specific implementations for schema introspection
and connection management. It focuses on reading and analyzing 3NF schemas to identify
vertex-like and edge-like tables.

Key Components:
    - PostgresConnection: PostgreSQL connection and schema introspection implementation

Example:
    >>> from graflo.db.postgres import PostgresConnection
    >>> from graflo.db.connection.onto import PostgresConfig
    >>> config = PostgresConfig.from_docker_env()
    >>> conn = PostgresConnection(config)
    >>> schema_info = conn.introspect_schema()
    >>> conn.close()
"""

from .conn import PostgresConnection

__all__ = [
    "PostgresConnection",
]
