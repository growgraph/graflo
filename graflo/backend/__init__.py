"""Database connection and management components.

This package provides database connection implementations and management utilities
for different graph databases (ArangoDB, Neo4j, TigerGraph). It includes connection interfaces,
query execution, and database operations.

Key Components:
    - Connection: Abstract database connection interface
    - ConnectionManager: Database connection management
    - ArangoDB: ArangoDB-specific implementation
    - Neo4j: Neo4j-specific implementation
    - TigerGraph: TigerGraph-specific implementation
    - Query: Query generation and execution utilities

Example:
    >>> from graflo.backend import ConnectionManager
    >>> from graflo.backend.arango import ArangoConnection
    >>> manager = ConnectionManager(
    ...     connection_config={"url": "http://localhost:8529"},
    ...     conn_class=ArangoConnection
    ... )
    >>> with manager as conn:
    ...     conn.init_db(schema)
"""

from .arango.conn import ArangoConnection
from .conn import Connection, ConnectionType
from .connection import BackendType, DBConfig
from .manager import ConnectionManager
from .neo4j.conn import Neo4jConnection
from .tigergraph.conn import TigerGraphConnection

__all__ = [
    "Connection",
    "ConnectionType",
    "BackendType",
    "DBConfig",
    "ConnectionManager",
    "ArangoConnection",
    "Neo4jConnection",
    "TigerGraphConnection",
]
