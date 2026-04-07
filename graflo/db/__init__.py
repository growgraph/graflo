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
    >>> from graflo.db import ConnectionManager
    >>> from graflo.db.arango import ArangoConnection
    >>> manager = ConnectionManager(
    ...     connection_config={"url": "http://localhost:8529"},
    ...     conn_class=ArangoConnection
    ... )
    >>> with manager as conn:
    ...     conn.init_db(schema)
"""

from .arango.conn import ArangoConnection
from .conn import (
    Connection,
    ConnectionType,
    InsertEdgesKwArgs,
    consume_insert_edges_kwargs,
)
from .cypher import (
    cypher_map_key,
    cypher_string_literal,
    rel_merge_props_map_from_row_index,
    rel_merge_props_map_from_row_props,
)
from .connection import (
    ArangoConfig,
    DBConfig,
    FalkordbConfig,
    GrafeoConfig,
    MemgraphConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    SparqlEndpointConfig,
    TigergraphConfig,
)
from .falkordb.conn import FalkordbConnection
from .grafeo.conn import GrafeoConnection
from .manager import ConnectionManager
from .memgraph.conn import MemgraphConnection
from .nebula.conn import NebulaConnection
from .neo4j.conn import Neo4jConnection
from .postgres.conn import PostgresConnection
from .tigergraph.conn import TigerGraphConnection


__all__ = [
    "ArangoConfig",
    "Connection",
    "ConnectionType",
    "InsertEdgesKwArgs",
    "consume_insert_edges_kwargs",
    "cypher_map_key",
    "cypher_string_literal",
    "rel_merge_props_map_from_row_index",
    "rel_merge_props_map_from_row_props",
    "DBConfig",
    "ConnectionManager",
    "FalkordbConfig",
    "GrafeoConfig",
    "MemgraphConfig",
    "NebulaConfig",
    "Neo4jConfig",
    "PostgresConfig",
    "SparqlEndpointConfig",
    "TigergraphConfig",
    "ArangoConnection",
    "FalkordbConnection",
    "GrafeoConnection",
    "MemgraphConnection",
    "NebulaConnection",
    "Neo4jConnection",
    "PostgresConnection",
    "TigerGraphConnection",
]
