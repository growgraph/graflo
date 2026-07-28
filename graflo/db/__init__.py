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
    NamespaceNotFoundError,
    SchemaExistsError,
    consume_insert_edges_kwargs,
)
from .connection import (
    ArangoConfig,
    DBConfig,
    FalkordbConfig,
    MemgraphConfig,
    MinioConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    S3EndpointConfig,
    SparqlEndpointConfig,
    TigergraphConfig,
    parse_dotenv_file,
)
from .cypher import (
    cypher_map_key,
    cypher_string_literal,
    rel_merge_props_map_from_row_index,
    rel_merge_props_map_from_row_props,
)
from .falkordb.conn import FalkordbConnection
from .manager import ConnectionManager
from .memgraph.conn import MemgraphConnection
from .nebula.conn import NebulaConnection
from .neo4j.conn import Neo4jConnection
from .postgres.conn import PostgresConnection
from .tigergraph.conn import TigerGraphConnection

__all__ = [
    "ArangoConfig",
    "ArangoConnection",
    "Connection",
    "ConnectionManager",
    "ConnectionType",
    "DBConfig",
    "FalkordbConfig",
    "FalkordbConnection",
    "InsertEdgesKwArgs",
    "MemgraphConfig",
    "MemgraphConnection",
    "MinioConfig",
    "NamespaceNotFoundError",
    "NebulaConfig",
    "NebulaConnection",
    "Neo4jConfig",
    "Neo4jConnection",
    "PostgresConfig",
    "PostgresConnection",
    "S3EndpointConfig",
    "SchemaExistsError",
    "SparqlEndpointConfig",
    "TigerGraphConnection",
    "TigergraphConfig",
    "consume_insert_edges_kwargs",
    "cypher_map_key",
    "cypher_string_literal",
    "parse_dotenv_file",
    "rel_merge_props_map_from_row_index",
    "rel_merge_props_map_from_row_props",
]
