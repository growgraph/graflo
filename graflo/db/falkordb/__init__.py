"""FalkorDB connection implementation for graph database operations.

This module implements the Connection interface for FalkorDB, providing
specific functionality for graph operations in FalkorDB. FalkorDB is a
Redis-based graph database that supports OpenCypher query language.

Key Features:
    - Label-based node organization (like Neo4j)
    - Relationship type management
    - Property indices
    - Cypher query execution
    - Batch node and relationship operations
    - Redis-based storage with graph namespacing

Example:
    >>> from graflo.db.falkordb import FalkordbConnection
    >>> from graflo.db import FalkordbConfig
    >>> config = FalkordbConfig(uri="redis://localhost:6379", database="mygraph")
    >>> conn = FalkordbConnection(config)
    >>> conn.init_db(schema, recreate_schema=True)
"""

from .conn import FalkordbConnection

__all__ = ["FalkordbConnection"]
