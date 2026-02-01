"""Memgraph connection implementation for graph database operations.

This module implements the Connection interface for Memgraph, providing
specific functionality for graph operations in Memgraph. Memgraph is a
high-performance, in-memory graph database that supports OpenCypher query language.

Key Features:
    - Label-based node organization (like Neo4j)
    - Relationship type management
    - Property indices
    - Cypher query execution
    - Batch node and relationship operations
    - In-memory storage with optional persistence

Example:
    >>> from graflo.db.memgraph import MemgraphConnection
    >>> from graflo.db.connection import MemgraphConfig
    >>> config = MemgraphConfig(uri="bolt://localhost:7687")
    >>> conn = MemgraphConnection(config)
    >>> conn.init_db(schema, recreate_schema=True)
"""

from .conn import MemgraphConnection

__all__ = ["MemgraphConnection"]
