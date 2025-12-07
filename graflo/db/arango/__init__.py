"""ArangoDB database implementation.

This package provides ArangoDB-specific implementations of the database interface,
including connection management, query execution, and utility functions.

Key Components:
    - ArangoConnection: ArangoDB connection implementation
    - Query: AQL query execution and profiling
    - Util: ArangoDB-specific utility functions

Example:
    >>> from graflo.db.arango import ArangoConnection
    >>> conn = ArangoConnection(config)
    >>> cursor = conn.execute("FOR doc IN users RETURN doc")
    >>> results = cursor.batch()
"""

from .conn import ArangoConnection

__all__ = [
    "ArangoConnection",
]
