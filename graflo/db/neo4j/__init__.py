"""Neo4j database implementation.

This package provides Neo4j-specific implementations of the database interface,
including connection management, query execution, and utility functions.

Key Components:
    - Neo4jConnection: Neo4j connection implementation
    - Query: Cypher query execution and profiling
    - Util: Neo4j-specific utility functions

Example:
    >>> from graflo.db.neo4j import Neo4jConnection
    >>> conn = Neo4jConnection(config)
    >>> result = conn.execute("MATCH (n:User) RETURN n")
    >>> nodes = result.data()
"""
