"""Database connection and management components.

This package provides database connection implementations and management utilities
for different graph databases (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph,
Nebula, PostgreSQL, and the on-disk GraFlo backend). It includes connection
interfaces, query execution, and database operations.

Connection *config* models live in :mod:`graflo.connections` — importable
without any DB driver. This façade is lazy (PEP 562): importing ``graflo.db``
does not load backend drivers; they load on first attribute access.

Example:
    >>> from graflo.db import ConnectionManager
    >>> from graflo.connections.onto import ArangoConfig
    >>> manager = ConnectionManager(connection_config=ArangoConfig(...))
    >>> with manager as conn:
    ...     conn.init_db(schema)
"""

from __future__ import annotations

from typing import Any

_EXPORTS: dict[str, str] = {
    "ArangoConnection": "graflo.db.arango.conn",
    "Connection": "graflo.db.conn",
    "ConnectionType": "graflo.db.conn",
    "InsertEdgesKwArgs": "graflo.db.conn",
    "NamespaceNotFoundError": "graflo.db.conn",
    "SchemaExistsError": "graflo.db.conn",
    "consume_insert_edges_kwargs": "graflo.db.conn",
    "cypher_map_key": "graflo.db.cypher",
    "cypher_string_literal": "graflo.db.cypher",
    "rel_merge_props_map_from_row_index": "graflo.db.cypher",
    "rel_merge_props_map_from_row_props": "graflo.db.cypher",
    "FalkordbConnection": "graflo.db.falkordb.conn",
    "ConnectionManager": "graflo.db.manager",
    "MemgraphConnection": "graflo.db.memgraph.conn",
    "NebulaConnection": "graflo.db.nebula.conn",
    "Neo4jConnection": "graflo.db.neo4j.conn",
    "PostgresConnection": "graflo.db.postgres.conn",
    "TigerGraphConnection": "graflo.db.tigergraph.conn",
    "GraFloBackendConnection": "graflo.db.graflo_backend.connection",
}

__all__ = [
    "ArangoConnection",
    "Connection",
    "ConnectionManager",
    "ConnectionType",
    "FalkordbConnection",
    "GraFloBackendConnection",
    "InsertEdgesKwArgs",
    "MemgraphConnection",
    "NamespaceNotFoundError",
    "NebulaConnection",
    "Neo4jConnection",
    "PostgresConnection",
    "SchemaExistsError",
    "TigerGraphConnection",
    "consume_insert_edges_kwargs",
    "cypher_map_key",
    "cypher_string_literal",
    "rel_merge_props_map_from_row_index",
    "rel_merge_props_map_from_row_props",
]


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        import importlib

        value = getattr(importlib.import_module(_EXPORTS[name]), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
