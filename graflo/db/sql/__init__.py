"""Dialect-neutral SQL introspection and schema inference.

PostgreSQL was the only relational source GraFlo could infer a graph from, and
the logic that made it work — classifying tables as vertex-like or edge-like,
naming relations, recovering endpoints — never depended on PostgreSQL. Only the
*metadata acquisition* did.

This package draws that line. :class:`~graflo.db.sql.provider.SqlMetadataProvider`
is the seam: anything that can answer seven questions about a schema can be
introspected, whether it answers them from ``pg_catalog``, from SQLAlchemy
reflection, or from somewhere else entirely.
"""

from graflo.db.sql.introspect import (
    build_raw_tables,
    detect_edge_tables,
    detect_vertex_tables,
    introspect_schema,
    is_edge_like_table,
)
from graflo.db.sql.provider import SqlMetadataProvider

__all__ = [
    "SqlMetadataProvider",
    "build_raw_tables",
    "detect_edge_tables",
    "detect_vertex_tables",
    "introspect_schema",
    "is_edge_like_table",
]
