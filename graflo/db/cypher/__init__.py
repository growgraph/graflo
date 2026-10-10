"""Shared Cypher query fragments (no drivers; safe string builders only)."""

from graflo.db.cypher.direction import cypher_rel_pattern
from graflo.db.cypher.escape import cypher_map_key, cypher_string_literal
from graflo.db.cypher.rel_merge import (
    partition_by_absent_merge_props,
    rel_absent_upsert_clause,
    rel_merge_props_map_from_row_index,
    rel_merge_props_map_from_row_props,
)
from graflo.db.cypher.traversal import cypher_neighbors_query

__all__ = [
    "cypher_map_key",
    "cypher_neighbors_query",
    "cypher_rel_pattern",
    "cypher_string_literal",
    "partition_by_absent_merge_props",
    "rel_absent_upsert_clause",
    "rel_merge_props_map_from_row_index",
    "rel_merge_props_map_from_row_props",
]
