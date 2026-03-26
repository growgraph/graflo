"""Relationship MERGE map fragments for parallel edges (property-graph backends)."""

from __future__ import annotations

from collections.abc import Sequence

from graflo.db.cypher.escape import cypher_map_key, cypher_string_literal


def _normalized_prop_names(prop_names: Sequence[str]) -> list[str]:
    keys: list[str] = []
    for raw in prop_names:
        k = raw.strip().replace("`", "")
        if k and k not in keys:
            keys.append(k)
    return keys


def rel_merge_props_map_from_row_index(
    prop_names: Sequence[str], *, row_index: int = 2
) -> str:
    """Build `` `k`: row[n]['k'], ... `` for MERGE relationship properties.

    Matches batches shaped as ``row`` = ``[source_doc, target_doc, props]`` (Neo4j,
    FalkorDB-style ``row[2]``).
    """
    row_access = f"row[{row_index}]"
    parts: list[str] = []
    for key in _normalized_prop_names(prop_names):
        bk = cypher_map_key(key)
        lit = cypher_string_literal(key)
        parts.append(f"{bk}: {row_access}[{lit}]")
    return ", ".join(parts)


def rel_merge_props_map_from_row_props(
    prop_names: Sequence[str], *, props_expr: str = "row.props"
) -> str:
    """Build `` `k`: row.props['k'], ... `` (Memgraph-style batch rows)."""
    parts: list[str] = []
    for key in _normalized_prop_names(prop_names):
        bk = cypher_map_key(key)
        lit = cypher_string_literal(key)
        parts.append(f"{bk}: {props_expr}[{lit}]")
    return ", ".join(parts)
