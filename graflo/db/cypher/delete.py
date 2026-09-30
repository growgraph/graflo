"""Cypher statements that remove nodes and relationships by their keys.

Each statement unwinds ``$data``, a list of maps, so one statement serves a
chunk of documents on Neo4j, Memgraph and FalkorDB alike.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from graflo.db.conn import deletable_docs, deletable_endpoints
from graflo.db.cypher.escape import cypher_map_key


def _label(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def delete_nodes_query(label: str, match_keys: Sequence[str]) -> str:
    """Remove the nodes of *label* matching a row of ``$data``, with their relationships."""
    condition = " AND ".join(
        f"n.{cypher_map_key(key)} = row.k{i}" for i, key in enumerate(match_keys)
    )
    return f"UNWIND $data AS row MATCH (n:{_label(label)}) WHERE {condition} DETACH DELETE n"


def delete_relationships_query(
    source_label: str,
    target_label: str,
    relation: str | None,
    source_keys: Sequence[str],
    target_keys: Sequence[str],
) -> str:
    """Remove the relationships of *relation* between the node pairs of ``$data``.

    With no *relation*, every relationship from the source to the target goes.
    """
    rel = f"[r:{_label(relation)}]" if relation else "[r]"
    condition = " AND ".join(
        [f"s.{cypher_map_key(key)} = row.s{i}" for i, key in enumerate(source_keys)]
        + [f"t.{cypher_map_key(key)} = row.t{i}" for i, key in enumerate(target_keys)]
    )
    return (
        f"UNWIND $data AS row MATCH (s:{_label(source_label)})-{rel}->"
        f"(t:{_label(target_label)}) WHERE {condition} DELETE r"
    )


def delete_nodes(
    run: Callable[[str, list[dict[str, Any]]], Any],
    label: str,
    key_docs: list[dict[str, Any]],
    match_keys: Sequence[str],
    chunk_size: int,
) -> None:
    """Run :func:`delete_nodes_query` over *key_docs*, a chunk per statement."""
    rows = [
        {f"k{i}": doc[key] for i, key in enumerate(match_keys)}
        for doc in deletable_docs(key_docs, match_keys)
    ]
    if not rows:
        return
    query = delete_nodes_query(label, match_keys)
    for start in range(0, len(rows), chunk_size):
        run(query, rows[start : start + chunk_size])


def delete_relationships(
    run: Callable[[str, list[dict[str, Any]]], Any],
    source_label: str,
    target_label: str,
    relation: str | None,
    endpoints: list[tuple[dict[str, Any], dict[str, Any]]],
    source_keys: Sequence[str],
    target_keys: Sequence[str],
    chunk_size: int,
) -> None:
    """Run :func:`delete_relationships_query` over *endpoints*, a chunk per statement."""
    rows = [
        {
            **{f"s{i}": source[key] for i, key in enumerate(source_keys)},
            **{f"t{i}": target[key] for i, key in enumerate(target_keys)},
        }
        for source, target in deletable_endpoints(endpoints, source_keys, target_keys)
    ]
    if not rows:
        return
    query = delete_relationships_query(
        source_label, target_label, relation, source_keys, target_keys
    )
    for start in range(0, len(rows), chunk_size):
        run(query, rows[start : start + chunk_size])
