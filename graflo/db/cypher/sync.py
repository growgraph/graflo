"""Cypher for record sync: ensure vertices, then replace what changed owners wrote.

:func:`owner_statements` turns an owner chunk into the statements one unit of
work runs, in order: replace the changed owners' properties (their hash left
unset), delete the edges they own, create their edges, store their hashes.
A backend runs them in one transaction, or, without one, in one batch with the
hash last, so that an owner whose batch failed half-way keeps no hash and is
rewritten by the next sync.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from graflo.db.cypher.bulk import create_relationships_query
from graflo.db.cypher.escape import cypher_map_key, cypher_name
from graflo.db.sync import SyncEdgeType, SyncOwnerChunk

#: A statement and its parameters.
Statement = tuple[str, dict[str, Any]]


def _match(keys: Sequence[str], row: str) -> str:
    return (
        "{"
        + ", ".join(f"{cypher_map_key(k)}: {row}.{cypher_name(k)}" for k in keys)
        + "}"
    )


def ensure_nodes_query(label: str, match_keys: Sequence[str]) -> str:
    """Create each row's node unless its key exists; an existing node is left as is."""
    return (
        f"UNWIND $rows AS row MERGE (n:{cypher_name(label)} {_match(match_keys, 'row')}) "
        "ON CREATE SET n = row"
    )


def stored_hashes_query(label: str, match_keys: Sequence[str], hash_field: str) -> str:
    """The stored hash of each row's node, by the row's ``i``; absent nodes return nothing."""
    return (
        f"UNWIND $rows AS row MATCH (n:{cypher_name(label)} "
        f"{_match(match_keys, 'row.key')}) "
        f"RETURN row.i AS i, n.{cypher_name(hash_field)} AS h"
    )


def replace_nodes_query(label: str, match_keys: Sequence[str]) -> str:
    """Replace every property of each row's node, creating it when absent."""
    return (
        f"UNWIND $rows AS row MERGE (n:{cypher_name(label)} {_match(match_keys, 'row')}) "
        "SET n = row"
    )


def set_hash_query(label: str, match_keys: Sequence[str], hash_field: str) -> str:
    """Store each row's hash ``h`` on its node."""
    return (
        f"UNWIND $rows AS row MATCH (n:{cypher_name(label)} "
        f"{_match(match_keys, 'row.key')}) SET n.{cypher_name(hash_field)} = row.h"
    )


def delete_owned_query(edge: SyncEdgeType, owner_keys: Sequence[str]) -> str:
    """Delete the ``edge`` relationships of the owners in ``$rows``.

    A row is the owner's key map, or for an edge owned by a property, the
    owner's identity value.
    """
    source = cypher_name(edge.source_class)
    target = cypher_name(edge.target_class)
    rel = cypher_name(edge.relation_name)
    if edge.side == "source":
        pattern = f"(n:{source} {_match(owner_keys, 'row')})-[r:{rel}]->(:{target})"
    elif edge.side == "target":
        pattern = f"(:{source})-[r:{rel}]->(n:{target} {_match(owner_keys, 'row')})"
    else:
        assert edge.owner_property is not None
        prop = cypher_name(edge.owner_property)
        return (
            f"UNWIND $rows AS row MATCH (:{source})-[r:{rel}]->(:{target}) "
            f"WHERE r.{prop} = row DELETE r"
        )
    return f"UNWIND $rows AS row MATCH {pattern} DELETE r"


def changed_owners(chunk: SyncOwnerChunk, stored: dict[int, Any]) -> list[int]:
    """Indexes of the owners whose hash differs from the stored one (*stored*: by index)."""
    return [
        i
        for i, owner in enumerate(chunk.owners)
        if stored.get(i) is None or stored[i] != owner.doc.get(chunk.hash_field)
    ]


def hash_rows(chunk: SyncOwnerChunk) -> list[dict[str, Any]]:
    """The rows :func:`stored_hashes_query` takes for every owner of *chunk*."""
    return [
        {"i": i, "key": {k: owner.doc[k] for k in chunk.match_keys}}
        for i, owner in enumerate(chunk.owners)
    ]


def owner_statements(chunk: SyncOwnerChunk, changed: Sequence[int]) -> list[Statement]:
    """The statements that rewrite the *changed* owners of *chunk*, hash last."""
    owners = [chunk.owners[i] for i in changed]
    if not owners:
        return []
    keys = chunk.match_keys
    label = chunk.class_name
    statements: list[Statement] = [
        (
            replace_nodes_query(label, keys),
            {
                "rows": [
                    {k: v for k, v in o.doc.items() if k != chunk.hash_field}
                    for o in owners
                ]
            },
        )
    ]
    key_rows = [{k: o.doc[k] for k in keys} for o in owners]
    for index, edge in enumerate(chunk.edge_types):
        rows: list[Any] = (
            [o.doc[keys[0]] for o in owners] if edge.side == "field" else key_rows
        )
        statements.append((delete_owned_query(edge, keys), {"rows": rows}))
        created = [list(row) for o in owners for row in o.edges[index]]
        if created:
            statements.append(
                (
                    create_relationships_query(
                        edge.source_class,
                        edge.target_class,
                        edge.relation_name,
                        edge.match_keys_source,
                        edge.match_keys_target,
                    ),
                    {"rows": created},
                )
            )
    statements.append(
        (
            set_hash_query(label, keys, chunk.hash_field),
            {
                "rows": [
                    {"key": row, "h": o.doc[chunk.hash_field]}
                    for row, o in zip(key_rows, owners)
                ]
            },
        )
    )
    return statements
