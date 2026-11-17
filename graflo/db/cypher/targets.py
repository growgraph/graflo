"""The Cypher read behind ``fetch_edge_targets``: the stored targets of each source.

The statement unwinds ``$data``, a list of maps carrying a source's keys and
its position, so one statement serves a chunk of sources on Neo4j, Memgraph
and FalkorDB alike. Its columns are ``i`` (the position) and ``t0`` .. ``tN``
(the target's keys, in order).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Any

from graflo.db.conn import deletable_docs
from graflo.db.cypher.escape import cypher_map_key, cypher_name


def _label(name: str) -> str:
    return cypher_name(name, quoted=True)


def relationship_targets_query(
    source_label: str,
    target_label: str,
    relation: str | None,
    source_keys: Sequence[str],
    target_keys: Sequence[str],
) -> str:
    """The targets of the relationships of *relation* leaving each source of ``$data``.

    With no *relation*, every relationship from the source to the target.
    """
    rel = f"[r:{_label(relation)}]" if relation else "[r]"
    condition = " AND ".join(
        f"s.{cypher_map_key(key)} = row.s{i}" for i, key in enumerate(source_keys)
    )
    columns = ", ".join(
        f"t.{cypher_map_key(key)} AS t{i}" for i, key in enumerate(target_keys)
    )
    return (
        f"UNWIND $data AS row MATCH (s:{_label(source_label)})-{rel}->"
        f"(t:{_label(target_label)}) WHERE {condition} RETURN row.i AS i, {columns}"
    )


def relationship_targets(
    read: Callable[[str, list[dict[str, Any]]], Iterable[Sequence[Any]]],
    source_label: str,
    target_label: str,
    relation: str | None,
    sources: list[dict[str, Any]],
    source_keys: Sequence[str],
    target_keys: Sequence[str],
    chunk_size: int,
) -> dict[int, list[dict[str, Any]]]:
    """Run :func:`relationship_targets_query` over *sources*, a chunk per statement.

    *read* runs a statement with ``$data`` bound and returns its rows as
    sequences in column order.
    """
    rows = [
        {"i": position, **{f"s{i}": doc[key] for i, key in enumerate(source_keys)}}
        for position, doc in enumerate(sources)
        if deletable_docs([doc], source_keys)
    ]
    found: dict[int, list[dict[str, Any]]] = {}
    if not rows or not source_keys or not target_keys:
        return found
    query = relationship_targets_query(
        source_label, target_label, relation, source_keys, target_keys
    )
    for start in range(0, len(rows), chunk_size):
        for row in read(query, rows[start : start + chunk_size]):
            position, *values = row
            found.setdefault(int(position), []).append(
                dict(zip(target_keys, values, strict=True))
            )
    return found
