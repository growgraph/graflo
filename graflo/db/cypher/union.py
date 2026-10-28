"""A Cypher node upsert that unions list fields with what the node holds.

The statement unwinds ``$data``, so one statement serves a chunk of documents on
Neo4j and FalkorDB alike.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from graflo.db.cypher.escape import cypher_name


def union_upsert_query(
    class_name: str, match_keys: Sequence[str], union_fields: Sequence[str]
) -> str:
    """Merge nodes of *class_name* on *match_keys*, unioning *union_fields*.

    Each row of ``$data`` carries the replaced properties under ``p`` and the
    union ones under ``u`` (:func:`union_upsert_rows`), so ``n += row.p`` can
    never overwrite a stored list. Members keep first-seen order, stored ones
    first; null members are dropped, since ``null IN acc`` is never true and
    they would pile up.
    """
    index_str = ", ".join(
        f"{cypher_name(k)}: row.p.{cypher_name(k)}" for k in match_keys
    )
    unions = ", ".join(
        f"n.{q} = reduce(acc = [], x IN [y IN "
        f"coalesce(n.{q}, []) + coalesce(row.u.{q}, []) "
        "WHERE y IS NOT NULL] | CASE WHEN x IN acc THEN acc ELSE acc + x END)"
        for q in (cypher_name(f, quoted=True) for f in union_fields)
    )
    return f"""
            UNWIND $data AS row
            MERGE (n:{cypher_name(class_name)} {{ {index_str} }})
            SET n += row.p
            SET {unions}
        """


def union_upsert_rows(
    docs: list[dict[str, Any]], union_fields: Sequence[str]
) -> list[dict[str, Any]]:
    """*docs* split into the ``p`` and ``u`` maps :func:`union_upsert_query` reads."""
    return [
        {
            "p": {k: v for k, v in doc.items() if k not in union_fields},
            "u": {k: doc[k] for k in union_fields if k in doc},
        }
        for doc in docs
    ]
