"""Read every relationship between two labels, whatever its endpoints' keys."""

from __future__ import annotations

from graflo.db.cypher.escape import cypher_name


def all_relationships_query(
    source_label: str,
    target_label: str,
    relation: str | None,
    *,
    limit: int | None = None,
) -> str:
    """Every ``relation`` relationship from a ``source_label`` to a ``target_label`` node.

    Returns the source node, the target node and the relationship, in that
    order, as entities; with no *relation*, relationships of every type.
    """
    rel = f":{cypher_name(relation)}" if relation else ""
    query = (
        f"MATCH (source:{cypher_name(source_label)})-[r{rel}]->"
        f"(target:{cypher_name(target_label)}) RETURN source, target, r"
    )
    return query if limit is None else f"{query} LIMIT {int(limit)}"
