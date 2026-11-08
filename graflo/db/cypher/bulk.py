"""Create-only Cypher for a bulk load, and the counts that verify it.

In a bulk load every row is new, so nodes and relationships are created with
no ``MERGE``: nothing is looked for but a relationship's two endpoints. Rows
are ``$rows``: node property maps, or ``[source keys, target keys, properties]``.
A ``null`` property is not stored, as an upsert's ``+=`` removes it.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from graflo.db.bulk import BulkCounts, StoredEdgeId
from graflo.db.cypher.escape import cypher_map_key, cypher_name, cypher_string_literal

#: Runs one read query and returns its records as dicts.
Run = Callable[[str], list[dict[str, Any]]]


def create_nodes_query(label: str) -> str:
    """Create one ``label`` node per row."""
    return f"UNWIND $rows AS row CREATE (n:{cypher_name(label)}) SET n = row"


def _key_map(keys: Sequence[str], row_index: int) -> str:
    return ", ".join(
        f"{cypher_map_key(k)}: row[{row_index}][{cypher_string_literal(k)}]"
        for k in keys
    )


def create_relationships_query(
    source_label: str,
    target_label: str,
    relation: str,
    match_keys_source: Sequence[str],
    match_keys_target: Sequence[str],
) -> str:
    """Create one relationship per row between endpoints found by their keys.

    A row whose endpoint is missing creates nothing; the load's count check
    reports it.
    """
    return (
        "UNWIND $rows AS row "
        f"MATCH (source:{cypher_name(source_label)} "
        f"{{{_key_map(match_keys_source, 0)}}}) "
        f"MATCH (target:{cypher_name(target_label)} "
        f"{{{_key_map(match_keys_target, 1)}}}) "
        f"CREATE (source)-[r:{cypher_name(relation)}]->(target) SET r = row[2]"
    )


def _single(rows: list[dict[str, Any]], key: str = "c") -> Any:
    return rows[0][key] if rows else 0


def bulk_counts(run: Run) -> BulkCounts:
    """Nodes per label and relationships per (source label, type, target label).

    A relationship type with one source and one target label is counted from
    per-label counts, which Neo4j answers from its count store; a type with
    several is counted by a scan of that type alone.
    """
    labels = [r["label"] for r in run("CALL db.labels() YIELD label RETURN label")]
    vertices = {
        label: int(_single(run(f"MATCH (n:{cypher_name(label)}) RETURN count(n) AS c")))
        for label in labels
    }
    types = [
        r["relationshipType"]
        for r in run(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
        )
    ]
    edges: dict[StoredEdgeId, int] = {}
    for rel in types:
        r = cypher_name(rel)
        sources = {
            label: int(
                _single(
                    run(
                        f"MATCH (:{cypher_name(label)})-[r:{r}]->() RETURN count(r) AS c"
                    )
                )
            )
            for label in labels
        }
        targets = {
            label: int(
                _single(
                    run(
                        f"MATCH ()-[r:{r}]->(:{cypher_name(label)}) RETURN count(r) AS c"
                    )
                )
            )
            for label in labels
        }
        sources = {k: v for k, v in sources.items() if v}
        targets = {k: v for k, v in targets.items() if v}
        if len(sources) == 1 and len(targets) == 1:
            ((source, count),) = sources.items()
            (target,) = targets
            edges[(source, rel, target)] = count
            continue
        for row in run(
            f"MATCH (s)-[r:{r}]->(t) "
            "RETURN head(labels(s)) AS s, head(labels(t)) AS t, count(r) AS c"
        ):
            edges[(row["s"], rel, row["t"])] = int(row["c"])
    return BulkCounts(vertices={k: v for k, v in vertices.items() if v}, edges=edges)


def scanned_counts(run: Run) -> BulkCounts:
    """Nodes per label and relationships per (source label, type, target label), by a scan.

    For a server without ``db.labels()`` or a count store; one pass over the
    nodes and one over the relationships.
    """
    vertices = {
        row["label"]: int(row["c"])
        for row in run("MATCH (n) RETURN head(labels(n)) AS label, count(n) AS c")
    }
    edges: dict[StoredEdgeId, int] = {
        (row["s"], row["r"], row["t"]): int(row["c"])
        for row in run(
            "MATCH (s)-[r]->(t) RETURN head(labels(s)) AS s, type(r) AS r, "
            "head(labels(t)) AS t, count(r) AS c"
        )
    }
    return BulkCounts(vertices=vertices, edges=edges)
