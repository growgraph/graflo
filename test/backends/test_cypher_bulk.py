"""Create-only Cypher for bulk loads, and the counts that verify them."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import Mock

from graflo.db.bulk import BulkEdgeChunk, BulkVertexChunk
from graflo.db.cypher.bulk import (
    bulk_counts,
    create_nodes_query,
    create_relationships_query,
)
from graflo.db.neo4j.conn import Neo4jConnection


def test_nodes_are_created_without_a_lookup() -> None:
    assert (
        create_nodes_query("Person")
        == "UNWIND $rows AS row CREATE (n:Person) SET n = row"
    )


def test_relationships_find_each_endpoint_by_its_keys() -> None:
    query = create_relationships_query(
        "Person", "City", "lives in", ["pid"], ["cid", "country"]
    )
    assert query == (
        "UNWIND $rows AS row "
        "MATCH (source:Person {`pid`: row[0]['pid']}) "
        "MATCH (target:City {`cid`: row[1]['cid'], `country`: row[1]['country']}) "
        "CREATE (source)-[r:`lives in`]->(target) SET r = row[2]"
    )


class _Graph:
    """Answers the count queries for a graph given as label and edge counts."""

    def __init__(self, nodes: dict[str, int], edges: dict[tuple, int]) -> None:
        self.nodes = nodes
        self.edges = edges
        self.scans: list[str] = []

    def __call__(self, query: str) -> list[dict[str, Any]]:
        if query.startswith("CALL db.labels()"):
            return [{"label": n} for n in self.nodes]
        if query.startswith("CALL db.relationshipTypes()"):
            return [{"relationshipType": r} for r in {e[1] for e in self.edges}]
        if query.startswith("MATCH (n:"):
            label = query.split(":")[1].split(")")[0]
            return [{"c": self.nodes[label]}]
        if query.startswith("MATCH (:"):
            label, rest = query[len("MATCH (:") :].split(")-[r:", 1)
            rel = rest.split("]")[0]
            return [
                {
                    "c": sum(
                        c
                        for (s, r, _), c in self.edges.items()
                        if (s, r) == (label, rel)
                    )
                }
            ]
        if query.startswith("MATCH ()-[r:"):
            rel, rest = query[len("MATCH ()-[r:") :].split("]->(:", 1)
            label = rest.split(")")[0]
            return [
                {
                    "c": sum(
                        c
                        for (_, r, t), c in self.edges.items()
                        if (r, t) == (rel, label)
                    )
                }
            ]
        self.scans.append(query)
        rel = query.split("[r:")[1].split("]")[0]
        return [
            {"s": s, "t": t, "c": c} for (s, r, t), c in self.edges.items() if r == rel
        ]


def test_counts_come_from_per_label_counts_unless_a_type_joins_several_pairs() -> None:
    graph = _Graph(
        {"Person": 3, "City": 2, "Firm": 1},
        {
            ("Person", "lives_in", "City"): 4,
            ("Person", "at", "City"): 1,
            ("Firm", "at", "City"): 2,
        },
    )

    counts = bulk_counts(graph)

    assert counts.vertices == {"Person": 3, "City": 2, "Firm": 1}
    assert counts.edges == graph.edges
    assert len(graph.scans) == 1 and "[r:at]" in graph.scans[0]


def _neo4j() -> Any:
    fake = Mock()
    fake.conn.execute_write.side_effect = lambda work: work(fake.tx)
    return fake


def test_a_vertex_chunk_is_created_and_a_replayed_one_merged() -> None:
    fake = _neo4j()
    chunk = BulkVertexChunk(
        chunk_id="v:0", class_name="Person", match_keys=["pid"], rows=[{"pid": "p1"}]
    )

    Neo4jConnection.bulk_write_vertices(fake, cast(Any, None), chunk)
    fake._write.assert_called_once_with(create_nodes_query("Person"), [{"pid": "p1"}])

    Neo4jConnection.bulk_write_vertices(
        fake, cast(Any, None), chunk.model_copy(update={"replay": True})
    )
    fake.upsert_docs_batch.assert_called_once_with([{"pid": "p1"}], "Person", ["pid"])
    fake._await_indexes.assert_called_once_with()


def test_a_replayed_edge_chunk_merges_on_its_identity() -> None:
    fake = _neo4j()
    chunk = BulkEdgeChunk(
        chunk_id="e:0",
        source_class="Person",
        target_class="City",
        relation_name="lives_in",
        match_keys_source=["pid"],
        match_keys_target=["cid"],
        collection_name=None,
        merge_properties=["role"],
        rows=[({"pid": "p1"}, {"cid": "c1"}, {"role": "home"})],
        replay=True,
    )

    Neo4jConnection.bulk_write_edges(fake, cast(Any, None), chunk)

    fake._write.assert_not_called()
    kwargs = fake.insert_edges_batch.call_args.kwargs
    assert kwargs == {"relationship_merge_properties": ["role"]}
