"""Reading every relationship between two labels, on the Cypher backends."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock

from graflo.db.cypher.scan import all_relationships_query
from graflo.db.memgraph.conn import MemgraphConnection


def test_the_scan_names_both_labels_and_the_type() -> None:
    assert all_relationships_query("Person", "City", "lives_in") == (
        "MATCH (source:Person)-[r:lives_in]->(target:City) RETURN source, target, r"
    )


def test_the_scan_without_a_type_reads_every_type_and_honours_a_limit() -> None:
    assert all_relationships_query("Person", "City", None, limit=3) == (
        "MATCH (source:Person)-[r]->(target:City) RETURN source, target, r LIMIT 3"
    )


def test_a_label_that_is_a_keyword_is_quoted() -> None:
    assert "(source:`MATCH`)" in all_relationships_query("MATCH", "City", "r")


def _memgraph(result_set: list[tuple[Any, ...]]) -> Any:
    conn = cast(Any, MemgraphConnection.__new__(MemgraphConnection))
    conn.execute = MagicMock(return_value=MagicMock(result_set=result_set))
    return conn


def test_memgraph_reads_every_edge_as_source_target_and_properties() -> None:
    conn = _memgraph([({"pid": "p1", "_id": 7}, {"cid": "c1"}, {"role": "home"})])

    rows = conn.fetch_all_edges("Person", "City", "lives_in")

    assert rows == [[{"pid": "p1"}, {"cid": "c1"}, {"role": "home"}]]
    conn.execute.assert_called_once_with(
        all_relationships_query("Person", "City", "lives_in")
    )


def test_memgraph_reads_every_node_of_a_label() -> None:
    conn = cast(Any, MemgraphConnection.__new__(MemgraphConnection))
    conn.fetch_docs = MagicMock(return_value=[{"pid": "p1", "labels": ["Person"]}])

    assert conn.fetch_all_docs("Person") == [{"pid": "p1"}]
    conn.fetch_docs.assert_called_once_with("Person", limit=None)
