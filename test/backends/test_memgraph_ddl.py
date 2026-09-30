"""The index statements the Memgraph connection sends."""

from __future__ import annotations

from graflo.db.memgraph.conn import edge_property_index_query


def test_an_edge_index_is_an_edge_type_index() -> None:
    assert (
        edge_property_index_query("REVIEWS", "rating")
        == "CREATE EDGE INDEX ON :REVIEWS(rating)"
    )
