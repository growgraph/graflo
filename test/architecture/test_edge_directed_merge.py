"""``directed`` must survive every path that rebuilds an :class:`Edge`.

Losing it is not cosmetic: a merged edge that silently becomes directed then
picks up a synthesized inverse from ``AddInverseEdgesOp``, duplicating the very
relationship the undirected flag exists to keep single.
"""

from __future__ import annotations

import pytest

from graflo.architecture.evolution.merge_core import (
    merge_edge_pair,
    redirect_and_merge_edges,
    remap_relation_and_merge_edges,
)
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Field


def test_merging_two_undirected_edges_stays_undirected() -> None:
    a = Edge(source="person", target="person", relation="knows", directed=False)
    b = Edge(
        source="person",
        target="person",
        relation="knows",
        directed=False,
        properties=[Field(name="since")],
    )
    assert merge_edge_pair(a, b).directed is False


def test_merging_two_directed_edges_stays_directed() -> None:
    a = Edge(source="person", target="company", relation="works_at")
    b = Edge(source="person", target="company", relation="works_at")
    assert merge_edge_pair(a, b).directed is True


@pytest.mark.parametrize("undirected_first", [True, False])
def test_undirected_wins_a_mixed_merge(undirected_first: bool) -> None:
    """The weaker assertion wins, in either argument order."""
    undirected = Edge(source="a", target="b", relation="r", directed=False)
    directed = Edge(source="a", target="b", relation="r", directed=True)
    pair = (undirected, directed) if undirected_first else (directed, undirected)
    assert merge_edge_pair(*pair).directed is False


def test_endpoint_redirect_collision_preserves_undirected() -> None:
    """``merge_vertices`` funnels two edges onto one id — the collision path."""
    edges = [
        Edge(source="person", target="staff", relation="knows", directed=False),
        Edge(source="person", target="employee", relation="knows", directed=False),
    ]
    merged = redirect_and_merge_edges(edges, {"staff": "worker", "employee": "worker"})
    assert len(merged) == 1
    assert merged[0].edge_id == ("person", "worker", "knows")
    assert merged[0].directed is False


def test_relation_remap_collision_preserves_undirected() -> None:
    """``MergeEdgesOp`` collapses relation names onto one canonical edge."""
    edges = [
        Edge(source="person", target="person", relation="knows", directed=False),
        Edge(source="person", target="person", relation="acquainted", directed=False),
    ]
    merged = remap_relation_and_merge_edges(edges, {"acquainted": "knows"})
    assert len(merged) == 1
    assert merged[0].directed is False
