"""``directed`` must survive every path that rebuilds an :class:`Edge`.

Losing it is not cosmetic: a merged edge that silently becomes directed then
picks up a synthesized inverse from ``AddInverseEdgesOp``, duplicating the very
relationship the undirected flag exists to keep single.
"""

from __future__ import annotations

import pytest

from graflo.architecture.evolution.merge_core import (
    EdgeMergeError,
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


class TestSingleValuedEdgesSurviveAMerge:
    """``merge`` / ``exclusive`` and parallel-edge keys are part of what an edge is."""

    def test_replace_and_exclusive_are_carried(self) -> None:
        a = Edge(source="m", target="l", relation="in", merge="replace", exclusive=True)
        b = Edge(source="m", target="l", relation="in", merge="replace", exclusive=True)

        merged = merge_edge_pair(a, b)

        assert (merged.merge, merged.exclusive) == ("replace", True)

    @pytest.mark.parametrize(
        "other",
        [
            pytest.param({}, id="union"),
            pytest.param({"merge": "replace"}, id="replace-not-exclusive"),
        ],
    )
    def test_a_different_merge_mode_is_refused(self, other: dict) -> None:
        a = Edge(source="m", target="l", relation="in", merge="replace", exclusive=True)
        b = Edge(source="m", target="l", relation="in", **other)

        with pytest.raises(EdgeMergeError, match="how many targets a source holds"):
            merge_edge_pair(a, b)

    def test_endpoint_redirect_refuses_a_merge_mode_clash(self) -> None:
        edges = [
            Edge(source="a", target="l", relation="in", merge="replace"),
            Edge(source="b", target="l", relation="in"),
        ]

        with pytest.raises(EdgeMergeError):
            redirect_and_merge_edges(edges, {"a": "m", "b": "m"})


class TestParallelEdgeKeys:
    def test_one_side_without_keys_takes_the_others(self) -> None:
        a = Edge(source="p", target="o", relation="r", identities=[["since"]])
        b = Edge(source="p", target="o", relation="r")

        assert merge_edge_pair(a, b).identities == [["since"]]
        assert merge_edge_pair(b, a).identities == [["since"]]

    def test_the_same_keys_are_kept_once(self) -> None:
        a = Edge(source="p", target="o", relation="r", identities=[["since"]])
        b = Edge(source="p", target="o", relation="r", identities=[["since"]])

        assert merge_edge_pair(a, b).identities == [["since"]]

    def test_different_keys_are_refused(self) -> None:
        a = Edge(source="p", target="o", relation="r", identities=[["since"]])
        b = Edge(source="p", target="o", relation="r", identities=[["role"]])

        with pytest.raises(EdgeMergeError, match="key parallel edges differently"):
            merge_edge_pair(a, b)
