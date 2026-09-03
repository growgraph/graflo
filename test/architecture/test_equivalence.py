"""Tests for :mod:`graflo.architecture.evolution.equivalence`."""

from __future__ import annotations

import pytest

from graflo.architecture.evolution import (
    ClusterConflictError,
    ComposeManifestsOp,
    RelationEquivalence,
    VertexEquivalence,
    index_clusters,
)


def _index(op: ComposeManifestsOp, **names):
    return index_clusters(
        op,
        left_vertices=names.get("left_vertices", ()),
        right_vertices=names.get("right_vertices", ()),
        left_relations=names.get("left_relations", ()),
        right_relations=names.get("right_relations", ()),
    )


def test_bare_str_is_a_singleton_cluster() -> None:
    op = ComposeManifestsOp(
        vertices=[VertexEquivalence(left="Company", right="Org", into="Company")]
    )
    index = _index(op, left_vertices={"Company"}, right_vertices={"Org"})
    assert len(index.vertices) == 1
    cluster = index.vertices[0]
    assert cluster.left == ("Company",)
    assert cluster.right == ("Org",)
    assert cluster.into == "Company"


def test_nary_cluster_indexes_all_members() -> None:
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
            )
        ],
        allow_merges=True,
    )
    index = _index(
        op,
        left_vertices={"Company", "Shop"},
        right_vertices={"Org", "Branch"},
    )
    assert len(index.vertices) == 1
    cluster = index.vertices[0]
    assert cluster.left_names == frozenset({"Company", "Shop"})
    assert cluster.right_names == frozenset({"Org", "Branch"})
    assert index.labels == frozenset({"Company"})
    assert index.vertex_members("left") == frozenset({"Company", "Shop"})
    assert index.vertex_members("right") == frozenset({"Org", "Branch"})


def test_overlapping_declarations_raise() -> None:
    """The prompt's bug: two 'clusters' sharing a node with disagreeing into.

    {Company}~{Org} into X and {Company, Deal}~{Branch} into Y both claim
    left:Company -- this is the overlap the author must merge into one
    declaration, not two.
    """
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left="Company", right="Org", into="X"),
            VertexEquivalence(left=["Company", "Deal"], right="Branch", into="Y"),
        ],
        allow_merges=True,
    )
    with pytest.raises(ClusterConflictError, match="claimed"):
        _index(
            op,
            left_vertices={"Company", "Deal"},
            right_vertices={"Org", "Branch"},
        )


def test_shared_into_raises() -> None:
    """Two disjoint declarations must not share one `into` -- that collapses
    them into one composed class and must be spelled as one n-ary cluster."""
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left="A", right="X", into="Z"),
            VertexEquivalence(left="B", right="Y", into="Z"),
        ],
        allow_merges=True,
    )
    with pytest.raises(ClusterConflictError, match="into"):
        _index(
            op,
            left_vertices={"A", "B"},
            right_vertices={"X", "Y"},
        )


def test_occupied_into_raises() -> None:
    """`into` naming an existing non-member class on a side must not silently merge."""
    op = ComposeManifestsOp(
        vertices=[VertexEquivalence(left="A", right="B", into="Person")]
    )
    with pytest.raises(ClusterConflictError, match="not a member"):
        _index(
            op,
            left_vertices={"A", "Person"},
            right_vertices={"B"},
        )


def test_into_as_a_member_does_not_raise() -> None:
    """`into` naming an existing class that *is* a declared member is fine (a merge)."""
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left=["Company", "Shop"], right="Org", into="Company")
        ],
        allow_merges=True,
    )
    index = _index(
        op,
        left_vertices={"Company", "Shop"},
        right_vertices={"Org"},
    )
    assert len(index.vertices) == 1


def test_relations_share_the_same_checks() -> None:
    op = ComposeManifestsOp(
        relations=[
            RelationEquivalence(left=["signs", "owns"], right="has", into="signs")
        ],
        allow_merges=True,
    )
    index = _index(
        op,
        left_relations={"signs", "owns"},
        right_relations={"has"},
    )
    assert len(index.relations) == 1
    assert index.relation_labels() == frozenset({"signs"})
    assert index.relation_members("left") == frozenset({"signs", "owns"})


def test_relation_shared_into_raises() -> None:
    op = ComposeManifestsOp(
        relations=[
            RelationEquivalence(left="a", right="x", into="z"),
            RelationEquivalence(left="b", right="y", into="z"),
        ],
        allow_merges=True,
    )
    with pytest.raises(ClusterConflictError, match="into"):
        _index(op, left_relations={"a", "b"}, right_relations={"x", "y"})


def test_two_disjoint_clusters_are_independent() -> None:
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left="A", right="X", into="A"),
            VertexEquivalence(left="B", right="Y", into="B"),
        ]
    )
    index = _index(op, left_vertices={"A", "B"}, right_vertices={"X", "Y"})
    assert len(index.vertices) == 2
    assert index.labels == frozenset({"A", "B"})
