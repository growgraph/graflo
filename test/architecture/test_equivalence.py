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
        vertex_equivalences=[
            VertexEquivalence(left="Company", right="Org", into="Company")
        ]
    )
    index = _index(op, left_vertices={"Company"}, right_vertices={"Org"})
    assert len(index.vertices) == 1
    cluster = index.vertices[0]
    assert cluster.left == ("Company",)
    assert cluster.right == ("Org",)
    assert cluster.into == "Company"


def test_nary_cluster_indexes_all_members() -> None:
    op = ComposeManifestsOp(
        vertex_equivalences=[
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
    assert frozenset(cluster.left) == frozenset({"Company", "Shop"})
    assert frozenset(cluster.right) == frozenset({"Org", "Branch"})
    assert index.labels == frozenset({"Company"})
    assert index.cluster_for_label("Company") is cluster
    assert index.cluster_for_label("Org") is None
    assert index.vertex_members("left") == frozenset({"Company", "Shop"})
    assert index.vertex_members("right") == frozenset({"Org", "Branch"})


def test_overlapping_declarations_raise() -> None:
    """The prompt's bug: two 'clusters' sharing a node with disagreeing into.

    {Company}~{Org} into X and {Company, Deal}~{Branch} into Y both claim
    left:Company -- this is the overlap the author must merge into one
    declaration, not two.
    """
    op = ComposeManifestsOp(
        vertex_equivalences=[
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
        vertex_equivalences=[
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
        vertex_equivalences=[VertexEquivalence(left="A", right="B", into="Person")]
    )
    with pytest.raises(ClusterConflictError, match="not a member"):
        _index(
            op,
            left_vertices={"A", "Person"},
            right_vertices={"B"},
        )


def test_into_renamed_away_by_another_declaration_is_allowed() -> None:
    """A: {X}~{Y} -> Z while B: {Z}~{W} -> Q.

    Z exists on the left but B renames it away, and the lowered rename map
    applies in one step, so A's single-member side lands on a free name. The
    pre-fix check refused this and suggested adding Z to A, which the overlap
    check then refuses in turn.
    """
    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="X", right="Y", into="Z"),
            VertexEquivalence(left="Z", right="W", into="Q"),
        ]
    )
    index = _index(op, left_vertices={"X", "Z"}, right_vertices={"Y", "W"})
    assert index.labels == frozenset({"Z", "Q"})


def test_a_merge_into_a_name_another_declaration_renames_away_still_raises() -> None:
    """Merges are lowered before renames, so a merge would land on the old occupant."""
    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left=["X", "X2"], right="Y", into="Z"),
            VertexEquivalence(left="Z", right="W", into="Q"),
        ],
        allow_merges=True,
    )
    with pytest.raises(ClusterConflictError, match="lowered before renames"):
        _index(op, left_vertices={"X", "X2", "Z"}, right_vertices={"Y", "W"})


def test_relation_overlapping_declarations_raise() -> None:
    op = ComposeManifestsOp(
        relation_equivalences=[
            RelationEquivalence(left="a", right="x", into="p"),
            RelationEquivalence(left=["a", "b"], right="y", into="q"),
        ],
        allow_merges=True,
    )
    with pytest.raises(ClusterConflictError, match="claimed"):
        _index(op, left_relations={"a", "b"}, right_relations={"x", "y"})


def test_relation_occupied_into_raises() -> None:
    """The only check reading the side-name collections, on the relation branch."""
    op = ComposeManifestsOp(
        relation_equivalences=[RelationEquivalence(left="a", right="x", into="owns")]
    )
    with pytest.raises(ClusterConflictError, match="not a member"):
        _index(op, left_relations={"a", "owns"}, right_relations={"x"})


def test_relation_into_renamed_away_by_another_declaration_is_allowed() -> None:
    op = ComposeManifestsOp(
        relation_equivalences=[
            RelationEquivalence(left="a", right="x", into="owns"),
            RelationEquivalence(left="owns", right="y", into="holds"),
        ]
    )
    index = _index(op, left_relations={"a", "owns"}, right_relations={"x", "y"})
    assert index.relation_labels == frozenset({"owns", "holds"})


def test_into_as_a_member_does_not_raise() -> None:
    """`into` naming an existing class that *is* a declared member is fine (a merge)."""
    op = ComposeManifestsOp(
        vertex_equivalences=[
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
        relation_equivalences=[
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
    assert index.relation_labels == frozenset({"signs"})
    assert index.relation_members("left") == frozenset({"signs", "owns"})


def test_relation_shared_into_raises() -> None:
    op = ComposeManifestsOp(
        relation_equivalences=[
            RelationEquivalence(left="a", right="x", into="z"),
            RelationEquivalence(left="b", right="y", into="z"),
        ],
        allow_merges=True,
    )
    with pytest.raises(ClusterConflictError, match="into"):
        _index(op, left_relations={"a", "b"}, right_relations={"x", "y"})


def test_two_disjoint_clusters_are_independent() -> None:
    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="A", right="X", into="A"),
            VertexEquivalence(left="B", right="Y", into="B"),
        ]
    )
    index = _index(op, left_vertices={"A", "B"}, right_vertices={"X", "Y"})
    assert len(index.vertices) == 2
    assert index.labels == frozenset({"A", "B"})
