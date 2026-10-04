"""Group shape: overlap, shared and occupied names, and names vacated in the same step."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    MergeManifestsOp,
    RelationEquivalence,
    VertexEquivalence,
    resolve_clusters,
)
from graflo.architecture.evolution.naming_graph import MergeNamingError


def _side(
    name: str, vertices: list[str], relations: Sequence[str] = ()
) -> GraphManifest:
    """A schema-only manifest declaring *vertices*, and *relations* between two of its own."""
    hubs = [f"{name}_hub_a", f"{name}_hub_b"] if relations else []
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": v, "properties": ["id"], "identity": ["id"]}
                            for v in [*vertices, *hubs]
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": hubs[0], "target": hubs[1], "relation": r}
                            for r in relations
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _kinds(
    op: MergeManifestsOp, left: GraphManifest, right: GraphManifest
) -> list[str]:
    with pytest.raises(MergeNamingError) as excinfo:
        resolve_clusters(op, left=left, right=right)
    return [f.kind for f in excinfo.value.findings]


def test_bare_str_is_a_singleton_cluster() -> None:
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Company", right="Org", into="Company")
        ]
    )
    index = resolve_clusters(
        op, left=_side("l", ["Company"]), right=_side("r", ["Org"])
    ).index
    (cluster,) = index.vertices
    assert (cluster.left, cluster.right, cluster.into) == (
        ("Company",),
        ("Org",),
        "Company",
    )


def test_nary_cluster_indexes_all_members() -> None:
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
            )
        ],
    )
    index = resolve_clusters(
        op,
        left=_side("l", ["Company", "Shop"]),
        right=_side("r", ["Org", "Branch"]),
    ).index
    (cluster,) = index.vertices
    assert frozenset(cluster.left) == frozenset({"Company", "Shop"})
    assert frozenset(cluster.right) == frozenset({"Org", "Branch"})
    assert index.labels == frozenset({"Company"})
    assert index.cluster_for_label("Company") is cluster
    assert index.cluster_for_label("Org") is None
    assert index.vertex_members("left") == frozenset({"Company", "Shop"})
    assert index.vertex_members("right") == frozenset({"Org", "Branch"})


def test_overlapping_declarations_raise() -> None:
    """Two declarations claiming left:Company: one class lives in one equivalence."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Company", right="Org", into="X"),
            VertexEquivalence(left=["Company", "Deal"], right="Branch", into="Y"),
        ],
    )
    kinds = _kinds(op, _side("l", ["Company", "Deal"]), _side("r", ["Org", "Branch"]))
    assert "cluster_overlap" in kinds


def test_shared_into_raises() -> None:
    """Two unlinked groups must not arrive at one name."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="A", right="X", into="Z"),
            VertexEquivalence(left="B", right="Y", into="Z"),
        ],
    )
    assert _kinds(op, _side("l", ["A", "B"]), _side("r", ["X", "Y"])) == ["shared_into"]


def test_occupied_into_raises() -> None:
    """`into` naming an existing non-member class on a side must not silently merge."""
    op = MergeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="A", right="B", into="Person")]
    )
    assert _kinds(op, _side("l", ["A", "Person"]), _side("r", ["B"])) == [
        "occupied_into"
    ]


def test_into_renamed_away_by_another_declaration_is_allowed() -> None:
    """A: {X}~{Y} -> Z while B: {Z}~{W} -> Q: the relabel applies in one step."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="X", right="Y", into="Z"),
            VertexEquivalence(left="Z", right="W", into="Q"),
        ]
    )
    index = resolve_clusters(
        op, left=_side("l", ["X", "Z"]), right=_side("r", ["Y", "W"])
    ).index
    assert index.labels == frozenset({"Z", "Q"})


def test_a_merge_into_a_name_another_declaration_renames_away_is_allowed() -> None:
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left=["X", "X2"], right="Y", into="Z"),
            VertexEquivalence(left="Z", right="W", into="Q"),
        ],
    )
    index = resolve_clusters(
        op, left=_side("l", ["X", "X2", "Z"]), right=_side("r", ["Y", "W"])
    ).index
    assert index.labels == frozenset({"Z", "Q"})


def test_into_renamed_away_by_renames_is_allowed() -> None:
    op = MergeManifestsOp.model_validate(
        {
            "vertex_equivalences": [{"left": "A", "right": "B", "into": "Person"}],
            "renames": {"left": {"vertices": {"Person": "Individual"}}},
        }
    )
    resolution = resolve_clusters(
        op, left=_side("l", ["A", "Person"]), right=_side("r", ["B"])
    )
    assert resolution.side_maps.left.vertices == {
        "A": "Person",
        "Person": "Individual",
    }


def test_relation_overlapping_declarations_raise() -> None:
    op = MergeManifestsOp(
        relation_equivalences=[
            RelationEquivalence(left="a", right="x", into="p"),
            RelationEquivalence(left=["a", "b"], right="y", into="q"),
        ],
    )
    kinds = _kinds(op, _side("l", [], ["a", "b"]), _side("r", [], ["x", "y"]))
    assert "cluster_overlap" in kinds


def test_relation_occupied_into_raises() -> None:
    op = MergeManifestsOp(
        relation_equivalences=[RelationEquivalence(left="a", right="x", into="owns")]
    )
    assert _kinds(op, _side("l", [], ["a", "owns"]), _side("r", [], ["x"])) == [
        "occupied_into"
    ]


def test_relation_into_renamed_away_by_another_declaration_is_allowed() -> None:
    op = MergeManifestsOp(
        relation_equivalences=[
            RelationEquivalence(left="a", right="x", into="owns"),
            RelationEquivalence(left="owns", right="y", into="holds"),
        ]
    )
    index = resolve_clusters(
        op, left=_side("l", [], ["a", "owns"]), right=_side("r", [], ["x", "y"])
    ).index
    assert index.relation_labels == frozenset({"owns", "holds"})


def test_into_as_a_member_does_not_raise() -> None:
    """`into` naming an existing class that *is* a declared member is a merge."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left=["Company", "Shop"], right="Org", into="Company")
        ],
    )
    index = resolve_clusters(
        op, left=_side("l", ["Company", "Shop"]), right=_side("r", ["Org"])
    ).index
    assert len(index.vertices) == 1


def test_relations_share_the_same_checks() -> None:
    op = MergeManifestsOp(
        relation_equivalences=[
            RelationEquivalence(left=["signs", "owns"], right="has", into="signs")
        ],
    )
    index = resolve_clusters(
        op, left=_side("l", [], ["signs", "owns"]), right=_side("r", [], ["has"])
    ).index
    assert len(index.relations) == 1
    assert index.relation_labels == frozenset({"signs"})
    assert index.relation_members("left") == frozenset({"signs", "owns"})


def test_relation_shared_into_raises() -> None:
    op = MergeManifestsOp(
        relation_equivalences=[
            RelationEquivalence(left="a", right="x", into="z"),
            RelationEquivalence(left="b", right="y", into="z"),
        ],
    )
    assert _kinds(op, _side("l", [], ["a", "b"]), _side("r", [], ["x", "y"])) == [
        "shared_into"
    ]


def test_two_disjoint_clusters_are_independent() -> None:
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="A", right="X", into="A"),
            VertexEquivalence(left="B", right="Y", into="B"),
        ]
    )
    index = resolve_clusters(
        op, left=_side("l", ["A", "B"]), right=_side("r", ["X", "Y"])
    ).index
    assert len(index.vertices) == 2
    assert index.labels == frozenset({"A", "B"})
