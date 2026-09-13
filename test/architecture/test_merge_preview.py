"""The merge projection: a slot tree, and what each branch did to it.

Unlike compose, ``merge_three_way`` already *returns* its conflicts, so there
is nothing to re-derive here — only a shape to put them in. What the tests
guard is that shape: a tree whose every node's parent is its own slot prefix,
with contested slots carrying both branches and the ancestor.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.merge3 import merge_three_way, take_left
from graflo.architecture.evolution.ops import (
    AddVertexPropertiesOp,
    IdentityReplacement,
    NaturalIdentityTarget,
    ReplaceIdentityOp,
)
from graflo.architecture.evolution.preview import MergePreview, build_merge_preview


def _manifest(identity: list[str], properties: list[str]) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "people", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "person",
                                "properties": properties,
                                "identity": identity,
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


@pytest.fixture
def ancestor() -> GraphManifest:
    return _manifest(["ssn"], ["ssn", "email", "name"])


@pytest.fixture
def branches(ancestor):
    """Two branches that re-key ``person`` differently: a contested slot."""
    left = apply_evolution(
        ancestor,
        [
            ReplaceIdentityOp(
                replacements={
                    "person": IdentityReplacement(
                        to=NaturalIdentityTarget(identity=["email"])
                    )
                }
            )
        ],
        bump_version=False,
        finish_init=False,
    )
    right = apply_evolution(
        ancestor,
        [
            ReplaceIdentityOp(
                replacements={
                    "person": IdentityReplacement(
                        to=NaturalIdentityTarget(identity=["name"])
                    )
                }
            )
        ],
        bump_version=False,
        finish_init=False,
    )
    return left, right


def test_a_contested_slot_carries_both_branches_and_the_ancestor(ancestor, branches):
    left, right = branches
    _merged, result = merge_three_way(ancestor, left, right)

    preview = build_merge_preview(result)

    assert preview.conflicts == 1
    assert not preview.clean
    (contested,) = preview.contested
    assert contested.id == "vertex/person/identity"
    assert contested.segment == "identity"
    assert contested.left_ops == ["replace_identity"]
    assert contested.right_ops == ["replace_identity"]
    assert "identity" in (contested.base_excerpt or {}), (
        "the ancestor's state is what a decision turns on"
    )


def test_the_slots_form_a_tree(ancestor, branches):
    """Slots are paths, so containment is the tree; every prefix is a node."""
    left, right = branches
    _merged, result = merge_three_way(ancestor, left, right)

    preview = build_merge_preview(result)

    ids = {node.id for node in preview.nodes}
    for node in preview.nodes:
        assert (node.parent is None) == (node.depth == 0)
        if node.parent is not None:
            assert node.parent in ids, f"{node.id} has no parent node"
            assert node.id == f"{node.parent}/{node.segment}"
    assert [n.id for n in preview.children_of(None)] == ["vertex"]
    assert [n.id for n in preview.children_of("vertex")] == ["vertex/person"]


def test_a_contested_slot_reports_nothing_as_applied(ancestor, branches):
    """A contested slot is held back whole, so nothing there was applied."""
    left, right = branches
    _merged, result = merge_three_way(ancestor, left, right)

    preview = build_merge_preview(result)

    (contested,) = preview.contested
    assert contested.clean_ops == []


def test_a_resolved_merge_shows_what_it_applied_and_where(ancestor, branches):
    left, right = branches
    left = apply_evolution(
        left,
        [AddVertexPropertiesOp(additions={"person": ["nickname"]})],
        bump_version=False,
        finish_init=False,
    )
    _m, unresolved = merge_three_way(ancestor, left, right)
    _merged, result = merge_three_way(
        ancestor,
        left,
        right,
        resolutions=[take_left(c) for c in unresolved.conflicts],
    )

    preview = build_merge_preview(result)

    assert preview.clean
    assert preview.conflicts == 0
    assert not preview.contested
    applied = {node.id: node.clean_ops for node in preview.nodes if node.clean_ops}
    assert "vertex/person/identity" in applied
    assert any("nickname" in node.id for node in preview.nodes), (
        "the uncontested addition is in the tree too"
    )


def test_a_clean_merge_of_nothing_is_an_empty_tree(ancestor):
    _merged, result = merge_three_way(ancestor, ancestor, ancestor)

    preview = build_merge_preview(result)

    assert preview.clean
    assert preview.nodes == []
    assert preview.conflicts == 0


def test_a_preview_survives_a_round_trip_through_json(ancestor, branches):
    import json

    left, right = branches
    _merged, result = merge_three_way(ancestor, left, right)

    preview = build_merge_preview(result)
    payload = preview.to_dict()

    assert MergePreview.model_validate(json.loads(json.dumps(payload))).to_dict() == (
        payload
    )
