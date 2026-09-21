"""The differ's replay invariant on the shapes a merge produces.

Each case is a change that used to make ``diff_manifests_verified`` report a
difference no op expressed -- so a merge commit over it was refused -- although
every one of them is expressible:

* two spellings of one pipeline step, which are not a change at all;
* indexes derived from secondary identities, which move with the identity;
* a registry transform arriving with the resource that uses it;
* transform steps appended to an existing pipeline;
* a joined vertex description;
* a class the merge renamed or folded, which the commit must record as the
  merge's own relabel rather than as an unrelated add and remove.
"""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.autogenerate import diff_manifests_verified
from graflo.architecture.evolution.commit import CommitError, build_root_commit
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import History, checkout
from graflo.architecture.evolution.inverse import invert_op
from graflo.architecture.evolution.merge import merge_manifests
from graflo.architecture.evolution.merge3 import build_merge_recipe
from graflo.architecture.evolution.merge_commit import build_merge_commit
from graflo.architecture.evolution.ops import (
    MergeManifestsOp,
    RenameVerticesOp,
    SetVertexDescriptionsOp,
)

PARTY: dict[str, Any] = {
    "name": "party",
    "properties": ["id", "name"],
    "identity": ["id"],
}
ORDER: dict[str, Any] = {"name": "order", "properties": ["oid"], "identity": ["oid"]}
PLACES: dict[str, Any] = {"source": "party", "target": "order", "relation": "places"}

TAGGER: dict[str, Any] = {
    "name": "tagger",
    "module": "graflo.util.transform",
    "foo": "tagged_key",
    "params": {"tag": "crm", "sep": ":"},
    "input": ["id"],
    "output": ["local_key"],
}


def _manifest(
    vertices: list[dict[str, Any]],
    *,
    edges: list[dict[str, Any]] | None = None,
    resources: list[dict[str, Any]] | None = None,
    transforms: list[dict[str, Any]] | None = None,
) -> GraphManifest:
    payload: dict[str, Any] = {
        "schema": {
            "metadata": {"name": "replay", "version": "1.0.0"},
            "graph": {
                "vertex_config": {"vertices": vertices},
                "edge_config": {"edges": edges or []},
            },
        }
    }
    if resources is not None:
        payload["ingestion_model"] = {
            "resources": resources,
            "transforms": transforms or [],
        }
    manifest = GraphManifest.model_validate(payload)
    manifest.finish_init()
    return manifest


def _replays(base: GraphManifest, target: GraphManifest) -> list[Any]:
    ops, warnings = diff_manifests_verified(base, target)
    assert warnings == []
    replayed = apply_evolution(base, ops, bump_version=False, finish_init=False)
    assert manifest_hash(replayed) == manifest_hash(target)
    return ops


class TestStepSpelling:
    @pytest.mark.parametrize(
        ("authored", "respelled"),
        [
            ({"vertex": "party"}, {"vertex": "party", "type": "vertex"}),
            (
                {"from": "party", "to": "order", "relation": "places"},
                {"source": "party", "target": "order", "relation": "places"},
            ),
            (
                {"edge": {"from": "party", "to": "order", "relation": "places"}},
                {"from": "party", "to": "order", "relation": "places"},
            ),
        ],
        ids=["explicit-type", "from-to-vs-source-target", "edge-wrapper-vs-flat"],
    )
    def test_two_spellings_of_one_step_are_one_manifest(
        self, authored: dict[str, Any], respelled: dict[str, Any]
    ) -> None:
        def build(step: dict[str, Any]) -> GraphManifest:
            return _manifest(
                [PARTY, ORDER],
                edges=[PLACES],
                resources=[
                    {"name": "src", "pipeline": [{"vertex": "order"}, step]},
                ],
            )

        left, right = build(authored), build(respelled)

        assert manifest_hash(left) == manifest_hash(right)
        ops, warnings = diff_manifests_verified(left, right)
        assert (ops, warnings) == ([], [])


class TestDerivedIndexes:
    def test_removing_a_secondary_identity_does_not_emit_its_derived_index(
        self,
    ) -> None:
        keyed = {
            **PARTY,
            "secondary_identities": [{"name": "by_name", "fields": ["name"]}],
        }
        base, target = _manifest([keyed]), _manifest([PARTY])

        kinds = [op.op for op in _replays(base, target)]

        assert "remove_secondary_identities" in kinds
        assert "remove_vertex_indexes" not in kinds


class TestRegistryTransforms:
    def test_a_transform_arrives_with_the_resource_that_uses_it(self) -> None:
        base = _manifest([PARTY], resources=[])
        target = _manifest(
            [PARTY],
            resources=[
                {
                    "name": "crm",
                    "pipeline": [
                        {"transform": {"call": {"use": "tagger"}}},
                        {"vertex": "party"},
                    ],
                }
            ],
            transforms=[TAGGER],
        )

        [op] = _replays(base, target)

        assert op.op == "add_resources"
        assert [t.name for t in op.transforms] == ["tagger"]
        # remove_resources would leave the transform registered, and no op
        # withdraws one: this addition has no single-op inverse.
        assert invert_op(op, manifest=base) is None

    def test_appended_transform_steps_are_add_resource_transforms(self) -> None:
        pipeline = [{"vertex": "party"}]
        inline = {
            "transform": {
                "call": {
                    "module": "graflo.util.transform",
                    "foo": "tagged_key",
                    "params": {"tag": "crm", "sep": ":"},
                    "input": ["id"],
                    "output": ["local_key"],
                }
            }
        }
        base = _manifest([PARTY], resources=[{"name": "crm", "pipeline": pipeline}])
        target = _manifest(
            [PARTY], resources=[{"name": "crm", "pipeline": [*pipeline, inline]}]
        )

        [op] = _replays(base, target)

        assert op.op == "add_resource_transforms"
        assert list(op.additions) == ["crm"]


class TestDescriptions:
    def test_a_changed_description_is_set_and_inverts(self) -> None:
        base = _manifest([PARTY])
        target = _manifest([{**PARTY, "description": "A trading party."}])

        [op] = _replays(base, target)

        assert isinstance(op, SetVertexDescriptionsOp)
        assert op.descriptions == {"party": "A trading party."}
        inverse = invert_op(op, manifest=base)
        assert isinstance(inverse, SetVertexDescriptionsOp)
        assert inverse.descriptions == {"party": None}


class TestRenameReach:
    def test_a_vertex_rename_reaches_an_untyped_flat_edge_step(self) -> None:
        """``{from, to, relation}`` is an edge; a rename that skipped it stranded it."""
        manifest = _manifest(
            [PARTY, ORDER],
            edges=[PLACES],
            resources=[
                {
                    "name": "src",
                    "pipeline": [
                        {"vertex": "party"},
                        {"vertex": "order"},
                        {"from": "party", "to": "order", "relation": "places"},
                    ],
                }
            ],
        )

        renamed = apply_evolution(
            manifest,
            [RenameVerticesOp(renames={"order": "purchase"})],
            bump_version=False,
            finish_init=False,
        )

        assert renamed.ingestion_model is not None
        edge_step = renamed.ingestion_model.resources[0].pipeline[2]
        assert edge_step["to"] == "purchase"


class TestMergeCommitRelabel:
    @staticmethod
    def _sides() -> tuple[GraphManifest, GraphManifest, MergeManifestsOp]:
        left = _manifest(
            [{"name": "server", "properties": ["id", "serial"], "identity": ["id"]}],
            resources=[{"name": "cmdb", "pipeline": [{"vertex": "server"}]}],
        )
        right = _manifest(
            [{"name": "host", "properties": ["id", "sn"], "identity": ["id"]}],
            resources=[{"name": "scan", "pipeline": [{"vertex": "host"}]}],
        )
        op = MergeManifestsOp.model_validate(
            {
                "vertex_equivalences": [
                    {
                        "left": "server",
                        "right": "host",
                        "into": "machine",
                        "properties": [
                            {"left": "serial", "right": "sn", "into": "hw_serial"}
                        ],
                    }
                ]
            }
        )
        return left, right, op

    def test_a_folding_merge_records_and_replays_from_its_first_parent(self) -> None:
        left, right, op = self._sides()
        merged = merge_manifests(left, right, op, bump_version=False)
        recipe = build_merge_recipe(left, right, op)
        roots = [
            build_root_commit(left, scope="l"),
            build_root_commit(right, scope="r"),
        ]

        entry = build_merge_commit(
            left,
            merged,
            parents=[roots[0].id, roots[1].id],
            recipe=recipe,
            right=right,
        )

        assert entry.ops[0].op == "canonicalize"
        replayed = checkout(left, History(commits=[*roots, entry]), entry.id)
        assert manifest_hash(replayed) == manifest_hash(merged)

    def test_without_the_right_side_the_fold_is_not_recordable(self) -> None:
        """The relabel is resolved against both inputs; a plain diff cannot see it."""
        left, right, op = self._sides()
        merged = merge_manifests(left, right, op, bump_version=False)

        with pytest.raises(CommitError):
            build_merge_commit(
                left,
                merged,
                parents=["a" * 12, "b" * 12],
                recipe=build_merge_recipe(left, right, op),
            )
