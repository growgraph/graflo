"""Deriving a change set from two manifests.

Nothing produced `ManifestOp` values before this: every op in the codebase was
hand-built, and the migrate-plane differ emits description records that cannot
be applied. The contract here is the replay invariant — applying the derived ops
to the base must reproduce the target, verified by content hash.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.autogenerate import (
    RenameHints,
    diff_manifests,
    diff_manifests_verified,
)
from graflo.architecture.evolution.codec import ops_from_yaml, ops_to_yaml_str
from graflo.architecture.evolution.hashing import manifest_hash

PARTY = {"name": "party", "properties": ["id", "name"], "identity": ["id"]}
ORDER = {"name": "order", "properties": ["oid", "total"], "identity": ["oid"]}
PLACES = {"source": "party", "target": "order", "relation": "places"}


def _manifest(
    vertices: list[dict],
    *,
    edges: list[dict] | None = None,
    vertex_indexes: dict | None = None,
    resources: list[dict] | None = None,
) -> GraphManifest:
    payload: dict = {
        "schema": {
            "metadata": {"name": "autogen-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {"vertices": vertices},
                "edge_config": {"edges": edges or []},
            },
        }
    }
    if vertex_indexes is not None:
        payload["schema"]["db_profile"] = {"vertex_indexes": vertex_indexes}
    if resources is not None:
        payload["ingestion_model"] = {"resources": resources}
    return GraphManifest.model_validate(payload)


def _assert_replays(base: GraphManifest, target: GraphManifest, **kwargs) -> list:
    """The invariant: derived ops must reproduce the target exactly."""
    ops, warnings = diff_manifests_verified(base, target, **kwargs)
    assert not warnings, f"unexpected warnings: {warnings}"
    replayed = apply_evolution(base, ops, bump_version=False, finish_init=False)
    assert manifest_hash(replayed) == manifest_hash(target)
    return ops


class TestReplayInvariant:
    def test_adding_a_vertex_replays(self) -> None:
        ops = _assert_replays(_manifest([PARTY]), _manifest([PARTY, ORDER]))
        assert [op.op for op in ops] == ["add_vertices"]

    def test_adding_an_edge_replays(self) -> None:
        ops = _assert_replays(
            _manifest([PARTY, ORDER]), _manifest([PARTY, ORDER], edges=[PLACES])
        )
        assert [op.op for op in ops] == ["add_edges"]

    def test_adding_a_property_replays(self) -> None:
        widened = {**PARTY, "properties": ["id", "name", "email"]}
        ops = _assert_replays(_manifest([PARTY]), _manifest([widened]))
        assert [op.op for op in ops] == ["add_vertex_properties"]

    def test_removing_a_property_replays(self) -> None:
        narrowed = {**PARTY, "properties": ["id"]}
        ops = _assert_replays(_manifest([PARTY]), _manifest([narrowed]))
        assert [op.op for op in ops] == ["remove_vertex_properties"]

    def test_removing_a_vertex_replays(self) -> None:
        ops = _assert_replays(_manifest([PARTY, ORDER]), _manifest([PARTY]))
        assert [op.op for op in ops] == ["remove_vertices"]

    def test_changing_identity_replays(self) -> None:
        rekeyed = {
            "name": "party",
            "properties": ["id", "name"],
            "identity": ["name"],
        }
        ops = _assert_replays(_manifest([PARTY]), _manifest([rekeyed]))
        assert [op.op for op in ops] == ["replace_identity"]

    def test_moving_to_a_funnel_identity_replays(self) -> None:
        funnelled = {
            "name": "party",
            "properties": ["id", "name", "email"],
            "identity_funnel": {
                "branches": [
                    {"id": "email", "fields": ["email"]},
                    {"id": "weak", "fields": ["name"]},
                ]
            },
        }
        ops = _assert_replays(
            _manifest([{**PARTY, "properties": ["id", "name", "email"]}]),
            _manifest([funnelled]),
        )
        assert [op.op for op in ops] == ["replace_identity"]

    def test_adding_an_index_replays(self) -> None:
        ops = _assert_replays(
            _manifest([PARTY]),
            _manifest([PARTY], vertex_indexes={"party": [{"fields": ["name"]}]}),
        )
        assert [op.op for op in ops] == ["add_vertex_indexes"]

    def test_removing_an_index_replays(self) -> None:
        ops = _assert_replays(
            _manifest([PARTY], vertex_indexes={"party": [{"fields": ["name"]}]}),
            _manifest([PARTY], vertex_indexes={}),
        )
        assert [op.op for op in ops] == ["remove_vertex_indexes"]

    def test_a_compound_change_replays(self) -> None:
        base = _manifest([PARTY, ORDER], edges=[PLACES])
        target = _manifest(
            [
                {"name": "party", "properties": ["id", "email"], "identity": ["email"]},
                {
                    "name": "order",
                    "properties": ["oid", "total", "currency"],
                    "identity": ["oid"],
                },
                {"name": "invoice", "properties": ["inv"], "identity": ["inv"]},
            ],
            edges=[
                PLACES,
                {"source": "order", "target": "invoice", "relation": "billed"},
            ],
        )

        ops = _assert_replays(base, target)

        assert "add_vertices" in {op.op for op in ops}
        assert "remove_vertex_properties" in {op.op for op in ops}


class TestOrdering:
    def test_removals_run_after_everything_else(self) -> None:
        """A removal first would delete elements later ops still address."""
        base = _manifest([PARTY, ORDER], edges=[PLACES])
        target = _manifest([{**PARTY, "properties": ["id", "name", "email"]}])

        ops, _ = diff_manifests(base, target)

        kinds = [op.op for op in ops]
        assert kinds.index("add_vertex_properties") < kinds.index("remove_vertices")
        assert kinds.index("remove_edges") < kinds.index("remove_vertices")

    def test_renames_run_first(self) -> None:
        base = _manifest([PARTY])
        target = _manifest(
            [
                {
                    "name": "person",
                    "properties": ["id", "name", "tag"],
                    "identity": ["id"],
                }
            ]
        )

        ops, _ = diff_manifests(
            base, target, hints=RenameHints(vertices={"party": "person"})
        )

        assert ops[0].op == "rename_vertices"


class TestRenameHints:
    def test_a_rename_hint_turns_a_drop_and_add_into_a_rename(self) -> None:
        base = _manifest([PARTY])
        target = _manifest(
            [{"name": "person", "properties": ["id", "name"], "identity": ["id"]}]
        )

        without = {op.op for op in diff_manifests(base, target)[0]}
        with_hint = {
            op.op
            for op in diff_manifests(
                base, target, hints=RenameHints(vertices={"party": "person"})
            )[0]
        }

        assert without == {"add_vertices", "remove_vertices"}
        assert with_hint == {"rename_vertices"}

    def test_a_hinted_rename_replays(self) -> None:
        base = _manifest([PARTY])
        target = _manifest(
            [{"name": "person", "properties": ["id", "name"], "identity": ["id"]}]
        )

        _assert_replays(base, target, hints=RenameHints(vertices={"party": "person"}))

    def test_a_property_rename_hint_replays(self) -> None:
        base = _manifest([PARTY])
        target = _manifest(
            [{"name": "party", "properties": ["id", "label"], "identity": ["id"]}]
        )

        _assert_replays(
            base,
            target,
            hints=RenameHints(vertex_properties={"party": {"name": "label"}}),
        )


class TestVerification:
    def test_identical_manifests_yield_no_ops(self) -> None:
        ops, warnings = diff_manifests_verified(_manifest([PARTY]), _manifest([PARTY]))

        assert ops == []
        assert warnings == []

    def test_an_unexpressible_bindings_change_is_reported_not_hidden(self) -> None:
        base = _manifest([PARTY])
        target = _manifest([PARTY])
        target.bindings = None
        base_with_bindings = GraphManifest.model_validate(
            {
                **base.to_dict(skip_defaults=True),
                "bindings": {
                    "connectors": [{"regex": "^a\\.csv$", "resource_name": "r"}]
                },
            }
        )

        _, warnings = diff_manifests_verified(base_with_bindings, target)

        assert any("bindings" in w for w in warnings)

    def test_a_resource_change_is_reported(self) -> None:
        base = _manifest(
            [PARTY], resources=[{"name": "src", "pipeline": [{"vertex": "party"}]}]
        )
        target = _manifest(
            [PARTY],
            resources=[
                {"name": "src", "pipeline": [{"vertex": "party"}]},
                {"name": "extra", "pipeline": [{"vertex": "party"}]},
            ],
        )

        _, warnings = diff_manifests_verified(base, target)

        assert any("resources added" in w for w in warnings)

    def test_a_resource_rename_hint_replays(self) -> None:
        base = _manifest(
            [PARTY], resources=[{"name": "src", "pipeline": [{"vertex": "party"}]}]
        )
        target = _manifest(
            [PARTY], resources=[{"name": "crm", "pipeline": [{"vertex": "party"}]}]
        )

        _assert_replays(base, target, hints=RenameHints(resources={"src": "crm"}))


class TestSerializableOutput:
    def test_a_derived_change_set_round_trips_through_yaml(self) -> None:
        """Autogenerate and the codec have to compose, or revisions cannot be stored."""
        base = _manifest([PARTY])
        target = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["id", "name", "email"],
                    "identity": ["email"],
                },
                ORDER,
            ]
        )
        ops, _ = diff_manifests(base, target)

        restored = ops_from_yaml(ops_to_yaml_str(ops))

        assert [op.op for op in restored] == [op.op for op in ops]
        replayed = apply_evolution(
            base, restored, bump_version=False, finish_init=False
        )
        assert manifest_hash(replayed) == manifest_hash(target)


@pytest.mark.parametrize(
    "target_vertices",
    [
        pytest.param([PARTY, ORDER], id="add-vertex"),
        pytest.param([{**PARTY, "properties": ["id", "name", "x"]}], id="add-property"),
        pytest.param([{**PARTY, "properties": ["id"]}], id="drop-property"),
        pytest.param(
            [{"name": "party", "properties": ["id", "name"], "identity": ["name"]}],
            id="rekey",
        ),
        pytest.param(
            [{"name": "party", "properties": ["id", "name"], "blank": True}],
            id="to-blank",
        ),
        pytest.param(
            [
                {
                    "name": "party",
                    "properties": ["id", "name"],
                    "hash_identity_properties": ["name"],
                }
            ],
            id="to-hash",
        ),
    ],
)
def test_replay_invariant_over_a_corpus(target_vertices: list[dict]) -> None:
    _assert_replays(_manifest([PARTY]), _manifest(target_vertices))
