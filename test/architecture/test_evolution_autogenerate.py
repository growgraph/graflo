"""Deriving a change set from two manifests.

Nothing produced `ManifestOp` values before this: every op in the codebase was
hand-built, and the migrate-plane differ emits description records that cannot
be applied. The contract here is the replay invariant — applying the derived ops
to the base must reproduce the target, verified by content hash.
"""

from __future__ import annotations

from typing import Any

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
    edge_specs: list[dict] | None = None,
    db_profile: dict | None = None,
    resources: list[dict] | None = None,
) -> GraphManifest:
    schema: dict[str, Any] = {
        "metadata": {"name": "autogen-demo", "version": "1.0.0"},
        "graph": {
            "vertex_config": {"vertices": vertices},
            "edge_config": {"edges": edges or []},
        },
    }
    if vertex_indexes is not None or edge_specs is not None or db_profile is not None:
        schema["db_profile"] = {
            "vertex_indexes": vertex_indexes or {},
            "edge_specs": edge_specs or [],
            **(db_profile or {}),
        }
    payload: dict[str, Any] = {"schema": schema}
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

    def test_an_edge_index_diff_replays(self) -> None:
        """`edge_specs` is a list; the differ must read it as one."""
        spec = {**PLACES, "indexes": [{"fields": ["when"]}]}
        ops = _assert_replays(
            _manifest([PARTY, ORDER], edges=[PLACES], edge_specs=[PLACES]),
            _manifest([PARTY, ORDER], edges=[PLACES], edge_specs=[spec]),
        )
        assert [op.op for op in ops] == ["add_edge_indexes"]
        ops = _assert_replays(
            _manifest([PARTY, ORDER], edges=[PLACES], edge_specs=[spec]),
            _manifest([PARTY, ORDER], edges=[PLACES], edge_specs=[PLACES]),
        )
        assert [op.op for op in ops] == ["remove_edge_indexes"]

    def test_an_edge_index_diff_addresses_the_purpose_variant(self) -> None:
        """A `purpose` variant is its own spec; the diff must not fold it onto the base."""
        base_spec = {**PLACES, "indexes": [{"fields": ["when"]}]}
        variant = {**PLACES, "purpose": "audit"}
        indexed_variant = {**variant, "indexes": [{"fields": ["when"]}]}
        ops = _assert_replays(
            _manifest([PARTY, ORDER], edges=[PLACES], edge_specs=[base_spec, variant]),
            _manifest(
                [PARTY, ORDER], edges=[PLACES], edge_specs=[base_spec, indexed_variant]
            ),
        )
        assert [op.op for op in ops] == ["add_edge_indexes"]
        assert [entry.purpose for entry in ops[0].edges] == ["audit"]

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


INVOICE = {"name": "invoice", "properties": ["inv"], "identity": ["inv"]}
BILLS = {"source": "order", "target": "invoice", "relation": "places"}


class TestEdgePropertiesAreAddressedPerRelation:
    """`add/remove_edge_properties` apply to every edge on a relation."""

    def test_a_gain_on_only_some_sibling_edges_is_reported_not_emitted(self) -> None:
        base = _manifest([PARTY, ORDER, INVOICE], edges=[PLACES, BILLS])
        target = _manifest(
            [PARTY, ORDER, INVOICE],
            edges=[{**PLACES, "properties": ["p1"]}, {**BILLS, "properties": ["p2"]}],
        )
        ops, warnings = diff_manifests(base, target)
        assert not any(op.op == "add_edge_properties" for op in ops)
        assert [w for w in warnings if "sibling" in w] == [
            (
                "relation 'places': property 'p1' differs between sibling edges; "
                "add/remove_edge_properties address a relation, not one edge"
            ),
            (
                "relation 'places': property 'p2' differs between sibling edges; "
                "add/remove_edge_properties address a relation, not one edge"
            ),
        ]
        _, verified = diff_manifests_verified(base, target)
        assert any(
            "not expressible" in w or "does not reproduce" in w for w in verified
        )

    def test_a_gain_on_every_sibling_edge_replays(self) -> None:
        base = _manifest([PARTY, ORDER, INVOICE], edges=[PLACES, BILLS])
        target = _manifest(
            [PARTY, ORDER, INVOICE],
            edges=[{**PLACES, "properties": ["p"]}, {**BILLS, "properties": ["p"]}],
        )
        ops = _assert_replays(base, target)
        assert [op.op for op in ops] == ["add_edge_properties"]
        assert ops[0].additions == {"places": ["p"]}

    def test_a_relation_less_edge_change_is_reported(self) -> None:
        bare = {"source": "party", "target": "order"}
        base = _manifest([PARTY, ORDER], edges=[bare])
        target = _manifest([PARTY, ORDER], edges=[{**bare, "properties": ["p"]}])
        _, warnings = diff_manifests(base, target)
        assert any("has no relation" in w for w in warnings)


class TestTypedAndGroundedFieldsReplay:
    def test_a_typed_new_vertex_property_replays_with_its_type(self) -> None:
        widened = {
            **PARTY,
            "properties": ["id", "name", {"name": "amt", "type": "FLOAT"}],
        }
        ops = _assert_replays(_manifest([PARTY]), _manifest([widened]))
        assert [op.op for op in ops] == ["add_vertex_properties"]
        (entry,) = ops[0].additions["party"]
        assert entry.name == "amt" and entry.type == "FLOAT"

    def test_a_typed_new_edge_property_replays_with_its_type(self) -> None:
        base = _manifest([PARTY, ORDER], edges=[PLACES])
        target = _manifest(
            [PARTY, ORDER],
            edges=[{**PLACES, "properties": [{"name": "amt", "type": "FLOAT"}]}],
        )
        ops = _assert_replays(base, target)
        assert [op.op for op in ops] == ["add_edge_properties"]

    def test_an_edge_field_type_change_replays(self) -> None:
        base = _manifest([PARTY, ORDER], edges=[{**PLACES, "properties": ["when"]}])
        target = _manifest(
            [PARTY, ORDER],
            edges=[{**PLACES, "properties": [{"name": "when", "type": "INT"}]}],
        )
        ops = _assert_replays(base, target)
        assert [op.op for op in ops] == ["change_field_types"]
        assert ops[0].edges["places"]["when"].type == "INT"

    def test_a_vertex_grounding_change_replays_and_clears(self) -> None:
        grounded = {**PARTY, "semantics": {"iri": "https://schema.org/Organization"}}
        ops = _assert_replays(_manifest([PARTY]), _manifest([grounded]))
        assert [op.op for op in ops] == ["set_vertex_semantics"]
        ops = _assert_replays(_manifest([grounded]), _manifest([PARTY]))
        assert [op.op for op in ops] == ["set_vertex_semantics"]
        assert ops[0].semantics == {"party": None}

    def test_a_field_grounding_change_replays(self) -> None:
        grounded = {
            **PARTY,
            "properties": [
                "id",
                {"name": "name", "semantics": {"iri": "https://schema.org/name"}},
            ],
        }
        ops = _assert_replays(_manifest([PARTY]), _manifest([grounded]))
        assert [op.op for op in ops] == ["set_field_semantics"]

    def test_an_edge_property_grounding_change_replays(self) -> None:
        base = _manifest([PARTY, ORDER], edges=[{**PLACES, "properties": ["when"]}])
        target = _manifest(
            [PARTY, ORDER],
            edges=[
                {
                    **PLACES,
                    "properties": [{"name": "when", "semantics": {"unit": "s"}}],
                }
            ],
        )
        ops = _assert_replays(base, target)
        assert [op.op for op in ops] == ["set_field_semantics"]
        assert ops[0].targets[0].edge_id() == ("party", "order", "places")

    def test_edge_groundings_are_grouped_by_value(self) -> None:
        base = _manifest([PARTY, ORDER, INVOICE], edges=[PLACES, BILLS])
        target = _manifest(
            [PARTY, ORDER, INVOICE],
            edges=[
                {**PLACES, "semantics": {"iri": "x:a"}},
                {**BILLS, "semantics": {"iri": "x:b"}},
            ],
        )
        ops = _assert_replays(base, target)
        assert [op.op for op in ops] == ["set_edge_semantics", "set_edge_semantics"]
        assert {op.semantics.iri for op in ops} == {"x:a", "x:b"}


def test_a_profile_difference_no_op_expresses_is_reported() -> None:
    base = _manifest([PARTY], db_profile={"vertex_storage_names": {"party": "p"}})
    target = _manifest([PARTY], db_profile={"vertex_storage_names": {"party": "q"}})
    _, warnings = diff_manifests(base, target)
    assert any("db_profile differs in ['vertex_storage_names']" in w for w in warnings)


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

        ops = _assert_replays(base, target)
        assert [op.op for op in ops] == ["add_resources"]
        assert [r.name for r in ops[0].resources] == ["extra"]

        ops = _assert_replays(target, base)
        assert [op.op for op in ops] == ["remove_resources"]
        assert ops[0].names == ["extra"]

    def test_a_pipeline_edit_is_reported_not_approximated(self) -> None:
        base = _manifest(
            [PARTY, ORDER],
            resources=[{"name": "src", "pipeline": [{"vertex": "party"}]}],
        )
        target = _manifest(
            [PARTY, ORDER],
            resources=[
                {"name": "src", "pipeline": [{"vertex": "party"}, {"vertex": "order"}]}
            ],
        )
        _, warnings = diff_manifests(base, target)
        assert any("resource 'src' differs" in w for w in warnings)

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


class TestListItemTypeIsVisible:
    """A LIST whose element type changed.

    ``LIST<STRING>`` and ``LIST<INT>`` share a ``type``, so a detector keyed on
    ``type`` alone reported no change for an edit that rewrites every stored
    value -- the one change most in need of a migration.
    """

    @staticmethod
    def _manifest(item_type: str, *, on_edge: bool = False) -> GraphManifest:
        tags = {"name": "tags", "type": "LIST", "item_type": item_type}
        return GraphManifest.model_validate(
            {
                "schema": {
                    "metadata": {"name": "m", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {
                            "vertices": [
                                {
                                    "name": "party",
                                    "properties": ["id"] if on_edge else ["id", tags],
                                    "identity": ["id"],
                                },
                                {
                                    "name": "asset",
                                    "properties": ["id"],
                                    "identity": ["id"],
                                },
                            ]
                        },
                        "edge_config": {
                            "edges": [
                                {
                                    "source": "party",
                                    "target": "asset",
                                    "relation": "holds",
                                    "properties": [tags] if on_edge else [],
                                }
                            ]
                        },
                    },
                }
            }
        )

    def test_a_vertex_list_item_type_change_produces_an_op(self) -> None:
        ops, _ = diff_manifests(self._manifest("STRING"), self._manifest("INT"))
        specs = [o.to_dict(skip_defaults=True) for o in ops]
        assert specs == [
            {"vertices": {"party": {"tags": {"type": "LIST", "item_type": "INT"}}}}
        ]

    def test_an_edge_list_item_type_change_produces_an_op(self) -> None:
        ops, _ = diff_manifests(
            self._manifest("STRING", on_edge=True), self._manifest("INT", on_edge=True)
        )
        assert ops, "an edge LIST item_type change produced no op"
        assert any("INT" in str(o.to_dict(skip_defaults=True)) for o in ops)

    def test_no_change_produces_no_op(self) -> None:
        ops, _ = diff_manifests(self._manifest("STRING"), self._manifest("STRING"))
        assert ops == []
