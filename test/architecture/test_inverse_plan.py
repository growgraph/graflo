"""The inverse planners: realize, repair, switch, withdraw, declare symmetric.

Each emits primitive ops and applies nothing. What is tested is the part that is
awkward by hand -- which relations an op may name, in what order, and that
everything left out comes with a reason -- and that the ops really do what the
plan says when applied.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    apply_evolution,
    invert_ops,
    plan_declare_symmetric,
    plan_realize_inverses,
    plan_repair_inverses,
    plan_switch_realization,
    plan_withdraw_realization,
)
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.profile.inverses import audit_inverses, manifest_for_audit

FORWARD = {"source": "person", "target": "institution", "relation": "employed_by"}
INVERSE = {"source": "institution", "target": "person", "relation": "employs"}
FUNDS = {"source": "person", "target": "institution", "relation": "funds"}
KNOWS = {"source": "person", "target": "person", "relation": "knows"}
PAIR = {"relation": "employed_by", "inverse": "employs"}
FUNDS_PAIR = {"relation": "funds", "inverse": "funded_by"}
VERTICES = [{"vertex": "person"}, {"vertex": "institution"}]
STEP = {"edge": {"from": "person", "to": "institution", "relation": "employed_by"}}
INVERSE_STEP = {"edge": {"from": "institution", "to": "person", "relation": "employs"}}


def _payload(
    resources: dict[str, dict[str, Any]] | None = None,
    *,
    edges: list[dict[str, Any]] | None = None,
    inverses: list[dict[str, str]] | None = None,
    symmetric: list[str] | None = None,
    flavor: str = "neo4j",
    native: list[str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": {
            "metadata": {"name": "plan", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "identity": ["pid"], "properties": ["pid"]},
                        {
                            "name": "institution",
                            "identity": ["iid"],
                            "properties": ["iid"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": copy.deepcopy(edges if edges is not None else [FORWARD]),
                    "inverses": inverses if inverses is not None else [PAIR],
                    "symmetric": symmetric or [],
                },
            },
            "db_profile": {"db_flavor": flavor, "native_inverses": native or []},
        }
    }
    if resources is not None:
        payload["ingestion_model"] = {
            "resources": [{"name": name, **body} for name, body in resources.items()]
        }
    return payload


def _manifest(*args: Any, **kwargs: Any) -> GraphManifest:
    manifest = GraphManifest.from_dict(_payload(*args, **kwargs))
    manifest.finish_init()
    return manifest


def _applied(manifest: GraphManifest, plan: Any) -> GraphManifest:
    out = apply_evolution(manifest, plan.ops, bump_version=False)
    out.finish_init()
    return out


class TestRealize:
    def test_auto_stores_nothing_where_the_reverse_read_is_cheap(self) -> None:
        """Most of the time storing the inverse only duplicates edges."""
        plan = plan_realize_inverses(_manifest(flavor="neo4j"))
        assert plan.ops == []
        (skipped,) = plan.skipped
        assert skipped.code == "declaration_suffices"
        assert "reverse traversal" in skipped.reason

    def test_auto_never_withdraws_what_is_already_stored(self) -> None:
        """It says the backend would not have needed it, and how to drop it."""
        plan = plan_realize_inverses(
            _manifest(edges=[FORWARD, INVERSE], flavor="neo4j")
        )
        assert plan.ops == []
        (skipped,) = plan.skipped
        assert skipped.code == "stored_not_needed"
        assert "already materialized" in skipped.reason

    def test_auto_has_tigergraph_maintain_the_inverse(self) -> None:
        manifest = _manifest(flavor="tigergraph")
        plan = plan_realize_inverses(manifest)
        assert [op.op for op in plan.ops] == ["set_native_inverses"]
        assert audit_inverses(_applied(manifest, plan)).pairs[0].state == "native"

    def test_auto_writes_the_reverse_view_where_direction_is_the_storage_key(
        self,
    ) -> None:
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, STEP]}}, flavor="graflo_backend"
        )
        plan = plan_realize_inverses(manifest)
        assert [op.op for op in plan.ops] == ["add_inverse_edges"]

    def test_native_for_every_eligible_pair_with_a_reason_for_each_of_the_rest(
        self,
    ) -> None:
        manifest = _manifest(
            edges=[FORWARD, FUNDS, KNOWS, {**FUNDS, "relation": "advises"}],
            inverses=[
                PAIR,
                FUNDS_PAIR,
                {"relation": "advises", "inverse": "person"},
            ],
            flavor="tigergraph",
        )
        plan = plan_realize_inverses(manifest, strategy="native")

        (op,) = plan.ops
        assert op.op == "set_native_inverses"
        assert op.relations == ["employed_by", "funds"]  # type: ignore[union-attr]
        assert [(s.relation, s.code) for s in plan.skipped] == [
            ("advises", "collides_with_vertex")
        ]
        assert {p.state for p in plan.after.pairs} == {"native", "declared"}

    def test_the_native_side_is_the_relation_that_has_edges(self) -> None:
        """Not the alphabetically first name of the pair."""
        manifest = _manifest(
            edges=[{**FORWARD, "relation": "works_at"}],
            inverses=[{"relation": "employs", "inverse": "works_at"}],
            flavor="tigergraph",
        )
        (op,) = plan_realize_inverses(manifest, strategy="native").ops
        assert op.relations == ["works_at"]  # type: ignore[union-attr]

    def test_native_is_refused_off_tigergraph_with_the_reason(self) -> None:
        plan = plan_realize_inverses(_manifest(flavor="neo4j"), strategy="native")
        assert plan.ops == []
        assert [s.code for s in plan.skipped] == ["not_tigergraph"]

    def test_materialized_declares_the_edge_and_flags_the_steps(self) -> None:
        manifest = _manifest({"rows": {"pipeline": [*VERTICES, STEP]}})
        plan = plan_realize_inverses(manifest, strategy="materialized")

        report = audit_inverses(_applied(manifest, plan))
        assert report.pairs[0].state == "materialized"
        assert report.pairs[0].feeding == {"rows": "emit_inverse"}
        assert report.findings == []

    def test_a_pair_is_never_realized_a_second_way(self) -> None:
        manifest = _manifest(flavor="tigergraph", native=["employed_by"])
        plan = plan_realize_inverses(manifest, strategy="materialized")
        assert plan.ops == []
        assert [s.code for s in plan.skipped] == ["native"]
        assert "plan_switch_realization" in plan.skipped[0].reason

    def test_relations_restrict_the_plan_and_unknown_ones_are_reported(self) -> None:
        manifest = _manifest(
            edges=[FORWARD, FUNDS], inverses=[PAIR, FUNDS_PAIR], flavor="tigergraph"
        )
        plan = plan_realize_inverses(
            manifest, strategy="native", relations=["funded_by", "manages"]
        )
        (op,) = plan.ops
        assert op.relations == ["funds"]  # type: ignore[union-attr]
        assert [(s.relation, s.code) for s in plan.skipped] == [
            ("manages", "undeclared")
        ]

    def test_a_conflicting_pair_is_left_alone(self) -> None:
        same_side = {**FORWARD, "relation": "employs"}
        plan = plan_realize_inverses(
            _manifest(edges=[FORWARD, same_side]), strategy="materialized"
        )
        assert plan.ops == []
        assert [s.code for s in plan.skipped] == ["conflicting"]
        assert [f.kind for f in plan.remaining] == ["same_side_pair"]

    def test_the_plan_does_not_change_the_manifest_it_was_given(self) -> None:
        manifest = _manifest({"rows": {"pipeline": [*VERTICES, STEP]}})
        before = manifest_hash(manifest)
        plan_realize_inverses(manifest, strategy="materialized")
        assert manifest_hash(manifest) == before


class TestRepair:
    def test_a_merged_manifest_where_one_source_reported_both_readings(self) -> None:
        manifest = _manifest(
            {
                "hr": {
                    "pipeline": [*VERTICES, STEP, INVERSE_STEP],
                    "infer_edges": False,
                },
                "registry": {"pipeline": [*VERTICES, STEP], "infer_edges": False},
            },
            edges=[FORWARD, INVERSE],
        )
        plan = plan_repair_inverses(manifest)

        (op,) = plan.ops
        assert op.op == "set_inverse_emission"
        assert list(op.steps) == ["registry"]  # type: ignore[union-attr]
        assert plan.remaining == []
        report = audit_inverses(_applied(manifest, plan))
        assert report.pairs[0].feeding == {"hr": "step", "registry": "emit_inverse"}

    def test_it_completes_a_pair_mirrored_for_some_endpoint_pairs_only(self) -> None:
        other = {"source": "person", "target": "person", "relation": "employed_by"}
        manifest = _manifest(edges=[FORWARD, INVERSE, other])
        plan = plan_repair_inverses(manifest)
        assert [op.op for op in plan.ops] == ["add_inverse_edges"]
        assert plan.after.pairs[0].state == "materialized"

    def test_it_propagates_a_property_only_one_mirror_states(self) -> None:
        richer = {**INVERSE, "properties": ["since"]}
        manifest = _manifest(edges=[FORWARD, richer])
        plan = plan_repair_inverses(manifest)
        (op,) = plan.ops
        assert op.op == "add_edge_properties"
        assert op.additions == {"employed_by": ["since"]}  # type: ignore[union-attr]
        assert plan.remaining == []

    def test_it_declares_a_relation_symmetric_when_every_edge_is_undirected(
        self,
    ) -> None:
        manifest = _manifest(edges=[FORWARD, {**KNOWS, "directed": False}])
        plan = plan_repair_inverses(manifest)
        assert [op.op for op in plan.ops] == ["declare_edge_inverses"]
        assert plan.after.symmetric == ["knows"]

    def test_it_repairs_a_manifest_that_does_not_load(self) -> None:
        payload = _payload(edges=[FORWARD, KNOWS], symmetric=["knows"])
        with pytest.raises(ValueError, match="must be undirected"):
            GraphManifest.from_dict(payload).finish_init()

        manifest = manifest_for_audit(payload)
        plan = plan_repair_inverses(manifest)
        assert [op.op for op in plan.ops] == ["set_edge_directed"]
        _applied(manifest, plan)

    def test_contradictions_are_listed_and_never_touched(self) -> None:
        drifted = {**INVERSE, "identities": [["since"]], "properties": ["since"]}
        keyed = {**FORWARD, "properties": ["since"]}
        plan = plan_repair_inverses(_manifest(edges=[keyed, drifted]))
        assert plan.ops == []
        assert [f.kind for f in plan.remaining] == ["identity_drift"]

    def test_a_clean_manifest_needs_nothing(self) -> None:
        plan = plan_repair_inverses(_manifest())
        assert (plan.ops, plan.skipped, plan.remaining) == ([], [], [])

    def test_a_repair_can_be_undone_exactly(self) -> None:
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, STEP], "infer_edges": False}},
            edges=[FORWARD, INVERSE],
        )
        plan = plan_repair_inverses(manifest)
        undo, blockers = invert_ops(plan.ops, manifest=manifest)
        assert blockers == []
        restored = apply_evolution(_applied(manifest, plan), undo, bump_version=False)
        assert manifest_hash(restored) == manifest_hash(manifest)


class TestSwitch:
    def test_materialized_to_native_withdraws_first_then_adds(self) -> None:
        manifest = _manifest(
            {
                "rows": {
                    "pipeline": [
                        *VERTICES,
                        {"edge": {**STEP["edge"], "emit_inverse": True}},
                    ]
                }
            },
            edges=[FORWARD, INVERSE],
            flavor="tigergraph",
        )
        plan = plan_switch_realization(manifest, ["employed_by"], to="native")

        assert [op.op for op in plan.ops] == ["remove_edges", "set_native_inverses"]
        out = _applied(manifest, plan)
        report = audit_inverses(out)
        assert report.pairs[0].state == "native"
        assert report.findings == []

    def test_a_switch_that_would_not_be_eligible_plans_nothing(self) -> None:
        """The pair is never left withdrawn and unrealized."""
        manifest = _manifest(edges=[FORWARD, INVERSE], flavor="neo4j")
        plan = plan_switch_realization(manifest, ["employed_by"], to="native")
        assert plan.ops == []
        assert [s.code for s in plan.skipped] == ["not_tigergraph"]

    def test_native_to_materialized(self) -> None:
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, STEP]}},
            flavor="tigergraph",
            native=["employed_by"],
        )
        plan = plan_switch_realization(manifest, ["employs"], to="materialized")
        assert [op.op for op in plan.ops] == [
            "set_native_inverses",
            "add_inverse_edges",
        ]
        assert plan.after.pairs[0].state == "materialized"

    def test_a_pair_that_is_only_declared_has_nothing_to_withdraw_first(self) -> None:
        manifest = _manifest(flavor="tigergraph")
        plan = plan_switch_realization(manifest, ["employed_by"], to="native")
        assert [op.op for op in plan.ops] == ["set_native_inverses"]

    def test_already_there(self) -> None:
        manifest = _manifest(edges=[FORWARD, INVERSE])
        plan = plan_switch_realization(manifest, ["employed_by"], to="materialized")
        assert [s.code for s in plan.skipped] == ["already"]


class TestWithdraw:
    """Stop storing the inverse; the declaration stays."""

    def test_a_materialized_inverse_loses_its_edges_and_the_flags_that_fed_them(
        self,
    ) -> None:
        flagged = {"edge": {**STEP["edge"], "emit_inverse": True}}
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, flagged]}}, edges=[FORWARD, INVERSE]
        )
        plan = plan_withdraw_realization(manifest, ["employed_by"])

        assert [op.op for op in plan.ops] == ["remove_edges"]
        (pair,) = plan.after.pairs
        assert (pair.state, pair.stored_sides) == ("declared", ["employed_by"])
        assert plan.after.findings == []
        _applied(manifest, plan)

    def test_a_native_inverse_is_handed_back(self) -> None:
        manifest = _manifest(flavor="tigergraph", native=["employed_by"])
        plan = plan_withdraw_realization(manifest, ["employs"])
        assert [op.op for op in plan.ops] == ["set_native_inverses"]
        assert plan.ops[0].enabled is False  # type: ignore[union-attr]
        assert plan.after.pairs[0].state == "declared"

    def test_the_named_relation_is_the_side_that_stays_stored(self) -> None:
        manifest = _manifest(edges=[FORWARD, INVERSE])
        plan = plan_withdraw_realization(manifest, ["employs"])
        assert plan.after.pairs[0].stored_sides == ["employs"]

    def test_the_pair_stays_declared(self) -> None:
        manifest = _manifest(edges=[FORWARD, INVERSE])
        out = _applied(manifest, plan_withdraw_realization(manifest, ["employed_by"]))
        assert out.graph_schema is not None
        pairs = out.graph_schema.core_schema.edge_config.inverses
        assert [(p.relation, p.inverse) for p in pairs] == [("employed_by", "employs")]

    def test_a_pair_that_is_only_declared_has_nothing_to_withdraw(self) -> None:
        plan = plan_withdraw_realization(_manifest(), ["employed_by"])
        assert plan.ops == []
        assert [s.code for s in plan.skipped] == ["already"]

    def test_it_undoes_exactly(self) -> None:
        manifest = _manifest(edges=[FORWARD, INVERSE])
        plan = plan_withdraw_realization(manifest, ["employed_by"])
        undo, blockers = invert_ops(plan.ops, manifest=manifest)
        assert blockers == []
        restored = apply_evolution(_applied(manifest, plan), undo, bump_version=False)
        assert manifest_hash(restored) == manifest_hash(manifest)


class TestDeclareSymmetric:
    def test_the_edges_become_undirected_and_then_the_relation_is_declared(
        self,
    ) -> None:
        manifest = _manifest(edges=[FORWARD, KNOWS])
        plan = plan_declare_symmetric(manifest, ["knows"])
        assert [op.op for op in plan.ops] == [
            "set_edge_directed",
            "declare_edge_inverses",
        ]
        out = _applied(manifest, plan)
        assert out.graph_schema is not None
        assert out.graph_schema.core_schema.edge_config.symmetric == ["knows"]

    def test_both_steps_undo_exactly(self) -> None:
        manifest = _manifest(edges=[FORWARD, KNOWS])
        plan = plan_declare_symmetric(manifest, ["knows"])
        undo, blockers = invert_ops(plan.ops, manifest=manifest)
        assert blockers == []
        restored = apply_evolution(_applied(manifest, plan), undo, bump_version=False)
        assert manifest_hash(restored) == manifest_hash(manifest)

    def test_a_paired_relation_cannot_also_be_its_own_inverse(self) -> None:
        plan = plan_declare_symmetric(_manifest(), ["employed_by"])
        assert plan.ops == []
        assert [s.code for s in plan.skipped] == ["paired"]


def test_a_plan_renders_as_text() -> None:
    plan = plan_realize_inverses(_manifest(flavor="neo4j"), strategy="native")
    lines = plan.to_lines()
    assert lines[0] == "0 op(s)"
    assert any("[not_tigergraph]" in line for line in lines)
