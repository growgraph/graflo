"""The state aspect on its own: change and measurement, no provenance types."""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects import LiftSpec, plan_lift
from graflo.architecture.evolution.aspects.state import plan_state
from test.architecture.test_lift import FULL_SPEC, PLAIN


def _manifest() -> GraphManifest:
    manifest = GraphManifest.from_config(PLAIN)
    manifest.finish_init()
    return manifest


def test_the_state_plan_keeps_its_removals_apart_from_its_additions() -> None:
    plan = plan_state(_manifest(), FULL_SPEC, authored=PLAIN)
    assert all(op.op != "remove_vertex_properties" for op in plan.ops)
    assert [op.op for op in plan.retire] == ["remove_vertex_properties"]
    assert plan.scaffolded == ["MachineState", "MachineObservation"]


def test_state_alone_adds_no_provenance_types() -> None:
    ops = plan_lift(_manifest(), FULL_SPEC, aspects=["state"], authored=PLAIN)
    core = apply_evolution(_manifest(), ops).require_schema().core_schema
    assert {"MachineState", "MachineObservation"} <= core.vertex_config.vertex_set
    assert "SourceRecord" not in core.vertex_config.vertex_set
    assert "Agent" not in core.vertex_config.vertex_set
    relations = {e.relation for e in core.edge_config.edges}
    assert "wasDerivedFrom" not in relations


def test_both_aspects_attach_the_scaffolds_to_the_source_record() -> None:
    ops = plan_lift(_manifest(), FULL_SPEC, authored=PLAIN)
    core = apply_evolution(_manifest(), ops).require_schema().core_schema
    derived = {
        e.source for e in core.edge_config.edges if e.relation == "wasDerivedFrom"
    }
    assert derived == {"MachineState", "MachineObservation"}
    # The removals still come last, after every aspect's additions.
    assert ops[-1].op == "remove_vertex_properties"


def test_a_spec_that_declines_provenance_leaves_the_aspect_out() -> None:
    spec = LiftSpec.model_validate({**FULL_SPEC.model_dump(), "provenance": False})
    ops = plan_lift(_manifest(), spec, authored=PLAIN)
    core = apply_evolution(_manifest(), ops).require_schema().core_schema
    assert "SourceRecord" not in core.vertex_config.vertex_set
