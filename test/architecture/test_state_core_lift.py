"""Lifting an arbitrary manifest into a twin-ready schema, through Operations.

The claim under test is narrow and checkable: a manifest that fails every
assertion of the ``world-model`` profile, plus a statement of what its types
*mean*, produces an op list that makes it pass. Nothing here asserts that the
lift is clever -- it is deliberately mechanical. What it must be is honest:
loud where a declaration is missing, refusing rather than guessing, and
additive except for one clearly separated destructive step.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution, invert_ops
from graflo.architecture.evolution.state_core import LiftError, LiftSpec, plan_lift
from graflo.architecture.profile import check_manifest_config
from graflo.cli.io import DECLARED_KEYS, manifest_to_dict

PROV = "http://www.w3.org/ns/prov#"
SOSA = "http://www.w3.org/ns/sosa/"

PLAIN: dict = {
    "schema": {
        "metadata": {"name": "cmdb", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "ConfigurationItem",
                        "properties": [
                            {"name": "ci_id", "type": "STRING"},
                            {"name": "status", "type": "STRING"},
                            {"name": "temp_c", "type": "FLOAT"},
                        ],
                        "identity": ["ci_id"],
                    },
                    {
                        "name": "BusinessService",
                        "properties": [{"name": "service_id", "type": "STRING"}],
                        "identity": ["service_id"],
                    },
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": "ConfigurationItem",
                        "target": "BusinessService",
                        "relation": "supports",
                    }
                ]
            },
        },
    }
}

FULL_SPEC = LiftSpec.model_validate(
    {
        "grounding": {
            "ConfigurationItem": {
                "iri": f"{PROV}Entity",
                "exact_match": [f"{PROV}Entity", f"{SOSA}FeatureOfInterest"],
            },
            "BusinessService": {"iri": f"{PROV}Entity"},
        },
        "edge_grounding": [
            {
                "source": "ConfigurationItem",
                "target": "BusinessService",
                "relation": "supports",
                "iri": f"{PROV}wasInfluencedBy",
            }
        ],
        "stateful": {"ConfigurationItem": ["status"]},
        "observed": ["ConfigurationItem"],
        "measured": {"ConfigurationItem.temp_c": "Cel"},
    }
)


def _manifest(config: dict | None = None) -> GraphManifest:
    manifest = GraphManifest.from_config(config or PLAIN)
    manifest.finish_init()
    return manifest


def _lift(spec: LiftSpec = FULL_SPEC, config: dict | None = None) -> GraphManifest:
    manifest = _manifest(config)
    return apply_evolution(
        manifest, plan_lift(manifest, spec, authored=config or PLAIN)
    )


def _report(manifest: GraphManifest):
    return check_manifest_config(
        manifest_to_dict(manifest, declare=DECLARED_KEYS), subject="lifted"
    )


# ── the claim ───────────────────────────────────────────────────────────────


def test_the_input_fails_the_profile_before_the_lift() -> None:
    """Without this the headline test proves nothing."""
    report = check_manifest_config(PLAIN, subject="plain")
    assert not report.ok
    failed = {a.id for a in report.assertions if a.status == "fail"}
    assert {"grounded-types", "declared-directionality", "temporal"} <= failed


def test_a_lifted_manifest_passes_the_profile_clean() -> None:
    report = _report(_lift())
    assert report.ok, [f.message for f in report.errors()]
    assert report.status == "pass"
    assert report.warnings() == []


# ── what the lift builds ────────────────────────────────────────────────────


def test_state_is_reified_onto_its_own_type_with_a_validity_interval() -> None:
    core = _lift().require_schema().core_schema
    state = core.vertex_config["ConfigurationItemState"]
    names = {field.name for field in state.properties}
    assert {"ci_id", "status", "valid_from", "valid_to"} <= names
    # Keyed on the subject plus the interval start: the same property of the
    # same entity holds many values over time, and those are different facts.
    assert state.hash_identity_properties == ["ci_id", "valid_from"]


def test_a_moved_property_leaves_the_entity_it_no_longer_belongs_on() -> None:
    core = _lift().require_schema().core_schema
    names = {f.name for f in core.vertex_config["ConfigurationItem"].properties}
    assert "status" not in names
    assert "ci_id" in names


def test_retire_keep_leaves_the_current_value_beside_the_history() -> None:
    spec = FULL_SPEC.model_copy(update={"retire": "keep"})
    core = _lift(spec).require_schema().core_schema
    names = {f.name for f in core.vertex_config["ConfigurationItem"].properties}
    assert "status" in names
    assert "status" in {
        f.name for f in core.vertex_config["ConfigurationItemState"].properties
    }


def test_the_observation_scaffold_carries_its_unit_per_row() -> None:
    """An abstract observation type cannot name one unit without lying."""
    core = _lift().require_schema().core_schema
    observation = core.vertex_config["ConfigurationItemObservation"]
    unit_field = next(f for f in observation.properties if f.name == "result_unit")
    assert unit_field.semantics is not None
    assert "ucumCode" in (unit_field.semantics.iri or "")


def test_provenance_is_expressible_even_with_nothing_to_attach() -> None:
    """``Evidence -wasAttributedTo-> Agent`` is what makes lineage sayable."""
    spec = LiftSpec(grounding={"BusinessService": {"iri": f"{PROV}Entity"}})
    core = _lift(spec).require_schema().core_schema
    assert {"Evidence", "Agent"} <= core.vertex_config.vertex_set
    relations = {e.relation for e in core.edge_config.edges}
    assert "wasAttributedTo" in relations


def test_provenance_can_be_declined() -> None:
    spec = FULL_SPEC.model_copy(update={"provenance": False})
    core = _lift(spec).require_schema().core_schema
    assert "Evidence" not in core.vertex_config.vertex_set


def test_an_existing_unit_grounding_is_widened_not_replaced() -> None:
    """A unit is an addition to what a field means, not a substitute for it."""
    config = {
        "schema": {
            "metadata": {"name": "m", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Reading",
                            "properties": [
                                {"name": "id", "type": "STRING"},
                                {
                                    "name": "value",
                                    "type": "FLOAT",
                                    "semantics": {"iri": f"{SOSA}hasSimpleResult"},
                                },
                            ],
                            "identity": ["id"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    }
    spec = LiftSpec(
        grounding={"Reading": {"iri": f"{SOSA}Observation"}},
        measured={"Reading.value": "Cel"},
        provenance=False,
    )
    core = _lift(spec, config).require_schema().core_schema
    field = next(
        f for f in core.vertex_config["Reading"].properties if f.name == "value"
    )
    assert field.semantics is not None
    assert field.semantics.unit == "Cel"
    assert field.semantics.iri == f"{SOSA}hasSimpleResult"


# ── refusing rather than guessing ───────────────────────────────────────────


def test_a_schemaless_manifest_is_refused_with_a_reason() -> None:
    manifest = GraphManifest.from_config(
        {"ingestion_model": {"resources": [{"name": "r", "apply": []}]}}
    )
    manifest.finish_init()
    with pytest.raises(LiftError, match="no schema block"):
        plan_lift(manifest, LiftSpec())


def test_a_spec_naming_a_type_that_is_not_there_is_refused() -> None:
    with pytest.raises(LiftError, match="does not have"):
        plan_lift(_manifest(), LiftSpec(stateful={"Nope": ["x"]}))


def test_moving_an_identity_property_is_refused() -> None:
    """An identity that changes over time is not an identity."""
    with pytest.raises(LiftError, match="identify the type"):
        plan_lift(_manifest(), LiftSpec(stateful={"ConfigurationItem": ["ci_id"]}))


def test_a_name_the_lift_would_mint_and_that_already_exists_is_refused() -> None:
    config = {
        "schema": {
            "metadata": {"name": "m", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Evidence",
                            "properties": [{"name": "id", "type": "STRING"}],
                            "identity": ["id"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    }
    with pytest.raises(LiftError, match="already has"):
        plan_lift(_manifest(config), LiftSpec())


def test_an_identity_fallback_is_refused_when_the_authored_document_shows_it() -> None:
    """Promoting "every property is the key" to a declaration would hide the bug."""
    config = {
        "schema": {
            "metadata": {"name": "m", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": "Loose", "properties": [{"name": "a"}, {"name": "b"}]}
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    }
    manifest = _manifest(config)
    with pytest.raises(LiftError, match="identity_from_all_properties"):
        plan_lift(manifest, LiftSpec(provenance=False), authored=config)

    # Declared in the spec, it plans and the result is conformant on A2.
    spec = LiftSpec(identity={"Loose": ["a"]}, provenance=False)
    lifted = apply_evolution(manifest, plan_lift(manifest, spec, authored=config))
    result = next(a for a in _report(lifted).assertions if a.id == "declared-identity")
    assert result.status == "pass"


def test_measured_naming_a_property_that_is_not_there_is_refused() -> None:
    with pytest.raises(LiftError, match="do not exist"):
        plan_lift(_manifest(), LiftSpec(measured={"ConfigurationItem.nope": "Cel"}))


# ── the op stream is the artifact ───────────────────────────────────────────


def test_the_lift_is_a_reviewable_op_list_and_applies_nothing() -> None:
    manifest = _manifest()
    before = {
        f.name
        for f in manifest.require_schema()
        .core_schema.vertex_config["ConfigurationItem"]
        .properties
    }
    plan_lift(manifest, FULL_SPEC, authored=PLAIN)
    after = {
        f.name
        for f in manifest.require_schema()
        .core_schema.vertex_config["ConfigurationItem"]
        .properties
    }
    assert before == after, "planning must not mutate the manifest"


def test_the_destructive_step_is_last_and_separable() -> None:
    """Everything before the removal is additive, which is what makes
    ``retire: keep`` a shorter op list rather than a different one."""
    full = plan_lift(_manifest(), FULL_SPEC, authored=PLAIN)
    assert full[-1].op == "remove_vertex_properties"
    kept = plan_lift(
        _manifest(), FULL_SPEC.model_copy(update={"retire": "keep"}), authored=PLAIN
    )
    assert [op.op for op in kept] == [op.op for op in full[:-1]]


def test_the_lift_inverts_back_to_the_original() -> None:
    manifest = _manifest()
    ops = plan_lift(manifest, FULL_SPEC, authored=PLAIN)
    lifted = apply_evolution(manifest, ops)
    inverses, reasons = invert_ops(ops, manifest=manifest)
    assert reasons == [], reasons

    back = apply_evolution(lifted, inverses)
    core = back.require_schema().core_schema
    assert core.vertex_config.vertex_set == {"ConfigurationItem", "BusinessService"}
    assert {f.name for f in core.vertex_config["ConfigurationItem"].properties} == {
        "ci_id",
        "status",
        "temp_c",
    }
