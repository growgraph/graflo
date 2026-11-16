"""Single-valued relations: ``merge`` and ``exclusive`` on edges, and their history."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects import LiftError, LiftSpec, plan_lift
from graflo.architecture.schema.edge import Edge
from graflo.rdf.deserializer import ManifestRdfDeserializer
from graflo.rdf.serializer import ManifestRdfSerializer

BASE_URI = "https://growgraph.dev/manifests/test/"


def _config(runs: dict | None = None) -> dict:
    return {
        "schema": {
            "metadata": {"name": "fleet", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Unit",
                            "properties": [
                                {"name": "serial", "type": "STRING"},
                                {"name": "site", "type": "STRING"},
                            ],
                            "identity": ["serial"],
                        },
                        {
                            "name": "Release",
                            "properties": [{"name": "release_id", "type": "STRING"}],
                            "identity": ["release_id"],
                        },
                        {
                            "name": "Model",
                            "properties": [{"name": "sku", "type": "STRING"}],
                            "identity": ["sku"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "Unit",
                            "target": "Release",
                            "relation": "runs",
                            **(runs or {}),
                        },
                        {
                            "source": "Unit",
                            "target": "Model",
                            "relation": "instanceOf",
                            "merge": "replace",
                            "exclusive": True,
                        },
                    ]
                },
            },
        }
    }


def _manifest(runs: dict | None = None) -> GraphManifest:
    manifest = GraphManifest.from_config(_config(runs))
    manifest.finish_init()
    return manifest


def _edge(manifest: GraphManifest, relation: str) -> Edge:
    core = manifest.require_schema().core_schema
    (edge,) = [e for e in core.edge_config.edges if e.relation == relation]
    return edge


def test_an_edge_accumulates_targets_unless_it_says_otherwise() -> None:
    runs = _edge(_manifest(), "runs")
    assert (runs.merge, runs.exclusive) == ("union", False)


def test_a_single_valued_edge_cannot_declare_parallel_identities() -> None:
    with pytest.raises(ValueError, match="cannot declare identities"):
        Edge(
            source="Unit",
            target="Release",
            relation="runs",
            merge="replace",
            identities=[["source", "target", "since"]],
        )


def test_exclusive_needs_one_value() -> None:
    with pytest.raises(ValueError, match="exclusive needs merge 'replace'"):
        Edge(source="Unit", target="Model", relation="instanceOf", exclusive=True)


def test_the_defaults_leave_the_canonical_form_unchanged() -> None:
    edges = _manifest().to_minimal_canonical_dict()["schema"]["core_schema"]
    runs = next(e for e in edges["edge_config"]["edges"] if e.get("relation") == "runs")
    assert "merge" not in runs and "exclusive" not in runs


def test_merge_and_exclusive_survive_the_rdf_round_trip() -> None:
    manifest = _manifest({"merge": "replace"})
    turtle = ManifestRdfSerializer(include_ontology=False).to_turtle(manifest, BASE_URI)
    back = ManifestRdfDeserializer().from_turtle(turtle, BASE_URI.rstrip("/"))
    back.finish_init()
    assert _edge(back, "runs").merge == "replace"
    model = _edge(back, "instanceOf")
    assert (model.merge, model.exclusive) == ("replace", True)
    assert back.to_minimal_canonical_dict() == manifest.to_minimal_canonical_dict()


def test_a_single_valued_relation_keeps_its_history_on_the_state() -> None:
    manifest = _manifest({"merge": "replace"})
    spec = LiftSpec(stateful={"Unit": ["site", "runs"]})
    lifted = apply_evolution(manifest, plan_lift(manifest, spec, aspects=["state"]))
    core = lifted.require_schema().core_schema
    state = core.vertex_config["UnitState"]
    assert {"serial", "site", "runs", "valid_from", "valid_to"} <= set(
        state.property_names
    )
    # The property moves off the entity; the relation stays as the current value.
    assert "site" not in core.vertex_config["Unit"].property_names
    assert _edge(lifted, "runs").merge == "replace"


def test_a_relation_with_many_targets_has_no_history_to_keep() -> None:
    manifest = _manifest()
    spec = LiftSpec(stateful={"Unit": ["runs"]})
    with pytest.raises(LiftError, match="merge 'union'"):
        plan_lift(manifest, spec, aspects=["state"])


def test_replace_survives_a_round_trip_with_every_field_written() -> None:
    """``to_dict(skip_defaults=False)`` writes ``identities: []``, which is no key."""
    edge = Edge.model_validate(
        {"source": "Unit", "target": "Release", "relation": "runs", "merge": "replace"}
    )
    again = Edge.model_validate(edge.to_dict(skip_defaults=False))
    assert again.merge == "replace"
    assert again.identities == []
