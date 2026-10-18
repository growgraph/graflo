"""The provenance aspect on its own: lineage becomes expressible, nothing else moves."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution, invert_ops
from graflo.architecture.evolution.aspects import LiftError, LiftSpec, plan_lift
from graflo.architecture.evolution.aspects.provenance import plan_provenance

PROV = "http://www.w3.org/ns/prov#"

TICKETS: dict = {
    "schema": {
        "metadata": {"name": "support", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Ticket",
                        "properties": [{"name": "key", "type": "STRING"}],
                        "identity": ["key"],
                    },
                    {
                        "name": "Claim",
                        "properties": [{"name": "claim_id", "type": "STRING"}],
                        "identity": ["claim_id"],
                    },
                ]
            },
            "edge_config": {"edges": []},
        },
    }
}


def _manifest(config: dict = TICKETS) -> GraphManifest:
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    return manifest


def test_provenance_alone_adds_exactly_the_two_types_and_the_attribution() -> None:
    ops = plan_provenance(_manifest())
    assert [op.op for op in ops] == ["add_vertices", "add_edges"]
    core = apply_evolution(_manifest(), ops).require_schema().core_schema
    assert core.vertex_config.vertex_set == {
        "Ticket",
        "Claim",
        "SourceRecord",
        "Agent",
    }
    (edge,) = core.edge_config.edges
    assert (edge.source, edge.relation, edge.target) == (
        "SourceRecord",
        "wasAttributedTo",
        "Agent",
    )
    assert [f.name for f in edge.properties] == ["role"]


def test_the_source_record_carries_an_address_and_a_hash_never_content() -> None:
    core = apply_evolution(_manifest(), plan_provenance(_manifest()))
    record = core.require_schema().core_schema.vertex_config["SourceRecord"]
    assert {f.name for f in record.properties} == {
        "source_record_id",
        "source_uri",
        "media_type",
        "content_hash",
        "retrieved_at",
    }
    assert record.identity == ["source_record_id"]
    assert record.semantics is not None
    assert record.semantics.iri == f"{PROV}Entity"


def test_attach_names_the_types_that_derive_from_a_source_record() -> None:
    ops = plan_provenance(_manifest(), attach=["Claim"])
    core = apply_evolution(_manifest(), ops).require_schema().core_schema
    relations = {(e.source, e.relation, e.target) for e in core.edge_config.edges}
    assert ("Claim", "wasDerivedFrom", "SourceRecord") in relations
    assert ("Ticket", "wasDerivedFrom", "SourceRecord") not in relations


def test_the_composed_lift_with_provenance_only_reads_provenance_for() -> None:
    spec = LiftSpec(provenance_for=["Claim"])
    ops = plan_lift(_manifest(), spec, aspects=["provenance"])
    core = apply_evolution(_manifest(), ops).require_schema().core_schema
    assert "SourceRecord" in core.vertex_config.vertex_set
    assert not any(name.endswith("State") for name in core.vertex_config.vertex_set)
    relations = {(e.source, e.relation) for e in core.edge_config.edges}
    assert ("Claim", "wasDerivedFrom") in relations


def test_provenance_for_an_unknown_type_is_refused() -> None:
    with pytest.raises(LiftError, match="does not have"):
        plan_lift(
            _manifest(), LiftSpec(provenance_for=["Nope"]), aspects=["provenance"]
        )


def test_a_name_the_aspect_would_mint_and_that_already_exists_is_refused() -> None:
    config = {
        "schema": {
            "metadata": {"name": "m", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Agent",
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
        plan_provenance(_manifest(config))


def test_an_unknown_aspect_is_refused_by_name() -> None:
    with pytest.raises(LiftError, match="no aspect named"):
        plan_lift(_manifest(), aspects=["access"])


def test_the_provenance_lift_inverts_back_to_the_original() -> None:
    manifest = _manifest()
    ops = plan_provenance(manifest, attach=["Claim"])
    lifted = apply_evolution(manifest, ops)
    inverses, reasons = invert_ops(ops, manifest=manifest)
    assert reasons == [], reasons
    back = apply_evolution(lifted, inverses)
    assert back.require_schema().core_schema.vertex_config.vertex_set == {
        "Ticket",
        "Claim",
    }
