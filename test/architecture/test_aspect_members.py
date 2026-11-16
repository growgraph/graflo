"""Which parts of a lifted schema an aspect added, read back from the schema alone."""

from __future__ import annotations

import copy

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects import (
    AspectEdge,
    LiftSpec,
    aspect_members,
    plan_lift,
)
from test.architecture.test_lift import FULL_SPEC, PLAIN, _lift
from test.architecture.test_provenance_aspect import TICKETS

PROV = "http://www.w3.org/ns/prov#"


def _members(manifest: GraphManifest):
    return aspect_members(manifest.require_schema())


def _plain(config: dict) -> GraphManifest:
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    return manifest


def _tickets_lifted() -> GraphManifest:
    spec = LiftSpec(provenance_for=["Claim", "Ticket"], mentions_for=["Ticket"])
    manifest = _plain(TICKETS)
    return apply_evolution(manifest, plan_lift(manifest, spec, aspects=["provenance"]))


def test_an_unlifted_schema_owes_nothing_to_an_aspect() -> None:
    members = _members(_plain(PLAIN))
    assert members.types == {}
    assert members.edges == []
    assert members.lifted_onto == {}


def test_provenance_types_relations_and_the_types_it_was_lifted_onto() -> None:
    members = _members(_tickets_lifted())
    assert members.types == {
        "SourceRecord": "provenance",
        "Agent": "provenance",
        "Assertion": "provenance",
    }
    assert {(e.source, e.relation, e.target) for e in members.edges} == {
        ("Claim", "wasDerivedFrom", "SourceRecord"),
        ("Ticket", "wasDerivedFrom", "SourceRecord"),
        ("SourceRecord", "mentions", "Ticket"),
        ("SourceRecord", "wasAttributedTo", "Agent"),
        ("Assertion", "wasDerivedFrom", "SourceRecord"),
        ("Assertion", "subject", "Ticket"),
        ("Assertion", "object", "Ticket"),
    }
    assert {e.aspect for e in members.edges} == {"provenance"}
    assert members.lifted_onto == {"Claim": ["provenance"], "Ticket": ["provenance"]}


def test_state_and_provenance_together() -> None:
    members = _members(_lift())
    assert members.types["MachineState"] == "state"
    assert members.types["MachineObservation"] == "state"
    assert (
        AspectEdge(
            source="MachineState",
            relation="specializationOf",
            target="Machine",
            aspect="state",
        )
        in members.edges
    )
    assert (
        AspectEdge(
            source="MachineObservation",
            relation="hasFeatureOfInterest",
            target="Machine",
            aspect="state",
        )
        in members.edges
    )
    assert "state" in members.lifted_onto["Machine"]


def test_a_domain_type_grounded_in_prov_is_not_an_aspect_type() -> None:
    # FULL_SPEC grounds Machine and ProductionLine as prov:Entity, as SourceRecord is.
    members = _members(_lift(FULL_SPEC))
    assert "Machine" not in members.types
    assert "ProductionLine" not in members.types


def test_a_domain_relation_grounded_in_prov_is_not_an_aspect_relation() -> None:
    # FULL_SPEC grounds installed_on as prov:wasInfluencedBy.
    members = _members(_lift(FULL_SPEC))
    assert not [e for e in members.edges if e.relation == "installed_on"]


def test_a_relation_with_another_grounding_is_not_counted() -> None:
    config = copy.deepcopy(TICKETS)
    # A relation with the aspect's name and ends but another IRI is someone else's.
    config["schema"]["graph"]["vertex_config"]["vertices"].append(
        {
            "name": "SourceRecord",
            "properties": [{"name": "source_record_id", "type": "STRING"}],
            "identity": ["source_record_id"],
        }
    )
    config["schema"]["graph"]["edge_config"]["edges"].append(
        {
            "source": "Ticket",
            "target": "SourceRecord",
            "relation": "wasDerivedFrom",
            "semantics": {"iri": f"{PROV}wasInfluencedBy"},
        }
    )
    members = _members(_plain(config))
    assert members.edges == []
    assert members.types == {}
    assert members.lifted_onto == {}
