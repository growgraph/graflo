"""Tests for :func:`land_rdf_facts`."""

from __future__ import annotations

from pathlib import Path

import pytest

from graflo.architecture.contract.bindings import SparqlConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.rdf_inferencer import RdfInferenceManager
from graflo.hq.rdf_landing import fact_classes, land_rdf_facts

ONTOLOGY = Path(__file__).parents[1] / "data" / "rdf" / "sample_ontology.ttl"

FACTS = """\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:  <http://example.org/> .

ex:alice a ex:Person ; ex:name "Alice" ; ex:worksFor ex:acme .
ex:bob   a ex:Person ; ex:knows ex:alice .
ex:acme  a ex:Organization ; ex:orgName "Acme" .
ex:p1    a ex:Project .
ex:s1    a rdf:Statement .
"""


@pytest.fixture()
def facts(tmp_path: Path) -> Path:
    path = tmp_path / "facts.ttl"
    path.write_text(FACTS)
    return path


def _sparql_connectors(manifest: GraphManifest) -> list[SparqlConnector]:
    return [
        c
        for c in manifest.require_bindings().connectors
        if isinstance(c, SparqlConnector)
    ]


def _schema_only(ontology: Path) -> GraphManifest:
    schema, ingestion_model = RdfInferenceManager().infer_schema(ontology)
    return GraphManifest(graph_schema=schema, ingestion_model=ingestion_model)


def test_fact_classes_counts_domain_classes_only(facts: Path) -> None:
    assert [(c.name, c.instances) for c in fact_classes(facts)] == [
        ("Organization", 1),
        ("Person", 2),
        ("Project", 1),
    ]


def test_a_manifest_without_a_schema_starts_from_the_ontology(facts: Path) -> None:
    landing = land_rdf_facts(None, facts, ontology=ONTOLOGY)

    vertices = {
        v.name
        for v in landing.manifest.require_schema().core_schema.vertex_config.vertices
    }
    assert vertices == {"Person", "Organization"}
    assert [c.name for c in landing.bound] == ["Organization", "Person"]
    assert [c.name for c in landing.unbound] == ["Project"]
    assert {c.rdf_file for c in _sparql_connectors(landing.manifest)} == {facts}


def test_a_manifest_without_a_schema_needs_the_ontology(facts: Path) -> None:
    with pytest.raises(ValueError, match="ontology is required"):
        land_rdf_facts(None, facts)


def test_facts_bind_to_the_resources_a_manifest_already_has(facts: Path) -> None:
    landing = land_rdf_facts(_schema_only(ONTOLOGY), facts)

    bindings = landing.manifest.require_bindings()
    for name in ("Person", "Organization"):
        (connector,) = bindings.get_connectors_for_resource(name)
        assert isinstance(connector, SparqlConnector)
        assert connector.rdf_file == facts
        assert connector.rdf_class == f"http://example.org/{name}"
    assert [c.name for c in landing.unbound] == ["Project"]


def test_landing_again_replaces_the_connector(facts: Path, tmp_path: Path) -> None:
    first = land_rdf_facts(_schema_only(ONTOLOGY), facts).manifest
    newer = tmp_path / "newer.ttl"
    newer.write_text(FACTS)

    again = land_rdf_facts(first, newer).manifest

    assert len(_sparql_connectors(again)) == len(_sparql_connectors(first))
    (person,) = again.require_bindings().get_connectors_for_resource("Person")
    assert isinstance(person, SparqlConnector)
    assert person.rdf_file == newer
