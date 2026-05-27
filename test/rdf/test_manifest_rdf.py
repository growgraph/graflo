"""Tests for GraphManifest RDF round-trip conversion."""

from __future__ import annotations

import pathlib

import yaml
from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF

from graflo import GraphManifest
from graflo.architecture.database_features import EdgePhysicalSpec
from graflo.architecture.graph_types import Index
from graflo.rdf import namespace as ns
from graflo.rdf.deserializer import ManifestRdfDeserializer
from graflo.rdf.serializer import ManifestRdfSerializer
from graflo.rdf.utils import load_ontology_graph, ontology_path


EXAMPLES_DIR = pathlib.Path(__file__).resolve().parents[2] / "examples"
BASE_URI = "https://growgraph.dev/manifests/test/"


def _load_example_manifest(name: str) -> GraphManifest:
    path = EXAMPLES_DIR / name / "manifest.yaml"
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return GraphManifest.from_dict(data)


def _canonical(manifest: GraphManifest) -> dict:
    return manifest.to_minimal_canonical_dict()


def test_ontology_file_exists_and_loads() -> None:
    path = ontology_path()
    assert path.is_file()
    graph = load_ontology_graph()
    ontology_uri = URIRef(ns.GF_ONTOLOGY_IRI)
    assert (ontology_uri, RDF.type, OWL.Ontology) in graph
    assert (ontology_uri, OWL.versionIRI, URIRef(ns.GF_VERSION_IRI)) in graph
    version_info = next(graph.objects(ontology_uri, OWL.versionInfo), None)
    assert version_info is not None
    assert str(version_info) == ns.GF_VERSION


def test_manifest_to_rdf_contains_core_triples() -> None:
    manifest = _load_example_manifest("1-ingest-csv")
    serializer = ManifestRdfSerializer(include_ontology=False)
    graph = serializer.to_graph(manifest, BASE_URI)

    manifest_uri = URIRef(BASE_URI.rstrip("/"))
    assert (manifest_uri, RDF.type, ns.GraphManifest) in graph
    assert (manifest_uri, ns.hasSchema, None) is not True
    assert len(list(graph.objects(manifest_uri, ns.hasSchema))) == 1
    assert len(list(graph.objects(manifest_uri, ns.hasIngestionModel))) == 1
    assert len(list(graph.objects(manifest_uri, ns.hasBindings))) == 1


def test_round_trip_example_1_ingest_csv() -> None:
    original = _load_example_manifest("1-ingest-csv")
    serializer = ManifestRdfSerializer(include_ontology=False)
    deserializer = ManifestRdfDeserializer()

    ttl = serializer.to_turtle(original, BASE_URI)
    restored = deserializer.from_turtle(ttl, BASE_URI.rstrip("/"))

    assert _canonical(restored) == _canonical(original)


def test_round_trip_example_2_with_transforms() -> None:
    original = _load_example_manifest("2-ingest-self-references")
    serializer = ManifestRdfSerializer(include_ontology=False)
    deserializer = ManifestRdfDeserializer()

    graph = serializer.to_graph(original, BASE_URI)
    restored = deserializer.from_graph(graph, BASE_URI.rstrip("/"))

    assert _canonical(restored) == _canonical(original)


def test_round_trip_example_3_edge_weights() -> None:
    original = _load_example_manifest("3-ingest-csv-edge-weights")
    serializer = ManifestRdfSerializer(include_ontology=False)
    deserializer = ManifestRdfDeserializer()

    ttl = serializer.to_turtle(original, BASE_URI)
    restored = deserializer.from_turtle(ttl, BASE_URI.rstrip("/"))

    assert _canonical(restored) == _canonical(original)


def test_turtle_output_serializes_with_ontology() -> None:
    manifest = _load_example_manifest("1-ingest-csv")
    serializer = ManifestRdfSerializer(include_ontology=True)
    ttl = serializer.to_turtle(manifest, BASE_URI)

    graph = Graph()
    graph.parse(data=ttl, format="turtle")
    manifest_uri = URIRef(BASE_URI.rstrip("/"))
    assert (manifest_uri, RDF.type, ns.GraphManifest) in graph


def test_json_ld_output_is_parseable() -> None:
    manifest = _load_example_manifest("1-ingest-csv")
    serializer = ManifestRdfSerializer(include_ontology=False)
    payload = serializer.to_json_ld(manifest, BASE_URI)

    graph = Graph()
    graph.parse(data=payload, format="json-ld")
    restored = ManifestRdfDeserializer().from_graph(graph, BASE_URI.rstrip("/"))
    assert _canonical(restored) == _canonical(manifest)


def test_round_trip_preserves_vertex_config_policy_fields() -> None:
    original = _load_example_manifest("1-ingest-csv")
    assert original.graph_schema is not None
    original.graph_schema.core_schema.vertex_config.force_types = {
        "Person": ["STRING", "INT"]
    }
    original.graph_schema.core_schema.vertex_config.identity_from_all_properties = False

    serializer = ManifestRdfSerializer(include_ontology=False)
    deserializer = ManifestRdfDeserializer()
    restored = deserializer.from_graph(
        serializer.to_graph(original, BASE_URI), BASE_URI.rstrip("/")
    )

    assert restored.graph_schema is not None
    restored_vertex_cfg = restored.graph_schema.core_schema.vertex_config
    assert restored_vertex_cfg.force_types == {"Person": ["STRING", "INT"]}
    assert restored_vertex_cfg.identity_from_all_properties is False


def test_context_has_new_vertex_config_and_label_terms() -> None:
    context_path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "graflo"
        / "rdf"
        / "ontology"
        / "graflo-context.jsonld"
    )
    payload = context_path.read_text(encoding="utf-8")
    assert '"forceTypes": "gf:forceTypes"' in payload
    assert '"identityFromAllProperties": "gf:identityFromAllProperties"' in payload
    assert '"prefLabel": "skos:prefLabel"' in payload


def test_profile_and_transform_actor_semantic_links_are_emitted() -> None:
    manifest = _load_example_manifest("2-ingest-self-references")
    assert manifest.graph_schema is not None
    manifest.graph_schema.db_profile.vertex_indexes = {
        "Person": [Index(fields=["name"])]
    }
    manifest.graph_schema.db_profile.edge_specs = [
        EdgePhysicalSpec(
            source="Person",
            target="Person",
            relation="follows",
            indexes=[Index(fields=["created_at"])],
        )
    ]

    graph = ManifestRdfSerializer(include_ontology=False).to_graph(manifest, BASE_URI)

    profile_nodes = list(graph.subjects(RDF.type, ns.DatabaseProfile))
    assert profile_nodes
    profile_node = profile_nodes[0]
    vertex_index_nodes = list(graph.objects(profile_node, ns.hasVertexIndex))
    edge_spec_nodes = list(graph.objects(profile_node, ns.hasEdgeSpec))
    assert vertex_index_nodes
    assert edge_spec_nodes
    assert any(graph.objects(vertex_index_nodes[0], ns.indexField))
    assert any(graph.objects(edge_spec_nodes[0], ns.hasIndex))

    transform_actor_nodes = list(graph.subjects(RDF.type, ns.TransformActorStep))
    assert transform_actor_nodes
    assert any(
        any(graph.objects(node, ns.executesTransform)) for node in transform_actor_nodes
    )
