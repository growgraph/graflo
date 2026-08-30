"""Tests for GraphManifest RDF round-trip conversion."""

from __future__ import annotations

import inspect
import json
import pathlib

import pytest
import yaml
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from graflo.architecture import GraphManifest
from graflo.architecture.contract.bindings.connectors import (
    BoundSourceKind,
    ResourceConnector,
)
from graflo.architecture.graph_types import Index
from graflo.architecture.schema.database_features import EdgePhysicalSpec
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
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


def _sample_funnel() -> IdentityFunnel:
    """Two-branch funnel: a strong unary key, then a weaker composite fallback."""
    return IdentityFunnel(
        branches=[
            IdentityBranch(id="by_isin", fields=["isin"]),
            IdentityBranch(
                id="by_name", when_all_present=["name", "sid"], fields=["name", "sid"]
            ),
        ]
    )


def _round_trip(manifest: GraphManifest) -> GraphManifest:
    serializer = ManifestRdfSerializer(include_ontology=False)
    deserializer = ManifestRdfDeserializer()
    return deserializer.from_turtle(
        serializer.to_turtle(manifest, BASE_URI), BASE_URI.rstrip("/")
    )


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


def test_context_has_identity_mode_terms() -> None:
    context_path = pathlib.Path(ontology_path()).parent / "graflo-context.jsonld"
    payload = context_path.read_text(encoding="utf-8")
    assert '"assigned"' in payload
    assert '"hasHashIdentity"' in payload
    assert '"hasSecondaryIdentity"' in payload
    assert '"SecondaryIdentity": "gf:SecondaryIdentity"' in payload
    assert '"hasIdentityFunnel"' in payload
    assert '"hasIdentityBranch"' in payload
    assert '"IdentityFunnel": "gf:IdentityFunnel"' in payload


def test_round_trip_example_16_secondary_identities() -> None:
    original = _load_example_manifest("16-secondary-identities")
    restored = _round_trip(original)

    assert _canonical(restored) == _canonical(original)

    vertices = restored.graph_schema.core_schema.vertex_config.vertices
    by_isin = vertices[0].secondary_identities[0]
    assert by_isin.name == "by_isin"
    assert by_isin.fields == ["isin"]


@pytest.mark.parametrize(
    "mutate, expected_mode",
    [
        (lambda v: None, "natural"),
        (lambda v: setattr(v, "hash_identity_properties", ["isin", "name"]), "hash"),
        (lambda v: setattr(v, "blank", True), "blank"),
        (
            lambda v: (setattr(v, "assigned", True), setattr(v, "identity", [])),
            "assigned",
        ),
        (
            lambda v: setattr(v, "identity_funnel", _sample_funnel()),
            "hash",
        ),
    ],
    ids=["natural", "hash", "blank", "assigned", "funnel"],
)
def test_round_trip_preserves_vertex_identity_modes(mutate, expected_mode) -> None:
    """All four identity modes survive RDF. Previously everything read back as natural."""
    original = _load_example_manifest("16-secondary-identities")
    vertex = original.graph_schema.core_schema.vertex_config.vertices[0]
    # blank and assigned vertices reject secondary identities.
    vertex.secondary_identities = []
    mutate(vertex)
    assert vertex.identity_mode == expected_mode

    restored = _round_trip(original)

    restored_vertex = restored.graph_schema.core_schema.vertex_config.vertices[0]
    assert restored_vertex.identity_mode == expected_mode
    assert restored_vertex.hash_identity_properties == vertex.hash_identity_properties


def test_round_trip_preserves_identity_funnel() -> None:
    """Branch order and conditions decide the key, so both must survive RDF."""
    original = _load_example_manifest("16-secondary-identities")
    vertex = original.graph_schema.core_schema.vertex_config.vertices[0]
    vertex.secondary_identities = []
    vertex.identity_funnel = _sample_funnel()

    restored = _round_trip(original)

    funnel = restored.graph_schema.core_schema.vertex_config.vertices[0].identity_funnel
    assert funnel is not None
    assert funnel.digest == "sha256"
    assert funnel.include_branch_id is True
    assert funnel.branch_ids == ["by_isin", "by_name"]
    assert funnel.branches[0].fields == ["isin"]
    # Absent condition must read back as None ("default to fields"), not as [].
    assert funnel.branches[0].when_all_present is None
    assert funnel.branches[1].when_all_present == ["name", "sid"]


def test_round_trip_preserves_identity_field_order() -> None:
    """Identity lists are positional; RDF triples are not, hence gf:artifactIndex."""
    original = _load_example_manifest("16-secondary-identities")
    vertex = original.graph_schema.core_schema.vertex_config.vertices[0]
    vertex.identity = ["sid", "isin", "name"]

    restored = _round_trip(original)

    restored_vertex = restored.graph_schema.core_schema.vertex_config.vertices[0]
    assert restored_vertex.identity == ["sid", "isin", "name"]


def test_round_trip_preserves_undirected_edge() -> None:
    """`directed` is a first-class predicate, not an opaque payload key."""
    original = _load_example_manifest("2-ingest-self-references")
    assert original.graph_schema is not None
    edges = original.graph_schema.core_schema.edge_config.edges
    edges[0].directed = False

    graph = ManifestRdfSerializer(include_ontology=False).to_graph(original, BASE_URI)
    edge_nodes = list(graph.subjects(RDF.type, ns.Edge))
    assert edge_nodes
    directed_literals = [
        obj
        for node in edge_nodes
        for obj in graph.objects(node, ns.edgeDirected)
        if isinstance(obj, Literal)
    ]
    assert [bool(literal.toPython()) for literal in directed_literals] == [False]

    restored = _round_trip(original)
    assert restored.graph_schema is not None
    restored_edges = restored.graph_schema.core_schema.edge_config.edges
    assert restored_edges[0].directed is False


def test_undirected_edge_is_also_written_to_the_legacy_payload() -> None:
    """A reader older than ontology 1.3.0 knows only ``gf:edgePayload``.

    Without the duplicate it would see no direction at all and silently treat
    the edge as directed — data loss the version bump cannot warn about.
    """
    original = _load_example_manifest("2-ingest-self-references")
    assert original.graph_schema is not None
    original.graph_schema.core_schema.edge_config.edges[0].directed = False

    graph = ManifestRdfSerializer(include_ontology=False).to_graph(original, BASE_URI)
    payloads = [
        json.loads(str(obj)) for _, obj in graph.subject_objects(ns.edgePayload)
    ]
    assert {"directed": False} in payloads


def test_directed_edges_emit_no_direction_triple() -> None:
    """True is the default; keep the common case out of the graph."""
    manifest = _load_example_manifest("2-ingest-self-references")
    graph = ManifestRdfSerializer(include_ontology=False).to_graph(manifest, BASE_URI)
    assert not list(graph.subject_objects(ns.edgeDirected))

    restored = _round_trip(manifest)
    assert restored.graph_schema is not None
    for edge in restored.graph_schema.core_schema.edge_config.edges:
        assert edge.directed is True


def test_legacy_edge_payload_still_carries_direction() -> None:
    """Graphs written before ``gf:edgeDirected`` existed must keep loading."""
    manifest = _load_example_manifest("2-ingest-self-references")
    graph = ManifestRdfSerializer(include_ontology=False).to_graph(manifest, BASE_URI)
    edge_node = next(iter(graph.subjects(RDF.type, ns.Edge)))
    graph.add((edge_node, ns.edgePayload, Literal('{"directed": false}')))

    restored = ManifestRdfDeserializer().from_turtle(
        graph.serialize(format="turtle"), BASE_URI.rstrip("/")
    )
    assert restored.graph_schema is not None
    undirected = [
        e for e in restored.graph_schema.core_schema.edge_config.edges if not e.directed
    ]
    assert len(undirected) == 1


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


# ------------------------------------------------------- connector vocabulary drift
#
# `gf:APIConnector` and `gf:KafkaConnector` were absent from the ontology and from
# the `ns.CONNECTOR_*` registries long after the Python models existed, so
# `to_graph` raised ``KeyError: 'APIConnector'`` on any manifest using one. These
# guards enumerate the Python side and demand the RDF side keep up, so the next
# connector fails here instead of at a user's serialization call.


def _concrete_connectors() -> list[type]:
    seen: list[type] = []

    def walk(cls: type) -> None:
        for sub in cls.__subclasses__():
            if not inspect.isabstract(sub):
                seen.append(sub)
            walk(sub)

    walk(ResourceConnector)
    return sorted(seen, key=lambda cls: cls.__name__)


def test_every_connector_model_is_registered_and_declared() -> None:
    ontology = load_ontology_graph()
    connectors = _concrete_connectors()
    assert {cls.__name__ for cls in connectors} >= {
        "FileConnector",
        "TableConnector",
        "SparqlConnector",
        "APIConnector",
        "KafkaConnector",
    }

    for cls in connectors:
        name = cls.__name__
        assert name in ns.CONNECTOR_CLASSES, name
        assert name in ns.CONNECTOR_MODELS, name
        assert ns.CONNECTOR_MODELS[name] is cls, name
        rdf_type = URIRef(str(ns.CONNECTOR_CLASSES[name]))
        assert ns.CONNECTOR_CLASS_BY_RDF_TYPE.get(rdf_type) == name, name
        assert (rdf_type, RDF.type, OWL.Class) in ontology, name
        assert (rdf_type, RDFS.subClassOf, ns.BoundConnector) in ontology, name


def test_every_bound_source_kind_has_an_individual() -> None:
    ontology = load_ontology_graph()
    for member in BoundSourceKind:
        individual = ns.BOUND_SOURCE_KIND_INDIVIDUALS.get(member.value)
        assert individual is not None, member.value
        uri = URIRef(str(individual))
        assert (uri, RDF.type, ns.GF.BoundSourceKind) in ontology, member.value
        assert (uri, ns.enumValue, Literal(member.value)) in ontology, member.value


def _manifest_with_one_connector_of_each_kind() -> GraphManifest:
    path = EXAMPLES_DIR / "1-ingest-csv" / "manifest.yaml"
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    # The example wires two file connectors to named resources; replace the whole
    # bindings block so the wiring does not have to be kept in sync here.
    data["bindings"] = {
        "connectors": [
            {"name": "file1", "regex": r".*\.csv", "resource_name": "r0"},
            {"name": "table1", "table_name": "people", "resource_name": "r1"},
            {
                "name": "sparql1",
                "rdf_class": "http://example.org/Person",
                "resource_name": "r2",
            },
            {"name": "api1", "path": "/api/users", "resource_name": "r3"},
            {
                "name": "kafka1",
                "topics": ["events"],
                "group_id": "g1",
                "resource_name": "r4",
            },
        ]
    }
    return GraphManifest.from_dict(data)


def test_round_trip_preserves_every_connector_kind() -> None:
    original = _manifest_with_one_connector_of_each_kind()
    assert [type(item).__name__ for item in original.bindings.connectors] == [
        "FileConnector",
        "TableConnector",
        "SparqlConnector",
        "APIConnector",
        "KafkaConnector",
    ]

    restored = _round_trip(original)
    by_name = {item.name: item for item in restored.bindings.connectors}
    assert set(by_name) == {"file1", "table1", "sparql1", "api1", "kafka1"}
    assert [type(item).__name__ for item in restored.bindings.connectors] == [
        type(item).__name__ for item in original.bindings.connectors
    ]

    assert by_name["file1"].regex == r".*\.csv"
    assert by_name["table1"].table_name == "people"
    assert by_name["sparql1"].rdf_class == "http://example.org/Person"
    assert by_name["api1"].path == "/api/users"
    assert by_name["kafka1"].topics == ["events"]
    assert by_name["kafka1"].group_id == "g1"


def test_each_connector_carries_its_bound_source_kind() -> None:
    manifest = _manifest_with_one_connector_of_each_kind()
    graph = ManifestRdfSerializer(include_ontology=False).to_graph(manifest, BASE_URI)

    emitted = {str(obj) for _, obj in graph.subject_objects(ns.boundSourceKind)}
    expected = {
        str(ns.BOUND_SOURCE_KIND_INDIVIDUALS[item.bound_source_kind().value])
        for item in manifest.bindings.connectors
    }
    assert emitted == expected
    assert len(expected) == len(manifest.bindings.connectors)


def test_context_maps_every_connector_class() -> None:
    with (ontology_path().parent / "graflo-context.jsonld").open(
        encoding="utf-8"
    ) as handle:
        context = json.load(handle)["@context"]
    for cls in _concrete_connectors():
        assert context.get(cls.__name__) == f"gf:{cls.__name__}", cls.__name__


def _grounded_manifest() -> GraphManifest:
    """Manifest carrying a semantics block at all four attachment points."""
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {
                    "name": "grounded",
                    "version": "1.0.0",
                    "semantics": {"iri": "https://schema.org/Dataset"},
                },
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "person",
                                "properties": [
                                    "email",
                                    {
                                        "name": "speed",
                                        "type": "FLOAT",
                                        "semantics": {
                                            "unit": "m/s",
                                            "iri": "https://qudt.org/vocab/quantitykind/Speed",
                                        },
                                    },
                                ],
                                "identity": ["email"],
                                "semantics": {
                                    "iri": "https://schema.org/Person",
                                    "exact_match": ["http://xmlns.com/foaf/0.1/Person"],
                                    "synonyms": ["human", "individual"],
                                },
                            },
                            {
                                "name": "company",
                                "properties": ["tax_id"],
                                "identity": ["tax_id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "person",
                                "target": "company",
                                "relation": "works_at",
                                "semantics": {"iri": "https://schema.org/worksFor"},
                            }
                        ]
                    },
                },
            }
        }
    )


def test_semantics_round_trips_at_every_attachment_point() -> None:
    manifest = _grounded_manifest()
    ttl = ManifestRdfSerializer().to_turtle(manifest, BASE_URI)
    restored = ManifestRdfDeserializer().from_turtle(ttl, BASE_URI)
    assert _canonical(restored) == _canonical(manifest)


def test_semantics_emits_expected_triples() -> None:
    graph = ManifestRdfSerializer(include_ontology=False).to_graph(
        _grounded_manifest(), BASE_URI
    )
    person = URIRef(f"{BASE_URI}schema/core/vertex/person")
    assert (person, ns.semanticIri, URIRef("https://schema.org/Person")) in graph
    assert (
        person,
        ns.exactMatch,
        URIRef("http://xmlns.com/foaf/0.1/Person"),
    ) in graph
    assert set(graph.objects(person, ns.altLabel)) == {
        Literal("human"),
        Literal("individual"),
    }
    speed = URIRef(f"{BASE_URI}schema/core/vertex/person/field/speed")
    assert (speed, ns.unit, Literal("m/s")) in graph


def test_schemas_without_semantics_emit_no_semantic_triples() -> None:
    """Absent blocks stay absent — the vocabulary costs nothing when unused."""
    graph = ManifestRdfSerializer(include_ontology=False).to_graph(
        _load_example_manifest("1-ingest-csv"), BASE_URI
    )
    assert not list(graph.subjects(ns.semanticIri, None))
    assert not list(graph.subjects(ns.unit, None))


def test_ontology_declares_semantic_terms() -> None:
    ontology = load_ontology_graph()
    assert (ns.semanticIri, RDF.type, OWL.ObjectProperty) in ontology
    assert (ns.unit, RDF.type, OWL.DatatypeProperty) in ontology
    # Domain-free on purpose: the docs viz derives its grouping from domain/range
    # structure, and these terms attach to four unrelated classes.
    assert not list(ontology.objects(ns.semanticIri, RDFS.domain))


def test_context_has_semantic_terms() -> None:
    payload = (
        pathlib.Path(ontology_path()).parent / "graflo-context.jsonld"
    ).read_text(encoding="utf-8")
    assert '"semanticIri"' in payload
    assert '"unit": "gf:unit"' in payload
    assert '"altLabel": "skos:altLabel"' in payload


# ----------------------------------------------------------------------
# Declared naming convention (1.5.0)
# ----------------------------------------------------------------------


def _named_manifest(**naming: object) -> GraphManifest:
    """A minimal manifest carrying a naming declaration."""
    return GraphManifest(
        schema={
            "metadata": {"name": "shop", "naming": naming},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Customer",
                            "properties": [{"name": "id"}],
                            "identity": ["id"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    )


def test_naming_convention_round_trips() -> None:
    """A declared convention must survive RDF, or it is worse than absent.

    A block that silently disappears on the way through leaves a consumer
    believing the schema declared nothing, which is the same failure mode as
    the field types that used to be dropped for want of an individual.
    """
    manifest = _named_manifest(
        vertex_case="pascal",
        relation_case="camel",
        property_case="snake",
        singular_vertex_names=False,
    )
    ttl = ManifestRdfSerializer().to_turtle(manifest, BASE_URI)
    restored = ManifestRdfDeserializer().from_turtle(ttl, BASE_URI)
    assert _canonical(restored) == _canonical(manifest)


@pytest.mark.parametrize(
    "case", ["pascal", "camel", "snake", "upper_snake", "kebab", "preserve"]
)
def test_every_name_case_has_an_individual(case: str) -> None:
    """Exhaustive by construction: an unmapped enum emits nothing at all.

    ``add_enum_individual`` is silent when a value has no individual, which is
    how ``UUID`` and ``LIST`` came to vanish from field types unnoticed.
    """
    assert case in ns.NAME_CASE_INDIVIDUALS
    manifest = _named_manifest(vertex_case=case, relation_case=case)
    restored = ManifestRdfDeserializer().from_turtle(
        ManifestRdfSerializer().to_turtle(manifest, BASE_URI), BASE_URI
    )
    assert restored.graph_schema is not None
    naming = restored.graph_schema.metadata.naming
    assert naming is not None
    assert str(naming.vertex_case) == case


def test_schemas_without_naming_emit_no_naming_triples() -> None:
    """Absent block stays absent — the vocabulary costs nothing when unused."""
    manifest = GraphManifest(
        schema={
            "metadata": {"name": "shop"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Customer",
                            "properties": [{"name": "id"}],
                            "identity": ["id"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    )
    graph = ManifestRdfSerializer().to_graph(manifest, BASE_URI)
    assert not list(graph.subject_objects(ns.hasNamingConvention))


def test_ontology_declares_naming_terms() -> None:
    ontology = load_ontology_graph()
    assert (ns.NamingConvention, RDF.type, OWL.Class) in ontology
    assert (ns.hasNamingConvention, RDF.type, OWL.ObjectProperty) in ontology
    assert (ns.singularVertexNames, RDF.type, OWL.DatatypeProperty) in ontology
    # Domain-bearing, unlike gf:semanticIri: the naming block attaches at exactly
    # one place, so a domain is honest — and it is what lets the docs viz reach
    # gf:NamingConvention rather than leaving it floating outside every block.
    assert ns.GraphMetadata in set(
        ontology.objects(ns.hasNamingConvention, RDFS.domain)
    )
