"""The domain TBox and SHACL shapes a schema exports."""

from __future__ import annotations

import pathlib

import pytest
import yaml
from rdflib import RDF, RDFS, Graph, Literal, URIRef
from rdflib.collection import Collection
from rdflib.namespace import OWL, SH, XSD

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.document import Schema
from graflo.hq.rdf_inferencer import RdfInferenceManager
from graflo.rdf import namespace as ns
from graflo.rdf.tbox import schema_to_owl, schema_to_shacl

EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / "examples"
BASE = "http://example.org/hr/"


def _schema(base_iri: str | None = BASE) -> Schema:
    metadata: dict = {"name": "hr", "version": "2.0.0"}
    if base_iri is not None:
        metadata["base_iri"] = base_iri
    schema = Schema.model_validate(
        {
            "metadata": metadata,
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Person",
                            "properties": [
                                {"name": "id", "type": "STRING"},
                                {"name": "email", "type": "STRING"},
                                {"name": "age", "type": "INT"},
                                {"name": "tags", "type": "LIST", "item_type": "STRING"},
                            ],
                            "identity": ["id"],
                            "secondary_identities": [["email"]],
                            "semantics": {
                                "synonyms": ["Employee"],
                                "exact_match": ["https://schema.org/Person"],
                            },
                        },
                        {
                            "name": "Manager",
                            "properties": [{"name": "id", "type": "STRING"}],
                            "identity": ["id"],
                            "subclass_of": [{"vertex": "Person"}],
                        },
                        {
                            "name": "Team",
                            "properties": [{"name": "code", "type": "STRING"}],
                            "identity": ["code"],
                            "semantics": {"iri": "https://example.com/vocab/Team"},
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {"source": "Person", "target": "Team", "relation": "member_of"},
                        {
                            "source": "Manager",
                            "target": "Team",
                            "relation": "member_of",
                        },
                        {
                            "source": "Team",
                            "target": "Person",
                            "relation": "has_member",
                            "properties": [{"name": "since", "type": "DATETIME"}],
                        },
                        {
                            "source": "Person",
                            "target": "Person",
                            "relation": "knows",
                            "directed": False,
                        },
                    ],
                    "inverses": [{"relation": "member_of", "inverse": "has_member"}],
                    "symmetric": ["knows"],
                },
            },
        }
    )
    schema.finish_init()
    return schema


@pytest.fixture(scope="module")
def owl() -> Graph:
    return schema_to_owl(_schema())


def test_vertices_are_classes_with_their_names(owl: Graph) -> None:
    person = URIRef(BASE + "Person")
    assert (person, RDF.type, OWL.Class) in owl
    assert (person, ns.elementName, Literal("Person")) in owl
    assert (person, ns.altLabel, Literal("Employee")) in owl
    assert (person, ns.exactMatch, URIRef("https://schema.org/Person")) in owl


def test_semantics_iri_wins_over_the_minted_one(owl: Graph) -> None:
    assert (URIRef("https://example.com/vocab/Team"), RDF.type, OWL.Class) in owl
    assert (URIRef(BASE + "Team"), RDF.type, OWL.Class) not in owl


def test_subclass_of_a_vertex(owl: Graph) -> None:
    assert (URIRef(BASE + "Manager"), RDFS.subClassOf, URIRef(BASE + "Person")) in owl


def test_fields_have_one_domain_and_an_xsd_range(owl: Graph) -> None:
    age = URIRef(BASE + "Person_age")
    assert (age, RDF.type, OWL.DatatypeProperty) in owl
    assert list(owl.objects(age, RDFS.domain)) == [URIRef(BASE + "Person")]
    assert (age, RDFS.range, XSD.integer) in owl
    assert (age, RDF.type, OWL.FunctionalProperty) in owl


def test_a_list_field_is_not_functional(owl: Graph) -> None:
    tags = URIRef(BASE + "Person_tags")
    assert (tags, RDFS.range, XSD.string) in owl
    assert (tags, RDF.type, OWL.FunctionalProperty) not in owl


def test_identity_and_secondary_identity_are_keys(owl: Graph) -> None:
    keys = [
        list(Collection(owl, head))
        for head in owl.objects(URIRef(BASE + "Person"), OWL.hasKey)
    ]
    assert sorted(keys) == sorted(
        [[URIRef(BASE + "Person_id")], [URIRef(BASE + "Person_email")]]
    )


def test_a_shared_relation_is_split_under_one_parent(owl: Graph) -> None:
    parent = URIRef(BASE + "member_of")
    for source in ("Person", "Manager"):
        prop = URIRef(f"{BASE}{source}_member_of_Team")
        assert (prop, RDFS.subPropertyOf, parent) in owl
        assert list(owl.objects(prop, RDFS.domain)) == [URIRef(BASE + source)]
    assert not list(owl.objects(parent, RDFS.domain))


def test_inverse_and_symmetric_relations(owl: Graph) -> None:
    assert (
        URIRef(BASE + "has_member"),
        OWL.inverseOf,
        URIRef(BASE + "member_of"),
    ) in owl
    assert (URIRef(BASE + "knows"), RDF.type, OWL.SymmetricProperty) in owl


def test_edge_attributes_travel_as_an_annotation(owl: Graph) -> None:
    [extension] = owl.objects(URIRef(BASE + "has_member"), ns.edgeExtension)
    assert '"since"' in str(extension)
    [undirected] = owl.objects(URIRef(BASE + "knows"), ns.edgeExtension)
    assert '"directed": false' in str(undirected)


def test_ontology_header(owl: Graph) -> None:
    ontology = URIRef("http://example.org/hr")
    assert (ontology, RDF.type, OWL.Ontology) in owl
    assert (ontology, OWL.versionInfo, Literal("2.0.0")) in owl


def test_without_a_base_iri_terms_are_urns() -> None:
    graph = schema_to_owl(_schema(base_iri=None))
    assert (URIRef("urn:graflo:hr:Person"), RDF.type, OWL.Class) in graph


def test_an_explicit_base_iri_overrides_metadata() -> None:
    graph = schema_to_owl(_schema(), base_iri="https://other.org/")
    assert (URIRef("https://other.org/Person"), RDF.type, OWL.Class) in graph


def test_serialization_is_deterministic() -> None:
    first = schema_to_owl(_schema()).serialize(format="turtle")
    assert first == schema_to_owl(_schema()).serialize(format="turtle")
    shapes = schema_to_shacl(_schema()).serialize(format="turtle")
    assert shapes == schema_to_shacl(_schema()).serialize(format="turtle")


def test_shapes_state_keys_and_cardinalities() -> None:
    shapes = schema_to_shacl(_schema())
    node = URIRef(BASE + "PersonShape")
    assert (node, RDF.type, SH.NodeShape) in shapes
    assert (node, SH.targetClass, URIRef(BASE + "Person")) in shapes
    identity = URIRef(BASE + "PersonShape_id")
    assert (identity, SH.minCount, Literal(1)) in shapes
    assert (identity, SH.maxCount, Literal(1)) in shapes
    tags = URIRef(BASE + "PersonShape_tags")
    assert (tags, SH.datatype, XSD.string) in shapes
    assert not list(shapes.objects(tags, SH.maxCount))
    edge = URIRef(BASE + "PersonShape_member_of_Team")
    assert (edge, SH["class"], URIRef("https://example.com/vocab/Team")) in shapes


def test_reimport_keeps_vertices_and_relations(tmp_path: pathlib.Path) -> None:
    """The inbound inferencer reads the TBox back; fields come back IRI-named."""
    path = EXAMPLES / "11-connection-proxy" / "manifest_shop.yaml"
    manifest = GraphManifest.from_dict(yaml.safe_load(path.read_text()))
    manifest.finish_init()
    schema = manifest.graph_schema
    assert schema is not None
    ttl = tmp_path / "shop.ttl"
    schema_to_owl(schema, base_iri="http://example.org/shop/").serialize(
        str(ttl), format="turtle"
    )
    back, _ = RdfInferenceManager().infer_schema(ttl)
    vertex_names = {v.name for v in back.core_schema.vertex_config.vertices}
    assert vertex_names == {v.name for v in schema.core_schema.vertex_config.vertices}
    relations = {
        (e.source, e.target, e.relation) for e in back.core_schema.edge_config.edges
    }
    assert relations == {
        (e.source, e.target, e.relation) for e in schema.core_schema.edge_config.edges
    }
