"""Tests for :class:`RdfInferenceManager`."""

from __future__ import annotations

from pathlib import Path

import pytest

from graflo.architecture.contract.bindings import SparqlConnector
from graflo.hq.rdf_inferencer import RdfInferenceManager

DATA_DIR = Path(__file__).parents[1] / "data" / "rdf"


@pytest.fixture()
def sample_ontology_path() -> Path:
    """Path to the sample TBox-only ontology file."""
    return DATA_DIR / "sample_ontology.ttl"


@pytest.fixture()
def sample_data_path() -> Path:
    """Path to the sample combined TBox + ABox data file."""
    return DATA_DIR / "sample_data.ttl"


class TestRdfInferenceManager:
    """Unit tests for ontology-based schema inference."""

    def test_infer_schema_vertices(self, sample_ontology_path: Path):
        """Vertices should be inferred from owl:Class declarations."""
        mgr = RdfInferenceManager()
        schema, _ = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        vertex_names = {v.name for v in schema.core_schema.vertex_config.vertices}
        assert "Person" in vertex_names
        assert "Organization" in vertex_names

    def test_infer_schema_fields(self, sample_ontology_path: Path):
        """Datatype properties should become vertex fields."""
        mgr = RdfInferenceManager()
        schema, _ = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        person_fields = schema.core_schema.vertex_config.property_names("Person")
        assert "name" in person_fields
        assert "age" in person_fields
        assert "_key" in person_fields
        assert "_uri" in person_fields

        org_fields = schema.core_schema.vertex_config.property_names("Organization")
        assert "orgName" in org_fields
        assert "founded" in org_fields

    def test_infer_schema_edges(self, sample_ontology_path: Path):
        """Object properties should become edges with correct source/target."""
        mgr = RdfInferenceManager()
        schema, _ = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        edges = schema.core_schema.edge_config.edges
        edge_tuples = {(e.source, e.target, e.relation) for e in edges}

        assert ("Person", "Organization", "worksFor") in edge_tuples
        assert ("Person", "Person", "knows") in edge_tuples

    def test_infer_schema_vertex_identity_uri(self, sample_ontology_path: Path):
        """RDF-inferred vertices should key ingestion on subject URI."""
        mgr = RdfInferenceManager()
        schema, _ = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        for v in schema.core_schema.vertex_config.vertices:
            assert v.identity == ["_uri"]

    def test_infer_schema_resource_pipeline_uri_object_properties(
        self, sample_ontology_path: Path
    ):
        """Object properties should emit target vertex + edge steps (incl. same-class)."""
        mgr = RdfInferenceManager()
        _, ingestion_model = mgr.infer_schema(
            sample_ontology_path, schema_name="test_rdf"
        )

        person = next(r for r in ingestion_model.resources if r.name == "Person")
        assert person.pipeline == [
            {"vertex": "Person", "role": "subject"},
            {
                "vertex": "Person",
                "from": {"_uri": "knows"},
                "extraction_scope": "mapped_only",
                "role": "knows",
            },
            {
                "edge": {
                    "from": "Person",
                    "to": "Person",
                    "relation": "knows",
                    "match_source": "subject",
                    "match_target": "knows",
                }
            },
            {
                "vertex": "Organization",
                "from": {"_uri": "worksFor"},
                "extraction_scope": "mapped_only",
                "role": "worksFor",
            },
            {
                "edge": {
                    "from": "Person",
                    "to": "Organization",
                    "relation": "worksFor",
                    "match_source": "subject",
                    "match_target": "worksFor",
                }
            },
        ]
        assert person.infer_edges is False

        organization = next(
            r for r in ingestion_model.resources if r.name == "Organization"
        )
        assert organization.pipeline == [{"vertex": "Organization"}]

    def test_the_subject_role_avoids_a_relation_of_the_same_name(self, tmp_path: Path):
        _, ingestion_model = _infer_all(
            tmp_path,
            """
ex:Book a owl:Class .
ex:Topic a owl:Class .
ex:subject a owl:ObjectProperty ; rdfs:domain ex:Book ; rdfs:range ex:Topic .
""",
        )

        book = ingestion_model.fetch_resource("Book")
        entities = book({"_uri": EX + "b1", "_key": "b1", "subject": EX + "t1"})

        assert book.config.pipeline[0] == {"vertex": "Book", "role": "_subject"}
        assert _edges(entities, "Book", "Topic", "subject") == [("b1", "t1")]

    def test_anonymous_classes_are_not_vertices(self, tmp_path: Path):
        schema, _ = _infer_all(
            tmp_path,
            """
ex:Cat a owl:Class .
ex:Dog a owl:Class .
ex:Pet a owl:Class ; owl:equivalentClass [ a owl:Class ; owl:unionOf ( ex:Cat ex:Dog ) ] .
""",
        )

        assert [v.name for v in schema.core_schema.vertex_config.vertices] == [
            "Cat",
            "Dog",
            "Pet",
        ]

    def test_infer_schema_resources(self, sample_ontology_path: Path):
        """One Resource per class should be created."""
        mgr = RdfInferenceManager()
        _schema, ingestion_model = mgr.infer_schema(
            sample_ontology_path, schema_name="test_rdf"
        )

        resource_names = {r.name for r in ingestion_model.resources}
        assert "Person" in resource_names
        assert "Organization" in resource_names

    def test_infer_schema_name(self, sample_ontology_path: Path):
        """Schema should use the provided name."""
        mgr = RdfInferenceManager()
        schema, _ = mgr.infer_schema(sample_ontology_path, schema_name="my_ontology")
        assert schema.metadata.name == "my_ontology"

    def test_create_bindings(self, sample_ontology_path: Path):
        """Bindings should contain one SparqlConnector per class."""
        mgr = RdfInferenceManager()
        bindings = mgr.create_bindings(sample_ontology_path)

        person_pat = bindings.get_connectors_for_resource("Person")[0]
        org_pat = bindings.get_connectors_for_resource("Organization")[0]
        assert isinstance(person_pat, SparqlConnector)
        assert isinstance(org_pat, SparqlConnector)
        assert person_pat.rdf_class == "http://example.org/Person"
        assert person_pat.rdf_file is not None

    def test_create_bindings_with_endpoint(self, sample_ontology_path: Path):
        """When endpoint_url is given, bindings should reference it."""
        mgr = RdfInferenceManager()
        endpoint = "http://localhost:3030/test/sparql"
        bindings = mgr.create_bindings(sample_ontology_path, endpoint_url=endpoint)

        for resource_name in ("Person", "Organization"):
            pat = bindings.get_connectors_for_resource(resource_name)[0]
            assert isinstance(pat, SparqlConnector)
            assert pat.endpoint_url == endpoint
            assert pat.rdf_file is None

    def test_infer_from_combined_file(self, sample_data_path: Path):
        """Inference should work on a file containing both TBox and ABox."""
        mgr = RdfInferenceManager()
        schema, _ = mgr.infer_schema(sample_data_path, schema_name="combined")

        vertex_names = {v.name for v in schema.core_schema.vertex_config.vertices}
        assert "Person" in vertex_names
        assert "Organization" in vertex_names


_PREFIXES = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex: <http://example.org/> .

ex:Person a owl:Class .
ex:Organization a owl:Class .
"""


def _infer(tmp_path: Path, body: str):
    path = tmp_path / "onto.ttl"
    path.write_text(_PREFIXES + body, encoding="utf-8")
    schema, _ = RdfInferenceManager().infer_schema(path, schema_name="inv")
    return schema.core_schema.edge_config


class TestDeclaredInverses:
    """What the ontology states about inverses is carried over; nothing is stored for it."""

    def test_inverse_of_becomes_a_declared_pair(self, tmp_path: Path):
        edge_config = _infer(
            tmp_path,
            """
ex:worksFor a owl:ObjectProperty ;
    rdfs:domain ex:Person ; rdfs:range ex:Organization ;
    owl:inverseOf ex:employs .
""",
        )
        assert [(p.relation, p.inverse) for p in edge_config.inverses] == [
            ("employs", "worksFor")
        ]
        # Declared only: the inverse labels no edge, and nothing is stored for it.
        assert [e.relation for e in edge_config.edges] == ["worksFor"]

    def test_a_pair_stated_from_both_sides_is_one_pair(self, tmp_path: Path):
        edge_config = _infer(
            tmp_path,
            """
ex:worksFor a owl:ObjectProperty ;
    rdfs:domain ex:Person ; rdfs:range ex:Organization ;
    owl:inverseOf ex:employs .
ex:employs a owl:ObjectProperty ;
    rdfs:domain ex:Organization ; rdfs:range ex:Person ;
    owl:inverseOf ex:worksFor .
""",
        )
        assert len(edge_config.inverses) == 1
        assert {e.relation for e in edge_config.edges} == {"worksFor", "employs"}

    def test_a_symmetric_property_is_declared_and_its_edges_are_undirected(
        self, tmp_path: Path
    ):
        edge_config = _infer(
            tmp_path,
            """
ex:knows a owl:ObjectProperty , owl:SymmetricProperty ;
    rdfs:domain ex:Person ; rdfs:range ex:Person .
""",
        )
        assert edge_config.symmetric == ["knows"]
        assert [e.directed for e in edge_config.edges] == [False]

    def test_a_property_that_is_its_own_inverse_is_symmetric(self, tmp_path: Path):
        edge_config = _infer(
            tmp_path,
            """
ex:marriedTo a owl:ObjectProperty ;
    rdfs:domain ex:Person ; rdfs:range ex:Person ;
    owl:inverseOf ex:marriedTo .
""",
        )
        assert edge_config.symmetric == ["marriedTo"]
        assert edge_config.inverses == []

    def test_statements_the_inverse_table_cannot_hold_are_dropped_not_fatal(
        self, tmp_path: Path, caplog
    ):
        with caplog.at_level("WARNING"):
            edge_config = _infer(
                tmp_path,
                """
ex:worksFor a owl:ObjectProperty ;
    rdfs:domain ex:Person ; rdfs:range ex:Organization ;
    owl:inverseOf ex:employs , ex:hasStaff .
""",
            )
        assert edge_config.inverses == []
        assert [e.relation for e in edge_config.edges] == ["worksFor"]
        assert "at most one inverse" in caplog.text

    def test_an_inverse_between_properties_that_label_no_edge_is_ignored(
        self, tmp_path: Path
    ):
        edge_config = _infer(tmp_path, "ex:a owl:inverseOf ex:b .\n")
        assert edge_config.inverses == []


_ONTOLOGY_PREFIXES = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .
"""


def _infer_all(tmp_path: Path, body: str, name: str = "onto.ttl"):
    path = tmp_path / name
    path.write_text(_ONTOLOGY_PREFIXES + body, encoding="utf-8")
    return RdfInferenceManager().infer_schema(path, schema_name="g")


class TestInferenceOrder:
    """The inferred manifest is ordered by IRI, not by how the ontology was read."""

    _CLASSES = ["Zebra", "Mango", "Apple", "Quince", "Birch", "Yew", "Elm", "Oak"]

    def _body(self) -> str:
        classes = "".join(f"ex:{name} a owl:Class .\n" for name in self._CLASSES)
        return (
            classes
            + """
ex:weight a owl:DatatypeProperty ; rdfs:domain ex:Apple ; rdfs:range xsd:float .
ex:colour a owl:DatatypeProperty ; rdfs:domain ex:Apple ; rdfs:range xsd:string .
ex:shades a owl:ObjectProperty ; rdfs:domain ex:Oak ; rdfs:range ex:Apple .
ex:feeds a owl:ObjectProperty ; rdfs:domain ex:Oak ; rdfs:range ex:Zebra , ex:Birch .
ex:eats a owl:ObjectProperty ; rdfs:domain ex:Zebra ; rdfs:range ex:Apple .
"""
        )

    def test_vertices_and_resources_are_sorted(self, tmp_path: Path) -> None:
        schema, ingestion_model = _infer_all(tmp_path, self._body())

        expected = sorted(self._CLASSES)
        assert [v.name for v in schema.core_schema.vertex_config.vertices] == expected
        assert [r.name for r in ingestion_model.resources] == expected

    def test_fields_are_sorted_after_the_key_and_the_uri(self, tmp_path: Path) -> None:
        schema, _ = _infer_all(tmp_path, self._body())

        assert schema.core_schema.vertex_config.property_names("Apple") == [
            "_key",
            "_uri",
            "colour",
            "weight",
        ]

    def test_edges_are_sorted(self, tmp_path: Path) -> None:
        schema, _ = _infer_all(tmp_path, self._body())

        edges = [
            (e.source, e.target, e.relation)
            for e in schema.core_schema.edge_config.edges
        ]
        assert edges == [
            ("Zebra", "Apple", "eats"),
            ("Oak", "Birch", "feeds"),
            ("Oak", "Zebra", "feeds"),
            ("Oak", "Apple", "shades"),
        ]

    def test_bindings_are_sorted(self, tmp_path: Path) -> None:
        path = tmp_path / "onto.ttl"
        path.write_text(_ONTOLOGY_PREFIXES + self._body(), encoding="utf-8")

        bindings = RdfInferenceManager().create_bindings(path)

        assert [
            c.rdf_class for c in bindings.connectors if isinstance(c, SparqlConnector)
        ] == [f"http://example.org/{name}" for name in sorted(self._CLASSES)]

    def test_two_classes_with_one_local_name_are_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Person") as raised:
            _infer_all(
                tmp_path,
                """
<http://a.example/Person> a owl:Class .
<http://b.example/Person> a owl:Class .
""",
            )
        assert "http://a.example/Person" in str(raised.value)
        assert "http://b.example/Person" in str(raised.value)


_PEOPLE = """
ex:Person a owl:Class .
ex:Organization a owl:Class .
ex:Paper a owl:Class .
ex:worksFor a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Organization .
ex:knows a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Person .
ex:wrote a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Paper .
ex:reviewed a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Paper .
"""

EX = "http://example.org/"


def _edges(entities, source: str, target: str, relation: str) -> list[tuple[str, str]]:
    return sorted(
        (s["_uri"].removeprefix(EX), t["_uri"].removeprefix(EX))
        for s, t, _ in entities[source, target, relation]
    )


class TestInferredResourceCast:
    """What a record of an inferred resource becomes."""

    @pytest.fixture
    def person(self, tmp_path: Path):
        _, ingestion_model = _infer_all(tmp_path, _PEOPLE)
        return ingestion_model.fetch_resource("Person")

    def test_every_object_of_a_property_gets_its_edge(self, person) -> None:
        entities = person(
            {
                "_uri": EX + "alice",
                "_key": "alice",
                "wrote": [EX + "p1", EX + "p3"],
            }
        )

        assert _edges(entities, "Person", "Paper", "wrote") == [
            ("alice", "p1"),
            ("alice", "p3"),
        ]
        assert sorted(v["_uri"] for v in entities["Paper"]) == [EX + "p1", EX + "p3"]

    def test_objects_of_a_property_are_not_linked_to_each_other(self, person) -> None:
        entities = person(
            {
                "_uri": EX + "alice",
                "_key": "alice",
                "knows": [EX + "bob", EX + "carol"],
            }
        )

        assert _edges(entities, "Person", "Person", "knows") == [
            ("alice", "bob"),
            ("alice", "carol"),
        ]

    def test_two_properties_to_one_class_keep_their_own_objects(self, person) -> None:
        entities = person(
            {
                "_uri": EX + "alice",
                "_key": "alice",
                "wrote": EX + "p1",
                "reviewed": EX + "p2",
            }
        )

        assert _edges(entities, "Person", "Paper", "wrote") == [("alice", "p1")]
        assert _edges(entities, "Person", "Paper", "reviewed") == [("alice", "p2")]

    def test_an_object_of_the_subject_class_is_not_a_second_subject(
        self, person
    ) -> None:
        entities = person(
            {
                "_uri": EX + "alice",
                "_key": "alice",
                "knows": EX + "bob",
                "worksFor": EX + "acme",
            }
        )

        assert _edges(entities, "Person", "Organization", "worksFor") == [
            ("alice", "acme")
        ]
        assert _edges(entities, "Person", "Person", "knows") == [("alice", "bob")]
