"""Tests for :class:`RdfInferenceManager`."""

from __future__ import annotations

from pathlib import Path

from graflo.architecture.contract.bindings import SparqlConnector
from graflo.hq.rdf_inferencer import RdfInferenceManager


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
            {"vertex": "Person"},
            {
                "vertex": "Organization",
                "from": {"_uri": "worksFor"},
                "extraction_scope": "mapped_only",
            },
            {
                "edge": {
                    "from": "Person",
                    "to": "Organization",
                    "relation": "worksFor",
                }
            },
            {
                "vertex": "Person",
                "from": {"_uri": "knows"},
                "extraction_scope": "mapped_only",
            },
            {
                "edge": {
                    "from": "Person",
                    "to": "Person",
                    "relation": "knows",
                }
            },
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
