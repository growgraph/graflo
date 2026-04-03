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

    def test_infer_schema_resources(self, sample_ontology_path: Path):
        """One Resource per class should be created."""
        mgr = RdfInferenceManager()
        schema, ingestion_model = mgr.infer_schema(
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
