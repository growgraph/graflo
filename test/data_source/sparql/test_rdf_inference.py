"""Tests for :class:`RdfInferenceManager`."""

from __future__ import annotations

from pathlib import Path

import pytest

rdflib = pytest.importorskip(
    "rdflib", reason="rdflib not installed (need graflo[sparql])"
)

from graflo.hq.rdf_inferencer import RdfInferenceManager  # noqa: E402


class TestRdfInferenceManager:
    """Unit tests for ontology-based schema inference."""

    def test_infer_schema_vertices(self, sample_ontology_path: Path):
        """Vertices should be inferred from owl:Class declarations."""
        mgr = RdfInferenceManager()
        schema = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        vertex_names = {v.name for v in schema.vertex_config.vertices}
        assert "Person" in vertex_names
        assert "Organization" in vertex_names

    def test_infer_schema_fields(self, sample_ontology_path: Path):
        """Datatype properties should become vertex fields."""
        mgr = RdfInferenceManager()
        schema = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        person_fields = schema.vertex_config.fields_names("Person")
        assert "name" in person_fields
        assert "age" in person_fields
        assert "_key" in person_fields
        assert "_uri" in person_fields

        org_fields = schema.vertex_config.fields_names("Organization")
        assert "orgName" in org_fields
        assert "founded" in org_fields

    def test_infer_schema_edges(self, sample_ontology_path: Path):
        """Object properties should become edges with correct source/target."""
        mgr = RdfInferenceManager()
        schema = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        edges = schema.edge_config.edges
        edge_tuples = {(e.source, e.target, e.relation) for e in edges}

        assert ("Person", "Organization", "worksFor") in edge_tuples
        assert ("Person", "Person", "knows") in edge_tuples

    def test_infer_schema_resources(self, sample_ontology_path: Path):
        """One Resource per class should be created."""
        mgr = RdfInferenceManager()
        schema = mgr.infer_schema(sample_ontology_path, schema_name="test_rdf")

        resource_names = {r.name for r in schema.resources}
        assert "Person" in resource_names
        assert "Organization" in resource_names

    def test_infer_schema_name(self, sample_ontology_path: Path):
        """Schema should use the provided name."""
        mgr = RdfInferenceManager()
        schema = mgr.infer_schema(sample_ontology_path, schema_name="my_ontology")
        assert schema.general.name == "my_ontology"

    def test_create_patterns(self, sample_ontology_path: Path):
        """Patterns should contain one SparqlPattern per class."""
        mgr = RdfInferenceManager()
        patterns = mgr.create_patterns(sample_ontology_path)

        assert "Person" in patterns.sparql_patterns
        assert "Organization" in patterns.sparql_patterns

        person_pat = patterns.sparql_patterns["Person"]
        assert person_pat.rdf_class == "http://example.org/Person"
        assert person_pat.rdf_file is not None

    def test_create_patterns_with_endpoint(self, sample_ontology_path: Path):
        """When endpoint_url is given, patterns should reference it."""
        mgr = RdfInferenceManager()
        endpoint = "http://localhost:3030/test/sparql"
        patterns = mgr.create_patterns(sample_ontology_path, endpoint_url=endpoint)

        for pat in patterns.sparql_patterns.values():
            assert pat.endpoint_url == endpoint
            assert pat.rdf_file is None

    def test_infer_from_combined_file(self, sample_data_path: Path):
        """Inference should work on a file containing both TBox and ABox."""
        mgr = RdfInferenceManager()
        schema = mgr.infer_schema(sample_data_path, schema_name="combined")

        vertex_names = {v.name for v in schema.vertex_config.vertices}
        assert "Person" in vertex_names
        assert "Organization" in vertex_names
