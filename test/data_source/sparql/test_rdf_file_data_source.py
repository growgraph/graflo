"""Tests for :class:`RdfFileDataSource`."""

from __future__ import annotations

from pathlib import Path

import pytest

rdflib = pytest.importorskip(
    "rdflib", reason="rdflib not installed (need graflo[sparql])"
)

from graflo.data_source.rdf import RdfFileDataSource, _triples_to_docs  # noqa: E402


class TestRdfFileDataSource:
    """Unit tests for RDF file parsing."""

    def test_parse_all_subjects(self, sample_data_path: Path):
        """Parse a .ttl file and get all subjects as flat dicts."""
        ds = RdfFileDataSource(path=sample_data_path)
        batches = list(ds.iter_batches(batch_size=100))

        assert len(batches) >= 1
        all_docs = [doc for batch in batches for doc in batch]

        # 3 persons + 2 organisations + class/property declarations (URIRefs)
        # We should get at least the 5 instance subjects
        uris = {doc["_uri"] for doc in all_docs}
        assert "http://example.org/alice" in uris
        assert "http://example.org/acme" in uris

    def test_filter_by_rdf_class(self, sample_data_path: Path):
        """Only subjects of a specific rdf:Class should be returned."""
        ds = RdfFileDataSource(
            path=sample_data_path,
            rdf_class="http://example.org/Person",
        )
        batches = list(ds.iter_batches())
        all_docs = [doc for batch in batches for doc in batch]

        assert len(all_docs) == 3
        names = {doc.get("name") for doc in all_docs}
        assert names == {"Alice", "Bob", "Carol"}

    def test_filter_organization_class(self, sample_data_path: Path):
        """Filter for Organization class."""
        ds = RdfFileDataSource(
            path=sample_data_path,
            rdf_class="http://example.org/Organization",
        )
        batches = list(ds.iter_batches())
        all_docs = [doc for batch in batches for doc in batch]

        assert len(all_docs) == 2
        names = {doc.get("orgName") for doc in all_docs}
        assert names == {"Acme Corp", "Globex Inc"}

    def test_limit(self, sample_data_path: Path):
        """Limit should cap the number of returned items."""
        ds = RdfFileDataSource(
            path=sample_data_path,
            rdf_class="http://example.org/Person",
        )
        batches = list(ds.iter_batches(limit=2))
        all_docs = [doc for batch in batches for doc in batch]
        assert len(all_docs) == 2

    def test_batch_size(self, sample_data_path: Path):
        """Batch size should control the batch partitioning."""
        ds = RdfFileDataSource(
            path=sample_data_path,
            rdf_class="http://example.org/Person",
        )
        batches = list(ds.iter_batches(batch_size=1))
        assert len(batches) == 3
        for batch in batches:
            assert len(batch) == 1

    def test_key_extraction(self, sample_data_path: Path):
        """Each doc should have a _key extracted from the URI local name."""
        ds = RdfFileDataSource(
            path=sample_data_path,
            rdf_class="http://example.org/Person",
        )
        all_docs = [doc for batch in ds.iter_batches() for doc in batch]
        keys = {doc["_key"] for doc in all_docs}
        assert keys == {"alice", "bob", "carol"}

    def test_object_property_as_uri(self, sample_data_path: Path):
        """Object properties should appear as URI strings."""
        ds = RdfFileDataSource(
            path=sample_data_path,
            rdf_class="http://example.org/Person",
        )
        all_docs = {doc["_key"]: doc for batch in ds.iter_batches() for doc in batch}
        alice = all_docs["alice"]
        assert alice["worksFor"] == "http://example.org/acme"

    def test_triples_to_docs_no_class_filter(self, sample_data_path: Path):
        """_triples_to_docs without class filter returns all subjects."""
        from rdflib import Graph

        g = Graph()
        g.parse(str(sample_data_path), format="turtle")

        docs = _triples_to_docs(g, rdf_class=None)
        assert len(docs) > 0
        # Should contain at least the 5 instance URIs
        uris = {d["_uri"] for d in docs}
        for expected in ("alice", "bob", "carol", "acme", "globex"):
            assert any(expected in u for u in uris)
