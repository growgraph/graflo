"""Tests for :class:`RdfFileDataSource`."""

from __future__ import annotations

from pathlib import Path

import pytest

rdflib = pytest.importorskip("rdflib", reason="rdflib not installed")

from graflo.data_source.rdf import RdfFileDataSource, _triples_to_docs

DATA_DIR = Path(__file__).parents[1] / "data" / "rdf"


@pytest.fixture()
def sample_ontology_path() -> Path:
    """Path to the sample TBox-only ontology file."""
    return DATA_DIR / "sample_ontology.ttl"


@pytest.fixture()
def sample_data_path() -> Path:
    """Path to the sample combined TBox + ABox data file."""
    return DATA_DIR / "sample_data.ttl"


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


_PREFIXES = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix ex: <http://example.org/> .
"""

EX = "http://example.org/"


def _docs(tmp_path: Path, body: str, **source_kwargs) -> list[dict]:
    path = tmp_path / "data.ttl"
    path.write_text(_PREFIXES + body, encoding="utf-8")
    source = RdfFileDataSource(path=path, **source_kwargs)
    return [doc for batch in source.iter_batches() for doc in batch]


class TestOrder:
    def test_subjects_are_read_in_iri_order(self, tmp_path: Path) -> None:
        body = "".join(
            f"ex:{name} a ex:Thing .\n" for name in ["q", "c", "x", "a", "m", "e", "z"]
        )

        docs = _docs(tmp_path, body, rdf_class=EX + "Thing")

        assert [doc["_key"] for doc in docs] == ["a", "c", "e", "m", "q", "x", "z"]

    def test_the_values_of_a_property_are_in_order(self, tmp_path: Path) -> None:
        docs = _docs(
            tmp_path,
            "ex:a a ex:Thing ; ex:tag 'q' , 'c' , 'x' , 'a' , 'm' .\n",
            rdf_class=EX + "Thing",
        )

        assert docs[0]["tag"] == ["a", "c", "m", "q", "x"]


_MEASUREMENTS = """
ex:s1 a ex:Sample ;
    ex:measured [ a ex:Measurement ; ex:value 3 ; ex:unit [ ex:symbol "nm" ] ] ;
    ex:measured [ a ex:Measurement ; ex:value 5 ; ex:unit [ ex:symbol "nm" ] ] .
"""


class TestBlankNodes:
    def test_a_blank_node_keeps_its_key_across_reads(self, tmp_path: Path) -> None:
        first = _docs(tmp_path, _MEASUREMENTS, rdf_class=EX + "Measurement")
        second = _docs(tmp_path, _MEASUREMENTS, rdf_class=EX + "Measurement")

        assert len(first) == 2
        assert [doc["_uri"] for doc in first] == [doc["_uri"] for doc in second]
        assert all(doc["_uri"] == "_:" + doc["_key"] for doc in first)

    def test_a_reference_to_a_blank_node_is_its_uri(self, tmp_path: Path) -> None:
        (sample,) = _docs(tmp_path, _MEASUREMENTS, rdf_class=EX + "Sample")
        measurements = _docs(tmp_path, _MEASUREMENTS, rdf_class=EX + "Measurement")

        assert sorted(sample["measured"]) == sorted(doc["_uri"] for doc in measurements)

    def test_blank_nodes_with_one_content_are_one_record(self, tmp_path: Path) -> None:
        docs = _docs(
            tmp_path,
            """
ex:s1 ex:unit [ a ex:Unit ; ex:symbol "nm" ] .
ex:s2 ex:unit [ a ex:Unit ; ex:symbol "nm" ] .
""",
            rdf_class=EX + "Unit",
        )

        assert [doc["symbol"] for doc in docs] == ["nm"]


_ALIASES = """
ex:alice a ex:Person ; ex:name "Alice" ; ex:knows ex:robert .
ex:alice_smith ex:email "alice@example.org" ; owl:sameAs ex:alice .
ex:bob a ex:Person ; ex:name "Bob" ; owl:sameAs ex:robert .
ex:robert a ex:Person ; ex:name "Bob" .
"""


class TestSameAs:
    def test_iris_said_to_be_the_same_are_one_record(self, tmp_path: Path) -> None:
        docs = _docs(tmp_path, _ALIASES, rdf_class=EX + "Person")

        assert [doc["_uri"] for doc in docs] == [EX + "alice", EX + "bob"]
        alice, bob = docs
        assert alice == {
            "_uri": EX + "alice",
            "_key": "alice",
            "name": "Alice",
            "email": "alice@example.org",
            "knows": EX + "bob",
            "_same_as": [EX + "alice_smith"],
        }
        assert bob == {
            "_uri": EX + "bob",
            "_key": "bob",
            "name": "Bob",
            "_same_as": [EX + "robert"],
        }

    def test_keep_leaves_the_statements_as_they_are(self, tmp_path: Path) -> None:
        docs = _docs(tmp_path, _ALIASES, rdf_class=EX + "Person", same_as="keep")

        by_key = {doc["_key"]: doc for doc in docs}
        assert sorted(by_key) == ["alice", "bob", "robert"]
        assert by_key["alice"]["knows"] == EX + "robert"
        assert by_key["bob"]["sameAs"] == EX + "robert"
        assert "_same_as" not in by_key["bob"]
