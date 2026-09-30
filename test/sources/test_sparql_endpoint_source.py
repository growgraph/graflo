"""The endpoint source against an in-memory graph answering its queries."""

from __future__ import annotations

import json
from typing import Any

import pytest
from rdflib import Graph

from graflo.data_source.rdf import (
    RdfFileDataSource,
    SparqlEndpointDataSource,
    SparqlSourceConfig,
)

EX = "http://example.org/"

_PREFIXES = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix ex: <http://example.org/> .
"""


class _Endpoint:
    """Answers SPARQL ``SELECT`` queries from a graph, in SPARQL JSON."""

    def __init__(self, graph: Graph) -> None:
        self.graph = graph
        self.queries: list[str] = []

    def setQuery(self, query: str) -> None:
        self.queries.append(query)

    def queryAndConvert(self) -> dict[str, Any]:
        result = self.graph.query(self.queries[-1])
        return json.loads(result.serialize(format="json"))


@pytest.fixture
def read(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Read a Turtle body through the endpoint source; returns ``(docs, endpoint)``."""

    def run(
        body: str,
        *,
        rdf_class: str | None = None,
        page_size: int = 1000,
        limit: int | None = None,
        **source_kwargs: Any,
    ) -> tuple[list[dict], _Endpoint]:
        graph = Graph()
        graph.parse(data=_PREFIXES + body, format="turtle")
        endpoint = _Endpoint(graph)
        monkeypatch.setattr(
            SparqlEndpointDataSource, "_create_wrapper", lambda self: endpoint
        )
        source = SparqlEndpointDataSource(
            config=SparqlSourceConfig(
                endpoint_url="http://sparql.invalid/ds",
                rdf_class=rdf_class,
                page_size=page_size,
            ),
            **source_kwargs,
        )
        docs = [doc for batch in source.iter_batches(limit=limit) for doc in batch]
        return docs, endpoint

    return run


def _file_docs(tmp_path, body: str, rdf_class: str) -> list[dict]:
    path = tmp_path / "data.ttl"
    path.write_text(_PREFIXES + body, encoding="utf-8")
    source = RdfFileDataSource(path=path, rdf_class=rdf_class)
    return [doc for batch in source.iter_batches() for doc in batch]


_PEOPLE = """
ex:alice a ex:Person ; ex:name "Alice" ; ex:age 30 ; ex:knows ex:bob , ex:carol .
ex:bob a ex:Person ; ex:name "Bob" .
ex:carol a ex:Person ; ex:name "Carol" .
ex:acme a ex:Organization ; ex:name "Acme" .
"""


class TestPlainRead:
    def test_subjects_of_the_class_are_read(self, read) -> None:
        docs, _ = read(_PEOPLE, rdf_class=EX + "Person")

        assert [doc["_key"] for doc in docs] == ["alice", "bob", "carol"]
        assert docs[0]["name"] == "Alice"
        assert docs[0]["age"] == 30
        assert sorted(docs[0]["knows"]) == [EX + "bob", EX + "carol"]

    def test_a_subject_split_across_pages_is_one_record(self, read) -> None:
        docs, endpoint = read(_PEOPLE, rdf_class=EX + "Person", page_size=2)

        assert [doc["_key"] for doc in docs] == ["alice", "bob", "carol"]
        assert sorted(docs[0]["knows"]) == [EX + "bob", EX + "carol"]
        assert len(endpoint.queries) > 3

    def test_the_limit_counts_subjects(self, read) -> None:
        docs, _ = read(_PEOPLE, rdf_class=EX + "Person", limit=2)

        assert [doc["_key"] for doc in docs] == ["alice", "bob"]

    def test_no_identity_query_is_sent_for_a_graph_that_needs_none(self, read) -> None:
        _, endpoint = read(_PEOPLE, rdf_class=EX + "Person", same_as="keep")

        assert len(endpoint.queries) == 1


_MEASUREMENTS = """
ex:s1 a ex:Sample ;
    ex:measured [ a ex:Measurement ; ex:value 3 ; ex:unit [ ex:symbol "nm" ] ] ;
    ex:measured [ a ex:Measurement ; ex:value 5 ; ex:unit [ ex:symbol "nm" ] ] .
"""


class TestBlankNodes:
    def test_a_blank_node_has_the_key_the_file_source_gives_it(
        self, read, tmp_path
    ) -> None:
        docs, _ = read(_MEASUREMENTS, rdf_class=EX + "Measurement")
        from_file = _file_docs(tmp_path, _MEASUREMENTS, EX + "Measurement")

        assert sorted(doc["_uri"] for doc in docs) == sorted(
            doc["_uri"] for doc in from_file
        )
        assert len({doc["_uri"] for doc in docs}) == 2

    def test_a_reference_to_a_blank_node_is_its_uri(self, read) -> None:
        (sample,), _ = read(_MEASUREMENTS, rdf_class=EX + "Sample")
        measurements, _ = read(_MEASUREMENTS, rdf_class=EX + "Measurement")

        assert sorted(sample["measured"]) == sorted(doc["_uri"] for doc in measurements)

    def test_an_endpoint_that_relabels_blank_nodes_is_refused(
        self, read, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        answer = _Endpoint.queryAndConvert

        def relabelled(self: _Endpoint) -> dict[str, Any]:
            results = answer(self)
            if "isBlank" not in self.queries[-1]:
                return results
            for binding in results["results"]["bindings"]:
                binding["s"]["value"] = "fresh-" + binding["s"]["value"]
            return results

        monkeypatch.setattr(_Endpoint, "queryAndConvert", relabelled)

        with pytest.raises(ValueError, match="blank-node labels"):
            read(_MEASUREMENTS, rdf_class=EX + "Measurement")


_ALIASES = """
ex:alice a ex:Person ; ex:name "Alice" ; ex:knows ex:robert .
ex:alice_smith ex:email "alice@example.org" ; owl:sameAs ex:alice .
ex:bob a ex:Person ; ex:name "Bob" ; owl:sameAs ex:robert .
ex:robert a ex:Person ; ex:name "Bob" .
ex:zed a ex:Person ; ex:name "Zed" .
"""


class TestSameAs:
    def test_iris_said_to_be_the_same_are_one_record(self, read, tmp_path) -> None:
        docs, _ = read(_ALIASES, rdf_class=EX + "Person")

        by_key = {doc["_key"]: doc for doc in docs}
        assert sorted(by_key) == ["alice", "bob", "zed"]
        assert by_key["alice"] == {
            "_uri": EX + "alice",
            "_key": "alice",
            "name": "Alice",
            "email": "alice@example.org",
            "knows": EX + "bob",
            "_same_as": [EX + "alice_smith"],
        }
        assert by_key["bob"]["_same_as"] == [EX + "robert"]
        assert sorted(docs, key=lambda d: d["_uri"]) == sorted(
            _file_docs(tmp_path, _ALIASES, EX + "Person"), key=lambda d: d["_uri"]
        )

    def test_a_record_is_not_split_by_the_page_size(self, read) -> None:
        docs, _ = read(_ALIASES, rdf_class=EX + "Person", page_size=2)

        by_key = {doc["_key"]: doc for doc in docs}
        assert sorted(by_key) == ["alice", "bob", "zed"]
        assert by_key["alice"]["email"] == "alice@example.org"

    def test_keep_leaves_the_statements_as_they_are(self, read) -> None:
        docs, _ = read(_ALIASES, rdf_class=EX + "Person", same_as="keep")

        by_key = {doc["_key"]: doc for doc in docs}
        assert sorted(by_key) == ["alice", "bob", "robert", "zed"]
        assert by_key["bob"]["sameAs"] == EX + "robert"


_FEEDING = """
ex:oak1 a ex:Oak ; ex:feeds ex:z1 , ex:b1 , ex:u1 .
ex:z1 a ex:Zebra .
ex:b1 a ex:Birch .
ex:z9 a ex:Zebra .
ex:z1 owl:sameAs ex:z9 .
ex:b2 a ex:Birch .
ex:oak2 a ex:Oak ; ex:feeds ex:b3 .
ex:b3 owl:sameAs ex:b2 .
"""


class TestTypedObjects:
    """Objects of a listed property are also read by their class."""

    def _check(self, docs: list[dict]) -> None:
        oak1, oak2 = docs
        assert sorted(oak1["feeds"]) == [EX + "b1", EX + "u1", EX + "z1"]
        assert oak1["feeds@Zebra"] == EX + "z1"
        assert oak1["feeds@Birch"] == EX + "b1"
        # A type stated on another member of the object's sameAs component.
        assert oak2["feeds@Birch"] == EX + "b2"

    def test_the_endpoint_splits_objects_by_type(self, read) -> None:
        docs, _ = read(_FEEDING, rdf_class=EX + "Oak", typed_objects=["feeds"])
        self._check(docs)

    def test_the_file_splits_objects_by_type(self, tmp_path) -> None:
        path = tmp_path / "data.ttl"
        path.write_text(_PREFIXES + _FEEDING, encoding="utf-8")
        source = RdfFileDataSource(
            path=path, rdf_class=EX + "Oak", typed_objects=["feeds"]
        )
        self._check([doc for batch in source.iter_batches() for doc in batch])

    def test_an_unlisted_property_is_read_as_before(self, read) -> None:
        docs, _ = read(_FEEDING, rdf_class=EX + "Oak")
        assert not [key for key in docs[0] if "@" in key]
