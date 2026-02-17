"""Integration tests for :class:`SparqlEndpointDataSource` against a running Fuseki instance.

These tests require a running Fuseki container (see ``docker/fuseki/``).
They are skipped automatically when Fuseki is not reachable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

rdflib = pytest.importorskip(
    "rdflib", reason="rdflib not installed (need graflo[sparql])"
)
SPARQLWrapper = pytest.importorskip(
    "SPARQLWrapper", reason="SPARQLWrapper not installed (need graflo[sparql])"
)

import requests  # noqa: E402

from graflo.data_source.rdf import SparqlEndpointDataSource, SparqlSourceConfig  # noqa: E402

DATA_DIR = Path(__file__).parent / "data"
SAMPLE_DATA = DATA_DIR / "sample_data.ttl"


def _fuseki_is_reachable(endpoint: str) -> bool:
    """Return True if Fuseki responds on the given base URL."""
    try:
        base = endpoint.rsplit("/", 2)[0]  # strip /dataset/sparql
        r = requests.get(f"{base}/$/ping", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def _ensure_dataset(config) -> None:
    """Create the test dataset in Fuseki if it does not exist."""
    base = (config.uri or "").rstrip("/")
    dataset = config.dataset or "test"
    url = f"{base}/$/datasets"

    r = requests.get(url, auth=(config.username, config.password), timeout=5)
    existing = {ds.get("ds.name", "").strip("/") for ds in r.json().get("datasets", [])}

    if dataset not in existing:
        requests.post(
            url,
            data={"dbName": dataset, "dbType": "tdb2"},
            auth=(config.username, config.password),
            timeout=10,
        )


def _upload_data(config) -> None:
    """Upload sample_data.ttl to the Fuseki test dataset."""
    endpoint = config.graph_store_endpoint
    with open(SAMPLE_DATA, "rb") as f:
        requests.put(
            endpoint,
            data=f.read(),
            headers={"Content-Type": "text/turtle"},
            params={"default": ""},
            auth=(config.username, config.password),
            timeout=10,
        )


@pytest.fixture(scope="module")
def fuseki_ready(fuseki_config):
    """Ensure Fuseki is running, dataset exists, and data is loaded."""
    if not _fuseki_is_reachable(fuseki_config.query_endpoint):
        pytest.skip("Fuseki is not reachable")

    _ensure_dataset(fuseki_config)
    _upload_data(fuseki_config)
    return fuseki_config


class TestSparqlEndpointDataSource:
    """Integration tests against a live Fuseki endpoint."""

    def test_query_all(self, fuseki_ready):
        """Query all triples from the endpoint."""
        config = SparqlSourceConfig(
            endpoint_url=fuseki_ready.query_endpoint,
            username=fuseki_ready.username,
            password=fuseki_ready.password,
        )
        ds = SparqlEndpointDataSource(config=config)
        batches = list(ds.iter_batches(batch_size=100))

        all_docs = [doc for batch in batches for doc in batch]
        assert len(all_docs) > 0

    def test_query_person_class(self, fuseki_ready):
        """Query only Person instances."""
        config = SparqlSourceConfig(
            endpoint_url=fuseki_ready.query_endpoint,
            rdf_class="http://example.org/Person",
            username=fuseki_ready.username,
            password=fuseki_ready.password,
        )
        ds = SparqlEndpointDataSource(config=config)
        all_docs = [doc for batch in ds.iter_batches() for doc in batch]

        assert len(all_docs) == 3
        names = {doc.get("name") for doc in all_docs}
        assert names == {"Alice", "Bob", "Carol"}

    def test_query_organization_class(self, fuseki_ready):
        """Query only Organization instances."""
        config = SparqlSourceConfig(
            endpoint_url=fuseki_ready.query_endpoint,
            rdf_class="http://example.org/Organization",
            username=fuseki_ready.username,
            password=fuseki_ready.password,
        )
        ds = SparqlEndpointDataSource(config=config)
        all_docs = [doc for batch in ds.iter_batches() for doc in batch]

        assert len(all_docs) == 2
        names = {doc.get("orgName") for doc in all_docs}
        assert names == {"Acme Corp", "Globex Inc"}

    def test_custom_sparql_query(self, fuseki_ready):
        """Use a custom SPARQL query."""
        custom_query = (
            "SELECT ?s ?p ?o WHERE { "
            "?s a <http://example.org/Person> . "
            "?s <http://example.org/name> ?name . "
            "FILTER(?name = 'Alice') "
            "?s ?p ?o . "
            "}"
        )
        config = SparqlSourceConfig(
            endpoint_url=fuseki_ready.query_endpoint,
            sparql_query=custom_query,
            username=fuseki_ready.username,
            password=fuseki_ready.password,
        )
        ds = SparqlEndpointDataSource(config=config)
        all_docs = [doc for batch in ds.iter_batches() for doc in batch]

        assert len(all_docs) == 1
        assert all_docs[0]["name"] == "Alice"

    def test_limit(self, fuseki_ready):
        """Limit should cap the total results."""
        config = SparqlSourceConfig(
            endpoint_url=fuseki_ready.query_endpoint,
            rdf_class="http://example.org/Person",
            username=fuseki_ready.username,
            password=fuseki_ready.password,
        )
        ds = SparqlEndpointDataSource(config=config)
        all_docs = [doc for batch in ds.iter_batches(limit=1) for doc in batch]
        assert len(all_docs) == 1

    def test_key_extraction(self, fuseki_ready):
        """Documents should have _key from URI local name."""
        config = SparqlSourceConfig(
            endpoint_url=fuseki_ready.query_endpoint,
            rdf_class="http://example.org/Person",
            username=fuseki_ready.username,
            password=fuseki_ready.password,
        )
        ds = SparqlEndpointDataSource(config=config)
        all_docs = {doc["_key"]: doc for batch in ds.iter_batches() for doc in batch}

        assert "alice" in all_docs
        assert "bob" in all_docs
        assert "carol" in all_docs
