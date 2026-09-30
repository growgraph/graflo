"""Fixtures for SPARQL / RDF data source tests."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def fuseki_config():
    """Load Fuseki / SPARQL endpoint config from docker/fuseki/.env.

    Skips if the .env file is missing (CI without Fuseki).
    """
    from graflo.connections.onto import SparqlEndpointConfig

    try:
        config = SparqlEndpointConfig.from_docker_env()
    except FileNotFoundError:
        pytest.skip("docker/fuseki/.env not found – Fuseki not configured")
    return config


@pytest.fixture(scope="module")
def fuseki_query_endpoint(fuseki_config) -> str:
    """Full SPARQL query endpoint URL for the Fuseki test dataset."""
    return fuseki_config.query_endpoint
