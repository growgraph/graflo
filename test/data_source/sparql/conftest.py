"""Fixtures for SPARQL / RDF data source tests."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def fuseki_config():
    """Load Fuseki / SPARQL endpoint config from docker/fuseki/.env.example.

    Skips if the .env.example file is missing (CI without Fuseki).
    """
    from graflo.connections.onto import SparqlEndpointConfig

    try:
        config = SparqlEndpointConfig.from_docker_env()
    except FileNotFoundError:
        pytest.skip("docker/fuseki/.env.example not found – Fuseki not configured")
    return config
