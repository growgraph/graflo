"""Fixtures for SPARQL / RDF data source tests."""

from __future__ import annotations

from pathlib import Path

import pytest


DATA_DIR = Path(__file__).parent / "data"
SAMPLE_ONTOLOGY = DATA_DIR / "sample_ontology.ttl"
SAMPLE_DATA = DATA_DIR / "sample_data.ttl"


@pytest.fixture()
def sample_ontology_path() -> Path:
    """Path to the sample TBox-only ontology file."""
    return SAMPLE_ONTOLOGY


@pytest.fixture()
def sample_data_path() -> Path:
    """Path to the sample combined TBox + ABox data file."""
    return SAMPLE_DATA


@pytest.fixture(scope="module")
def fuseki_config():
    """Load Fuseki / SPARQL endpoint config from docker/fuseki/.env.

    Skips if the .env file is missing (CI without Fuseki).
    """
    from graflo.db import SparqlEndpointConfig

    try:
        config = SparqlEndpointConfig.from_docker_env()
    except FileNotFoundError:
        pytest.skip("docker/fuseki/.env not found – Fuseki not configured")
    return config


@pytest.fixture(scope="module")
def fuseki_query_endpoint(fuseki_config) -> str:
    """Full SPARQL query endpoint URL for the Fuseki test dataset."""
    return fuseki_config.query_endpoint
