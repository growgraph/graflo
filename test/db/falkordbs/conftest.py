"""Pytest fixtures for FalkorDB connector tests.

This module provides test fixtures for FalkorDB integration tests,
including database configuration and cleanup utilities.
"""

import uuid

import pytest

from graflo.db import ConnectionManager
from graflo.db import FalkordbConfig


@pytest.fixture(scope="function")
def conn_conf():
    """Load FalkorDB config from docker/falkordb/.env file."""
    conn_conf = FalkordbConfig.from_docker_env()
    return conn_conf


@pytest.fixture(scope="function")
def test_graph_name(conn_conf):
    """Generate unique graph name with UUID, cleanup after test.

    This ensures test isolation by using a unique graph for each test.
    """
    graph_uuid = str(uuid.uuid4()).replace("-", "")[:8]
    graph_name = f"testgraph_{graph_uuid}"
    conn_conf.database = graph_name

    yield graph_name

    # Cleanup: delete the test graph after the test
    try:
        with ConnectionManager(connection_config=conn_conf) as db_client:
            db_client.delete_graph_structure(delete_all=True)
    except Exception:
        # Graph may not exist or already cleaned up
        pass


@pytest.fixture()
def clean_db(conn_conf, test_graph_name):
    """Clean database before test by deleting all nodes and relationships."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_graph_structure(delete_all=True)


@pytest.fixture(scope="function")
def test_db_name(test_graph_name):
    """Alias for test_graph_name for compatibility with shared test utilities."""
    return test_graph_name
