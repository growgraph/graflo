"""Fixtures for the Grafeo connector tests.

Grafeo is embedded, so these tests need no container: each test gets its own
named graph in the process's in-memory database, dropped afterwards.
"""

import uuid

import pytest

# Before the shared cases are imported, so their asserts are rewritten too.
pytest.register_assert_rewrite("test.db.cypher_cases")

from graflo.connections.grafeo import GrafeoConfig
from graflo.db.manager import ConnectionManager
from test.db.cypher_cases.traits import CypherTraits


@pytest.fixture(scope="function")
def conn_conf():
    """A config for the in-memory database; the graph is set per test."""
    return GrafeoConfig()


@pytest.fixture(scope="function")
def test_graph_name(conn_conf):
    """A fresh named graph for this test, dropped afterwards."""
    graph_name = f"testgraph_{uuid.uuid4().hex[:8]}"
    conn_conf.database = graph_name
    yield graph_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_database(graph_name)


@pytest.fixture()
def clean_db(conn_conf, test_graph_name):
    """Start from an empty graph."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_graph_structure(delete_all=True)


@pytest.fixture(scope="function")
def test_db_name(test_graph_name):
    """Alias of ``test_graph_name`` for the shared utilities."""
    return test_graph_name


@pytest.fixture(scope="session")
def cypher_traits() -> CypherTraits:
    """Engine behaviour the shared Cypher cases assert against."""
    return CypherTraits(
        connection_handle="db",
        upsert_null_removes_property=True,
        orders_mixed_types=True,
        min_single_insert_ops=100,
        min_read_ops=200,
        max_batch_sizes=(1000, 5000, 10000, 25000, 50000),
    )
