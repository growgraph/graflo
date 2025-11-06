import os
import uuid

import pytest
from suthing import FileHandle

from graflo.db import ConfigFactory, ConnectionManager

# Set GSQL_PASSWORD environment variable for TigerGraph tests
os.environ.setdefault("GSQL_PASSWORD", "tigergraph")


@pytest.fixture(scope="function")
def test_db_port():
    FileHandle.load("docker.tigergraph", ".env")
    port = os.environ["TG_REST"]
    return port


@pytest.fixture(scope="function")
def test_gs_port():
    FileHandle.load("docker.tigergraph", ".env")
    port = os.environ["TG_WEB"]
    return port


@pytest.fixture(scope="function")
def creds():
    FileHandle.load("docker.tigergraph", ".env")
    cred_name = "tigergraph"
    cred_pass = os.environ.get("GSQL_PASSWORD", "tigergraph")
    return cred_name, cred_pass


@pytest.fixture(scope="function")
def conn_conf(test_db_port, test_gs_port, creds):
    username, password = creds

    db_args = {
        "protocol": "http",
        "hostname": "localhost",
        "username": username,
        "password": password,
        "port": test_db_port,
        "gs_port": test_gs_port,
        "db_type": "tigergraph",
    }
    conn_conf = ConfigFactory.create_config(db_args)
    return conn_conf


@pytest.fixture()
def clean_db(conn_conf):
    """Fixture to clean all graphs, edges, and vertices before a test."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_graph_structure([], [], delete_all=True)


@pytest.fixture(scope="function")
def test_graph_name(conn_conf):
    """Fixture providing a test graph name for TigerGraph tests with automatic cleanup.

    The graph name is generated with a UUID suffix to make it less conspicuous.
    After the test completes, the graph and all global vertex/edge types will be deleted.

    Note: For schema-based tests, use test_graph fixture instead and set
    schema.general.name = test_graph.
    """
    # Generate a less conspicuous graph name with UUID suffix
    graph_uuid = str(uuid.uuid4()).replace("-", "")[:8]
    graph_name = f"g{graph_uuid}"

    # Set as default database/graph name for this test's connection
    conn_conf.database = graph_name

    yield graph_name

    # Cleanup: Delete the graph and all global vertex/edge types after the test
    try:
        with ConnectionManager(connection_config=conn_conf) as db_client:
            # Delete all graphs, edges, and vertices to ensure clean state
            db_client.delete_graph_structure([], [], delete_all=True)
    except Exception:
        # Silently ignore cleanup errors
        pass
