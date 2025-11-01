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
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_collections()


@pytest.fixture(scope="function")
def test_db_name():
    return "tigergraph"


# @pytest.fixture(scope="function")
# def test_graph_name():
#     """Fixture providing a test graph name for TigerGraph tests.
#
#     This name should be assigned to Schema.general.name and will be used
#     by default for create_database/graph operations.
#     """
#     # Generate a less conspicuous graph name with UUID suffix
#     graph_uuid = str(uuid.uuid4()).replace("-", "")[:8]
#     graph_name = f"g{graph_uuid}"
#     return graph_name


@pytest.fixture(scope="function")
def test_graph_name(conn_conf):
    """Fixture providing a test graph name for TigerGraph tests with automatic cleanup.

    The graph name is generated with a UUID suffix to make it less conspicuous.
    After the test completes, the graph will be automatically deleted.

    Note: For schema-based tests, use test_graph fixture instead and set
    schema.general.name = test_graph.
    """
    # Generate a less conspicuous graph name with UUID suffix
    graph_uuid = str(uuid.uuid4()).replace("-", "")[:8]
    graph_name = f"g{graph_uuid}"

    # Set as default database/graph name for this test's connection
    conn_conf.database = graph_name

    yield graph_name

    # Cleanup: Delete the graph after the test
    try:
        with ConnectionManager(connection_config=conn_conf) as db_client:
            db_client.delete_database(graph_name)
    except Exception:
        # Silently ignore cleanup errors
        pass
