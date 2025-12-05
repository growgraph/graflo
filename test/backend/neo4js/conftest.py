import pytest

from graflo.backend import ConnectionManager
from graflo.backend.connection.onto import Neo4jConfig


@pytest.fixture(scope="function")
def conn_conf():
    """Load Neo4j config from docker/neo4j/.env file."""
    conn_conf = Neo4jConfig.from_docker_env()
    # Ensure database is set
    if not conn_conf.database:
        conn_conf.database = "_system"
    return conn_conf


@pytest.fixture()
def clean_db(conn_conf):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_graph_structure()


@pytest.fixture(scope="function")
def test_db_name():
    return "neo4j"
