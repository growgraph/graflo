import os

import pytest
from suthing import FileHandle
from graflo.backend import ConfigFactory
from graflo.backend import ConnectionManager


@pytest.fixture(scope="function")
def test_db_port():
    FileHandle.load("docker.neo4j", ".env")
    port = os.environ["NEO4J_BOLT_PORT"]
    return port


@pytest.fixture(scope="function")
def creds():
    FileHandle.load("docker.neo4j", ".env")
    creds = os.environ["NEO4J_AUTH"].split("/")
    cred_name, cred_pass = creds[0], creds[1]
    return cred_name, cred_pass


@pytest.fixture(scope="function")
def conn_conf(test_db_port, creds):
    cred_name, cred_pass = creds

    db_args = {
        "protocol": "bolt",
        "hostname": "localhost",
        "cred_name": cred_name,
        "cred_pass": cred_pass,
        "port": test_db_port,
        "database": "_system",
        "db_type": "neo4j",
    }
    conn_conf = ConfigFactory.create_config(db_args)
    return conn_conf


@pytest.fixture()
def clean_db(conn_conf):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_graph_structure()


@pytest.fixture(scope="function")
def test_db_name():
    return "neo4j"
