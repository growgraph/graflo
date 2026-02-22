from test.conftest import ingest_atomic, verify

import pytest

from graflo.db import ConnectionManager
from graflo.db.connection.onto import ArangoConfig
from test.conftest import fetch_schema_obj


@pytest.fixture(scope="function")
def test_db_name():
    return "testdb"


@pytest.fixture(scope="function")
def conn_conf():
    """Load ArangoDB config from docker/arango/.env file."""
    conn_conf = ArangoConfig.from_docker_env()
    if not conn_conf.database:
        conn_conf.database = "_system"
    return conn_conf


@pytest.fixture(scope="function")
def create_db(conn_conf, test_db_name):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.create_database(test_db_name)
        db_client.delete_graph_structure([], [], delete_all=True)


def verify_from_db(conn_conf, current_path, test_db_name, mode, reset):
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        cols = db_client.get_collections()
        vc = {}
        contents = {}
        collections = [c["name"] for c in cols if not c["system"]]

        for c in sorted(collections):
            cursor = db_client.execute(f"return LENGTH({c})")
            size = next(cursor)
            vc[c] = size
            contents[c] = db_client.fetch_docs(
                c,
                unset_keys=[
                    "_id",
                    "_rev",
                    "publication@_id",
                ],
            )
    for k, docs in contents.items():
        if k not in ["mentions", "entities"]:
            for d in docs:
                d.pop("_key", None)

        ks = k.split("_")
        if len(ks) > 1:
            if set(ks[:2]) in {"mentions", "entities"}:
                pass
            else:
                if ks[0] not in {"mentions", "entities"}:
                    for d in docs:
                        d.pop("_from", None)
                if ks[1] not in {"mentions", "entities"}:
                    for d in docs:
                        d.pop("_to", None)

    verify(vc, current_path, mode, test_type="db", reset=reset)
    verify(
        contents,
        current_path,
        mode,
        test_type="db",
        kind="contents",
        reset=reset,
    )


def ingest(create_db, modes, conn_conf, current_path, test_db_name, reset, n_cores=1):
    _ = create_db
    for m in modes:
        schema_o = fetch_schema_obj(m)
        ingest_atomic(
            conn_conf, current_path, test_db_name, schema_o, mode=m, n_cores=n_cores
        )
        verify_from_db(
            conn_conf,
            current_path,
            test_db_name,
            mode=m,
            reset=reset,
        )
