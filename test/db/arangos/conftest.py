import os
from test.conftest import ingest_atomic, verify

import pytest
from suthing import ConfigFactory, FileHandle

from graflo.db import ConnectionManager
from graflo.filter.onto import ComparisonOperator
from graflo.onto import AggregationType
from test.conftest import fetch_schema_obj


@pytest.fixture(scope="function")
def test_db_name():
    return "testdb"


@pytest.fixture(scope="function")
def test_db_port():
    FileHandle.load("docker.arango", ".env")
    port = os.environ["ARANGO_PORT"]
    return port


@pytest.fixture(scope="function")
def conn_conf(test_db_port):
    cred_pass = FileHandle.load("docker.arango", "test.arango.secret")

    db_args = {
        "protocol": "http",
        "hostname": "localhost",
        "port": test_db_port,
        "cred_name": "root",
        "cred_pass": cred_pass,
        "database": "_system",
        "db_type": "arango",
    }

    conn_conf = ConfigFactory.create_config(db_args)
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


def ingest_files(
    create_db, modes, conn_conf, current_path, test_db_name, reset, n_cores=1
):
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
        if m == "lake_odds":
            conn_conf.database = test_db_name
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_docs("chunks")
                assert len(r) == 2
                assert r[0]["data"]
                r = db_client.fetch_docs("chunks", filters=["==", "odds", "kind"])
                assert len(r) == 1
                r = db_client.fetch_docs("chunks", limit=1)
                assert len(r) == 1
                r = db_client.fetch_docs(
                    "chunks",
                    filters=["==", "odds", "kind"],
                    return_keys=["kind"],
                )
                assert len(r[0]) == 1
            batch = [{"kind": "odds"}, {"kind": "strange"}]
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_present_documents(
                    batch,
                    "chunks",
                    match_keys=("kind",),
                    keep_keys=("_key",),
                    flatten=False,
                )
                assert len(r) == 1

            batch = [{"kind": "odds"}, {"kind": "scores"}, {"kind": "strange"}]
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_present_documents(
                    batch,
                    "chunks",
                    match_keys=("kind",),
                    keep_keys=("_key",),
                    flatten=False,
                    filters=[ComparisonOperator.NEQ, "odds", "kind"],
                )
                assert len(r) == 1

            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.keep_absent_documents(
                    batch,
                    "chunks",
                    match_keys=("kind",),
                    keep_keys=("_key",),
                    filters=[ComparisonOperator.EQ, None, "data"],
                )
                assert len(r) == 3

            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.aggregate(
                    "chunks",
                    aggregation_function=AggregationType.COUNT,
                    discriminant="kind",
                )
                assert len(r) == 2
                assert r == [
                    {"kind": "odds", "_value": 1},
                    {"kind": "scores", "_value": 1},
                ]

            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.aggregate(
                    "chunks",
                    aggregation_function=AggregationType.COUNT,
                    discriminant="kind",
                    filters=[ComparisonOperator.NEQ, "odds", "kind"],
                )
                assert len(r) == 1
