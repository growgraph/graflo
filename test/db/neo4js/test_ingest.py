import pytest

from graflo.db import ConnectionManager
from test.conftest import fetch_schema_obj, ingest_atomic


@pytest.fixture(scope="function")
def modes():
    return ["review"]


def test_ingest(
    clean_db,
    modes,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    _ = clean_db
    for m in modes:
        schema_o = fetch_schema_obj(m)
        ingest_atomic(
            conn_conf,
            current_path,
            test_db_name,
            schema_o=schema_o,
            mode=m,
        )
        if m == "review":
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_docs("Author")
                assert len(r) == 374
                r = db_client.fetch_docs("Author", filters=["==", "10", "hindex"])
                assert len(r) == 8
                r = db_client.fetch_docs("Author", limit=1)
                assert len(r) == 1
                r = db_client.fetch_docs(
                    "Author",
                    filters=["==", "10", "hindex"],
                    return_keys=["full_name"],
                )
                assert len(r[0]) == 1
