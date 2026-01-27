from test.conftest import ingest_atomic
from test.db.arangos.conftest import verify_from_db

from graflo import ConnectionManager


def test_ingest(
    create_db,
    conn_conf,
    schema_obj,
    current_path,
    test_db_name,
    reset,
):
    m = "oa-institution"
    _ = create_db
    schema_o = schema_obj(m)

    ingest_atomic(conn_conf, current_path, test_db_name, schema_o, mode=m)

    verify_from_db(
        conn_conf,
        current_path,
        test_db_name,
        mode=m,
        reset=reset,
    )

    with ConnectionManager(connection_config=conn_conf) as db_client:
        r = db_client.fetch_docs("institutions_institutions_edges")
        assert len(r) == 3
        assert all([item["relation"] == "child" for item in r])
