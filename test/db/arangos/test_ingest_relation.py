from pathlib import Path
from test.db.arangos.conftest import verify_from_db

from suthing import FileHandle

from graflo import Caster, ConnectionManager


def test_ingest(
    create_db,
    conn_conf,
    schema_obj,
    current_path,
    test_db_name,
    reset,
):
    _ = create_db
    schema_o = schema_obj("oa.institution")
    j_resource = FileHandle.load(Path(current_path) / "data/json/oa.institution.json")

    conn_conf.database = test_db_name

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(schema_o, clean_start=True)
    caster = Caster(schema_o)
    caster.process_resource(j_resource, "institutions", conn_conf=conn_conf)

    verify_from_db(
        conn_conf,
        current_path,
        test_db_name,
        mode="oa_relation",
        reset=reset,
    )

    with ConnectionManager(connection_config=conn_conf) as db_client:
        r = db_client.fetch_docs("institutions_institutions_edges")
        assert len(r) == 3
        assert all([item["relation"] == "child" for item in r])
