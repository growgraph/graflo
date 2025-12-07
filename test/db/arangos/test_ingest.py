from test.db.arangos.conftest import ingest
import pytest


@pytest.fixture(scope="function")
def modes():
    return [
        "kg",
        "ibes",
    ]


def test_ingest(
    create_db,
    modes,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    ingest(
        create_db,
        modes,
        conn_conf,
        current_path,
        test_db_name,
        reset,
        n_cores=1,
    )
