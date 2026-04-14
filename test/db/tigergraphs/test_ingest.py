from test.conftest import ingest_atomic, fetch_schema_obj

import pytest

from graflo.db import ConnectionManager
from graflo import ComparisonOperator


@pytest.fixture(scope="function")
def modes():
    return ["review-tigergraph"]


def test_ingest(
    modes,
    conn_conf,
    current_path,
    test_graph_name,
    reset,
):
    for m in modes:
        schema_o = fetch_schema_obj(m)

        ingest_atomic(
            conn_conf,
            current_path,
            test_graph_name,
            schema_o=schema_o,
            mode=m,
        )

        if m == "review-tigergraph":
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_docs("Author")
                assert len(r) == 374
                r = db_client.fetch_docs("Author", filters=["==", "10", "hindex"])
                assert len(r) == 8
                r = db_client.fetch_docs(
                    "Author",
                    filters=[ComparisonOperator.EQ, "10", "hindex"],
                    return_keys=["full_name"],
                )
                assert len(r[0]) == 1

                authors = db_client.fetch_docs(
                    "Author", filters=["==", "309238221625", "id"]
                )
                assert len(authors) == 1

                author_id = authors[0]["id"]
                # Fetch edges from this vertex using pyTigerGraph
                edges = db_client.fetch_edges(
                    from_type="Author",
                    from_id=author_id,
                    edge_type="belongsTo",
                )
                assert len(edges) == 1
                # Keep clear_data regression coverage here to avoid a second full ingest pass.
                db_client.clear_data(schema_o)
                assert db_client.graph_exists(test_graph_name)
                authors_after_clear = db_client.fetch_docs("Author")
                assert len(authors_after_clear) == 0
