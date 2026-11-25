"""Grafeo ingestion and aggregation.

The aggregation cases are shared from cypher_cases. Ingestion runs the shared
review data without its edge index, which Grafeo cannot build (it indexes no
relationship property).
"""

from graflo.db.manager import ConnectionManager
from test.conftest import fetch_schema_obj, ingest_atomic
from test.db.cypher_cases.lifecycle import (
    test_aggregation_count,
    test_aggregation_grouped_by_every_function,
    test_aggregation_min_max_avg,
    test_aggregation_sorted_unique,
    test_keep_absent_documents,
)

__all__ = [
    "test_aggregation_count",
    "test_aggregation_grouped_by_every_function",
    "test_aggregation_min_max_avg",
    "test_aggregation_sorted_unique",
    "test_keep_absent_documents",
]


def test_ingest(clean_db, conn_conf, current_path, test_db_name, reset):
    schema_o = fetch_schema_obj("review")
    for spec in schema_o.db_profile.edge_specs:
        spec.indexes = []
    ingest_atomic(
        conn_conf, current_path, test_db_name, schema_o=schema_o, mode="review"
    )

    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert len(db_client.fetch_docs("Author")) == 374
        assert len(db_client.fetch_docs("Author", filters=["==", "10", "hindex"])) == 8
        assert len(db_client.fetch_docs("Author", limit=1)) == 1
        projected = db_client.fetch_docs(
            "Author", filters=["==", "10", "hindex"], return_keys=["full_name"]
        )
        assert len(projected[0]) == 1
        author = db_client.fetch_docs("Author", filters=["==", "309238221625", "id"])
        edges = db_client.fetch_edges(
            from_type="Author", from_id=author[0]["id"], edge_type="belongsTo"
        )
        assert len(edges) == 1
