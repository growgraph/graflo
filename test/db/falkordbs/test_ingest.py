"""Tests for FalkorDB data ingestion operations."""

import pytest

from test.conftest import fetch_schema_obj, ingest_atomic

from graflo.db import ConnectionManager


@pytest.fixture(scope="function")
def modes():
    """Test modes to use for ingestion tests."""
    return ["review"]


def test_ingest(
    clean_db,
    modes,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Test full data ingestion workflow.

    This test ingests the review dataset and verifies:
    - Correct number of nodes created
    - Filtering works correctly
    - Limit works correctly
    - Projection (return_keys) works correctly
    """
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
                # Test basic fetch
                r = db_client.fetch_docs("Author")
                assert len(r) == 374, f"Expected 374 authors, got {len(r)}"

                # Test fetch with filter
                r = db_client.fetch_docs("Author", filters=["==", "10", "hindex"])
                assert len(r) == 8, f"Expected 8 authors with hindex=10, got {len(r)}"

                # Test fetch with limit
                r = db_client.fetch_docs("Author", limit=1)
                assert len(r) == 1, f"Expected 1 author with limit=1, got {len(r)}"

                # Test fetch with projection
                r = db_client.fetch_docs(
                    "Author",
                    filters=["==", "10", "hindex"],
                    return_keys=["full_name"],
                )
                assert len(r[0]) == 1, f"Expected 1 key in projection, got {len(r[0])}"


def test_aggregation_count(conn_conf, test_graph_name, clean_db):
    """Test COUNT aggregation."""
    _ = clean_db

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create test data
        docs = [
            {"id": "1", "type": "A"},
            {"id": "2", "type": "A"},
            {"id": "3", "type": "B"},
        ]
        db_client.upsert_docs_batch(docs, "Item", match_keys=["id"])

        # Test simple count
        from graflo.onto import AggregationType

        count = db_client.aggregate("Item", AggregationType.COUNT)
        assert count == 3

        # Test count with discriminant (group by)
        grouped = db_client.aggregate(
            "Item", AggregationType.COUNT, discriminant="type"
        )
        assert grouped.get("A") == 2
        assert grouped.get("B") == 1


def test_aggregation_min_max_avg(conn_conf, test_graph_name, clean_db):
    """Test MIN, MAX, and AVERAGE aggregations."""
    _ = clean_db

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create test data with numeric field
        docs = [
            {"id": "1", "score": 10},
            {"id": "2", "score": 20},
            {"id": "3", "score": 30},
        ]
        db_client.upsert_docs_batch(docs, "Score", match_keys=["id"])

        from graflo.onto import AggregationType

        # Test MIN
        min_val = db_client.aggregate(
            "Score", AggregationType.MIN, aggregated_field="score"
        )
        assert min_val == 10

        # Test MAX
        max_val = db_client.aggregate(
            "Score", AggregationType.MAX, aggregated_field="score"
        )
        assert max_val == 30

        # Test AVERAGE
        avg_val = db_client.aggregate(
            "Score", AggregationType.AVERAGE, aggregated_field="score"
        )
        assert avg_val == 20.0


def test_aggregation_sorted_unique(conn_conf, test_graph_name, clean_db):
    """Test SORTED_UNIQUE aggregation."""
    _ = clean_db

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create test data with duplicate values
        docs = [
            {"id": "1", "category": "B"},
            {"id": "2", "category": "A"},
            {"id": "3", "category": "C"},
            {"id": "4", "category": "A"},
        ]
        db_client.upsert_docs_batch(docs, "Item", match_keys=["id"])

        from graflo.onto import AggregationType

        unique_vals = db_client.aggregate(
            "Item", AggregationType.SORTED_UNIQUE, aggregated_field="category"
        )
        assert unique_vals == ["A", "B", "C"]


def test_keep_absent_documents(conn_conf, test_graph_name, clean_db):
    """Test keep_absent_documents functionality."""
    _ = clean_db

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create some initial data
        existing_docs = [
            {"id": "1", "name": "Existing 1"},
            {"id": "2", "name": "Existing 2"},
        ]
        db_client.upsert_docs_batch(existing_docs, "User", match_keys=["id"])

        # Check which documents from a batch are absent
        batch_to_check = [
            {"id": "1", "name": "Existing 1"},  # exists
            {"id": "3", "name": "New 3"},  # absent
            {"id": "4", "name": "New 4"},  # absent
        ]

        absent = db_client.keep_absent_documents(
            batch_to_check, "User", match_keys=["id"], keep_keys=["id", "name"]
        )

        assert len(absent) == 2
        absent_ids = {doc["id"] for doc in absent}
        assert "3" in absent_ids
        assert "4" in absent_ids
        assert "1" not in absent_ids
