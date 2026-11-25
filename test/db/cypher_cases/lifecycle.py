"""Connection, CRUD, ingestion and index tests shared as module-level functions.

Backend-agnostic Cypher tests, shared by the Memgraph and FalkorDB suites.

Not collected here: the module is outside any ``test_*.py``. Each backend's
``test_ingest.py`` / ``test_db_creation.py`` / ``test_db_index.py`` imports the
functions (and the ``modes`` fixture) it runs, so pytest collects them under
that module's node id; the backend conftest supplies the fixtures.
"""

from __future__ import annotations

import pytest

from graflo.db.manager import ConnectionManager
from graflo.onto import AggregationType
from test.conftest import fetch_schema_obj, ingest_atomic

# =============================================================================
# INGESTION AND AGGREGATION (test_ingest.py)
# =============================================================================


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

        unique_vals = db_client.aggregate(
            "Item", AggregationType.SORTED_UNIQUE, aggregated_field="category"
        )
        assert unique_vals == ["A", "B", "C"]


def test_aggregation_grouped_by_every_function(conn_conf, test_graph_name, clean_db):
    """A discriminant groups every function: {group value: the ungrouped result}."""
    _ = clean_db

    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [
            {"id": "1", "kind": "a", "score": 1},
            {"id": "2", "kind": "a", "score": 3},
            {"id": "3", "kind": "a", "score": 3},
            {"id": "4", "kind": "b", "score": 5},
            {"id": "5", "kind": "c"},
        ]
        db_client.upsert_docs_batch(docs, "Graded", match_keys=["id"])

        def grouped(function):
            return db_client.aggregate(
                "Graded", function, discriminant="kind", aggregated_field="score"
            )

        assert grouped(AggregationType.COUNT) == {"a": 3, "b": 1, "c": 1}
        assert grouped(AggregationType.MAX) == {"a": 3, "b": 5, "c": None}
        assert grouped(AggregationType.MIN) == {"a": 1, "b": 5, "c": None}
        assert grouped(AggregationType.AVERAGE) == {
            "a": pytest.approx(7 / 3),
            "b": 5.0,
            "c": None,
        }
        assert grouped(AggregationType.SORTED_UNIQUE) == {
            "a": [1, 3],
            "b": [5],
            "c": [],
        }


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


# =============================================================================
# CONNECTION AND BASIC CRUD (test_db_creation.py)
# =============================================================================


def test_connection_initialization(conn_conf, cypher_traits):
    """Test that the connection can be initialized."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert db_client is not None
        assert getattr(db_client, cypher_traits.connection_handle) is not None


def test_delete_graph_structure(conn_conf, test_graph_name):
    """Test that graph structure can be deleted."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # First create some data
        db_client.execute("CREATE (n:TestNode {name: 'test'})")

        # Verify data exists
        result = db_client.execute("MATCH (n:TestNode) RETURN count(n) as count")
        assert result.result_set[0][0] == 1

        # Delete graph structure
        db_client.delete_graph_structure(delete_all=True)

        # Verify data is deleted
        result = db_client.execute("MATCH (n) RETURN count(n) as count")
        assert result.result_set[0][0] == 0


def test_upsert_single_node(conn_conf, test_graph_name, clean_db):
    """Test upserting a single node."""
    _ = clean_db
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"id": "1", "name": "Test User", "email": "test@example.com"}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        # Verify node was created
        result = db_client.fetch_docs("User")
        assert len(result) == 1
        assert result[0]["name"] == "Test User"


def test_upsert_updates_existing_node(conn_conf, test_graph_name, clean_db):
    """Test that upsert updates existing nodes."""
    _ = clean_db
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Insert initial node
        docs = [{"id": "1", "name": "Original Name"}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        # Upsert with updated data
        docs = [{"id": "1", "name": "Updated Name"}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        # Verify only one node exists with updated name
        result = db_client.fetch_docs("User")
        assert len(result) == 1
        assert result[0]["name"] == "Updated Name"


def test_insert_edges(conn_conf, test_graph_name, clean_db):
    """Test inserting edges between nodes."""
    _ = clean_db
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create source and target nodes
        db_client.upsert_docs_batch(
            [{"id": "1", "name": "User 1"}], "User", match_keys=["id"]
        )
        db_client.upsert_docs_batch(
            [{"id": "2", "name": "User 2"}], "User", match_keys=["id"]
        )

        # Create edge
        edge_docs = [[{"id": "1"}, {"id": "2"}, {"since": "2024-01-01"}]]
        db_client.insert_edges_batch(
            edge_docs,
            source_class="User",
            target_class="User",
            relation_name="FOLLOWS",
            match_keys_source=["id"],
            match_keys_target=["id"],
        )

        # Verify edge exists
        result = db_client.execute(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN count(r) as count"
        )
        assert result.result_set[0][0] == 1


def test_fetch_docs_with_filter(conn_conf, test_graph_name, clean_db):
    """Test fetching documents with filters."""
    _ = clean_db
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create multiple nodes
        docs = [
            {"id": "1", "name": "Alice", "age": 25},
            {"id": "2", "name": "Bob", "age": 30},
            {"id": "3", "name": "Charlie", "age": 25},
        ]
        db_client.upsert_docs_batch(docs, "Person", match_keys=["id"])

        # Fetch with filter
        result = db_client.fetch_docs("Person", filters=["==", 25, "age"])
        assert len(result) == 2


def test_fetch_docs_with_limit(conn_conf, test_graph_name, clean_db):
    """Test fetching documents with limit."""
    _ = clean_db
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create multiple nodes
        docs = [{"id": str(i), "name": f"User {i}"} for i in range(10)]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        # Fetch with limit
        result = db_client.fetch_docs("User", limit=5)
        assert len(result) == 5


def test_fetch_docs_with_projection(conn_conf, test_graph_name, clean_db):
    """Test fetching documents with key projection."""
    _ = clean_db
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"id": "1", "name": "Test", "email": "test@test.com", "age": 30}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        # Fetch with specific keys
        result = db_client.fetch_docs("User", return_keys=["name", "email"])
        assert len(result) == 1
        assert "name" in result[0]
        assert "email" in result[0]
        # age should not be in the result
        assert "age" not in result[0]


# =============================================================================
# INDEXES (test_db_index.py)
# =============================================================================


def test_create_vertex_index(conn_conf, test_graph_name, schema_obj):
    """Test creating vertex indexes from schema."""
    schema_o = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indexes(
            schema_o.core_schema.vertex_config, schema=schema_o
        )

    # Vertex indexes are created silently: completing without error is the check.
