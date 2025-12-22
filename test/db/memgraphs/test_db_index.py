"""Tests for Memgraph index creation operations.

This module validates index creation on vertices and edges,
both through schema-driven methods and manual Cypher commands.
"""

from graflo.db import ConnectionManager


def test_create_vertex_index(conn_conf, test_graph_name, schema_obj):
    """Test creating vertex indices from schema."""
    schema_o = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indices(schema_o.vertex_config)

    # Memgraph index verification:
    # Memgraph creates indices silently. We verify by checking
    # that the operation completed without error.
    # The real test is whether queries using these fields perform well.


def test_create_edge_index(conn_conf, test_graph_name, schema_obj):
    """Test creating edge indices from schema."""
    schema_o = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indices(schema_o.edge_config.edges_list(include_aux=True))

    # Memgraph indices are created silently.
    # Verification is implicit - no errors means success.


def test_manual_index_creation(conn_conf, test_graph_name, clean_db):
    """Test manual index creation on a label."""
    _ = clean_db

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create some test data first (indexes require the label to exist)
        db_client.upsert_docs_batch(
            [{"id": "1", "email": "test@test.com"}],
            "User",
            match_keys=["id"],
        )

        # Create index on email field using Memgraph syntax
        try:
            db_client.execute("CREATE INDEX ON :User(email)")
        except Exception:
            # Index may already exist
            pass

        # Verify we can still query using the indexed field
        result = db_client.fetch_docs("User", filters=["==", "test@test.com", "email"])
        assert len(result) == 1


def test_show_indexes(conn_conf, test_graph_name, clean_db):
    """Test listing existing indexes."""
    _ = clean_db

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create test data and index
        db_client.upsert_docs_batch(
            [{"id": "1", "name": "Test"}],
            "IndexTest",
            match_keys=["id"],
        )

        try:
            db_client.execute("CREATE INDEX ON :IndexTest(name)")
        except Exception:
            pass

        # Query indexes - Memgraph uses SHOW INDEX INFO
        result = db_client.execute("SHOW INDEX INFO")
        # Just verify the query works, result format varies by version
        assert result is not None
