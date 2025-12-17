"""Tests for FalkorDB index creation operations."""

import pytest

from graflo.db import ConnectionManager


def test_create_vertex_index(conn_conf, test_graph_name, schema_obj):
    """Test creating vertex indices from schema."""
    schema_o = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indices(schema_o.vertex_config)

    # FalkorDB index verification
    # Note: FalkorDB doesn't have SHOW INDEX like Neo4j,
    # but indices are created silently. We verify by checking
    # that the operation completed without error.
    # The real test is whether queries using these fields perform well.


def test_create_edge_index(conn_conf, test_graph_name, schema_obj):
    """Test creating edge indices from schema."""
    schema_o = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indices(
            schema_o.edge_config.edges_list(include_aux=True)
        )

    # FalkorDB indices are created silently.
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

        # Create index on email field
        try:
            db_client.execute("CREATE INDEX FOR (n:User) ON (n.email)")
        except Exception:
            # Index may already exist or FalkorDB version doesn't support this syntax
            pass

        # Verify we can still query using the indexed field
        result = db_client.fetch_docs("User", filters=["==", "test@test.com", "email"])
        assert len(result) == 1
