"""Tests for TigerGraph database (graph) creation and deletion functionality."""

import uuid

import pytest

from graflo.db import ConnectionManager
from graflo.db.tigergraph.conn import TigerGraphConnection


def test_create_database(conn_conf, test_graph_name):
    """Test creating a new TigerGraph database (graph)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Graph should not exist initially (unique name from fixture)
        assert not db_client.graph_exists(test_graph_name)

        # Create the graph
        db_client.create_database(test_graph_name)

        # Verify the graph exists
        assert db_client.graph_exists(test_graph_name)


def test_create_database_already_exists(conn_conf, test_graph_name):
    """Test that creating an existing graph raises an exception."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph first
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Creating it again should raise an exception
        with pytest.raises(RuntimeError, match="already exists"):
            db_client.create_database(test_graph_name)


def test_delete_database(conn_conf, test_graph_name):
    """Test deleting a TigerGraph database (graph)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph first
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Delete the graph
        db_client.delete_database(test_graph_name)

        # Graph should no longer exist after successful drop.
        assert not db_client.graph_exists(test_graph_name)


def test_delete_nonexistent_database(conn_conf):
    """Test deleting a graph that doesn't exist."""
    nonexistent_graph = f"nonexistent_{uuid.uuid4().hex[:8]}"

    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert not db_client.graph_exists(nonexistent_graph)
        # Delete should not raise an exception even if graph doesn't exist
        db_client.delete_database(nonexistent_graph)
        assert not db_client.graph_exists(nonexistent_graph)


def test_create_and_delete_database(conn_conf, test_graph_name):
    """Test creating a graph and then deleting it."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Delete it
        db_client.delete_database(test_graph_name)
        assert not db_client.graph_exists(test_graph_name)


def test_schema_creation(conn_conf, test_graph_name, schema_obj):
    """Test creating schema using init_db (follows ArangoDB pattern).

    Pattern: init_db creates graph, defines schema, then defines indexes.
    Uses schema.metadata.name as the graph name (from test_graph fixture).

    Note: In TigerGraph, vertex and edge types are global and shared between graphs.
    The test verifies that types are created and associated with the test graph.
    The test verifies that types are created and associated with the test graph.
    """
    schema_obj = schema_obj("review")
    # Set graph name in schema.metadata.name; conn_conf.database is set by fixture
    schema_obj.metadata.name = test_graph_name

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # init_db will: create graph, define schema, define indexes
        # Graph name comes from schema.metadata.name
        db_client.init_db(schema_obj, recreate_schema=True)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify graph exists (using name from schema.metadata.name)
        assert db_client.graph_exists(test_graph_name)

        # Use the graph context to verify schema
        # getVertexTypes() and getEdgeTypes() require graph context via _ensure_graph_context
        with db_client._ensure_graph_context(test_graph_name):
            # Verify schema was created
            vertex_types = db_client._get_vertex_types()
            edge_types = db_client._get_edge_types()

            # Check expected types exist
            assert len(vertex_types) > 0, "No vertex types created"
            assert len(edge_types) > 0, "No edge types created"

            print(f"Created vertex types: {vertex_types}")
            print(f"Created edge types: {edge_types}")


def test_schema_creation_edges(conn_conf, test_graph_name, schema_obj):
    """Test creating schema using init_db (follows ArangoDB pattern).

    Pattern: init_db creates graph, defines schema, then defines indexes.
    Uses schema.metadata.name as the graph name (from test_graph fixture).

    Note: In TigerGraph, vertex and edge types are global and shared between graphs.
    The test verifies that types are created and associated with the test graph.
    The test verifies that types are created and associated with the test graph.
    """
    schema_obj = schema_obj("review-tigergraph-edges")
    # Set graph name in schema.metadata.name; conn_conf.database is set by fixture
    schema_obj.metadata.name = test_graph_name

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # init_db will: create graph, define schema, define indexes
        # Graph name comes from schema.metadata.name
        db_client.init_db(schema_obj, recreate_schema=True)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify graph exists (using name from schema.metadata.name)
        assert db_client.graph_exists(test_graph_name)

        # Use the graph context to verify schema
        # getVertexTypes() and getEdgeTypes() require graph context via _ensure_graph_context
        with db_client._ensure_graph_context(test_graph_name):
            # Verify schema was created
            vertex_types = db_client._get_vertex_types()
            edge_types = db_client._get_edge_types()

            # Check expected types exist
            assert len(vertex_types) > 0, "No vertex types created"
            assert len(edge_types) == 1, "No edge types created"
            assert len(edge_types["contains"]) == 2, (
                "Should have to edges for relation `contains`"
            )

            print(f"Created vertex types: {vertex_types}")
            print(f"Created edge types: {edge_types}")


def test_delete_database_scopes_query_cleanup_to_target_graph() -> None:
    """delete_database should clean only the target graph (queries, then jobs) before DROP GRAPH."""
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    dropped_graph_queries: list[str] = []
    executed_gsql: list[str] = []

    def _fake_drop_installed_queries_for_graph(graph_name: str) -> None:
        dropped_graph_queries.append(graph_name)

    def _fake_execute_gsql(gsql: str):
        executed_gsql.append(gsql)
        return {"accepted": True}

    conn._drop_installed_queries_for_graph = _fake_drop_installed_queries_for_graph
    conn._execute_gsql = _fake_execute_gsql

    graph_name = f"g_query_scope_a_{uuid.uuid4().hex[:8]}"
    conn.delete_database(graph_name)

    assert dropped_graph_queries == [graph_name]
    assert executed_gsql == [
        f"USE GRAPH {graph_name}\nSHOW JOB *",
        f"USE GLOBAL\nDROP GRAPH {graph_name}",
    ]
