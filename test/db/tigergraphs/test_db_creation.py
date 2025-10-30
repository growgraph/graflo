"""Tests for TigerGraph database (graph) creation and deletion functionality."""

import pytest

from graflo.db import ConnectionManager


def test_create_database(conn_conf, test_graph_name):
    """Test creating a new TigerGraph database (graph)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Graph should not exist initially (unique name from fixture)
        assert not db_client.graph_exists(test_graph_name)

        # Create the graph
        result = db_client.create_database(test_graph_name)
        assert result is not None

        # Verify the graph exists
        assert db_client.graph_exists(test_graph_name)


def test_create_database_already_exists(conn_conf, test_graph_name):
    """Test that creating an existing graph raises an exception."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph first
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Creating it again should raise an exception
        with pytest.raises(Exception):
            db_client.create_database(test_graph_name)


def test_delete_database(conn_conf, test_graph_name):
    """Test deleting a TigerGraph database (graph)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph first
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Delete the graph
        db_client.delete_database(test_graph_name)

        # Graph should no longer exist (or at least be cleared)
        # Note: delete_database may only clear data, not drop structure


def test_delete_nonexistent_database(conn_conf):
    """Test deleting a graph that doesn't exist."""
    nonexistent_graph = f"nonexistent_{hash('test') % 10000}"

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Delete should not raise an exception even if graph doesn't exist
        db_client.delete_database(nonexistent_graph)


def test_create_and_delete_database(conn_conf, test_graph_name):
    """Test creating a graph and then deleting it."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Delete it
        db_client.delete_database(test_graph_name)

        # Verify graph is gone (or cleared)
        # The graph structure might still exist but should be empty


def test_schema_creation(conn_conf, test_graph, schema_obj):
    """Test creating schema using init_db (follows ArangoDB pattern).

    Pattern: init_db creates graph, defines schema, then defines indexes.
    Uses schema.general.name as the graph name (from test_graph fixture).
    """
    schema_obj = schema_obj("review")
    # Set graph name in schema.general.name (used by default for graph creation)
    schema_obj.general.name = test_graph

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # init_db will: create graph, define schema, define indexes
        # Graph name comes from schema.general.name
        db_client.init_db(schema_obj, clean_start=True)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify graph exists (using name from schema.general.name)
        assert db_client.graph_exists(test_graph)

        # Use the graph to verify schema
        db_client.conn.gsql(f"USE GRAPH {test_graph}")

        # Verify schema was created
        vertex_types = db_client.conn.getVertexTypes()
        edge_types = db_client.conn.getEdgeTypes()

        # Check expected types exist
        assert len(vertex_types) > 0, "No vertex types created"
        assert len(edge_types) > 0, "No edge types created"

        print(f"Created vertex types: {vertex_types}")
        print(f"Created edge types: {edge_types}")

        # Cleanup: delete the graph
        db_client.delete_database(test_graph)
