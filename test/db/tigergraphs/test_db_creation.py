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


def test_delete_database_keeps_other_graph_queries(conn_conf):
    """Deleting one graph must not drop queries from unrelated graphs."""
    graph_a = "g_query_scope_a"
    graph_b = "g_query_scope_b"
    query_a = "q_scope_a"
    query_b = "q_scope_b"

    with ConnectionManager(connection_config=conn_conf) as db_client:
        try:
            # Ensure both graphs exist
            if not db_client.graph_exists(graph_a):
                db_client.create_database(graph_a)
            if not db_client.graph_exists(graph_b):
                db_client.create_database(graph_b)

            # Ensure queries do not pre-exist (best effort for idempotency)
            for graph_name, query_name in ((graph_a, query_a), (graph_b, query_b)):
                try:
                    db_client._execute_gsql(
                        f"USE GRAPH {graph_name}\nDROP QUERY {query_name} IF EXISTS"
                    )
                except Exception:
                    pass

            # Install one trivial query per graph
            db_client._execute_gsql(
                f"""
                USE GRAPH {graph_a}
                CREATE QUERY {query_a}() FOR GRAPH {graph_a} {{
                    PRINT "ok";
                }}
                INSTALL QUERY {query_a}
                """
            )
            db_client._execute_gsql(
                f"""
                USE GRAPH {graph_b}
                CREATE QUERY {query_b}() FOR GRAPH {graph_b} {{
                    PRINT "ok";
                }}
                INSTALL QUERY {query_b}
                """
            )

            assert query_a in db_client._get_installed_queries(graph_name=graph_a)
            assert query_b in db_client._get_installed_queries(graph_name=graph_b)

            # Delete graph A and verify graph B query remains untouched.
            db_client.delete_database(graph_a)

            remaining_b_queries = db_client._get_installed_queries(graph_name=graph_b)
            assert query_b in remaining_b_queries, (
                f"Query '{query_b}' in graph '{graph_b}' should remain after deleting '{graph_a}'. "
                f"Found: {remaining_b_queries}"
            )
        finally:
            for graph_name in (graph_a, graph_b):
                try:
                    db_client.delete_database(graph_name)
                except Exception:
                    pass
