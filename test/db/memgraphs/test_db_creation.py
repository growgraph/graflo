"""Tests for Memgraph database/graph creation and deletion operations."""

from graflo.db import ConnectionManager


def test_connection_initialization(conn_conf):
    """Test that Memgraph connection can be initialized."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert db_client is not None
        assert db_client.conn is not None


def test_create_database(conn_conf, test_graph_name):
    """Test that create_database is a no-op (Memgraph is single-database)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # create_database is a no-op for Memgraph
        db_client.create_database(test_graph_name)
        # Connection should still work
        assert db_client.conn is not None


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
