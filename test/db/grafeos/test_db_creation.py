"""Tests for Grafeo database creation, deletion, and basic CRUD operations.

Adapted from test/db/falkordbs/test_db_creation.py.
"""

from graflo.db import ConnectionManager


def test_connection_initialization(conn_conf):
    """Test that Grafeo connection can be initialized."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert db_client is not None
        assert db_client.db is not None


def test_delete_graph_structure(conn_conf, test_graph_name):
    """Test that graph data can be deleted."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.execute("CREATE (n:TestNode {name: 'test'})")

        result = db_client.execute("MATCH (n:TestNode) RETURN count(n) AS count")
        assert result.to_list()[0]["count"] == 1

        db_client.delete_graph_structure(delete_all=True)

        result = db_client.execute("MATCH (n) RETURN count(n) AS count")
        assert result.to_list()[0]["count"] == 0


def test_upsert_single_node(conn_conf, test_graph_name, clean_db):
    """Test upserting a single node."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"id": "1", "name": "Test User", "email": "test@example.com"}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        result = db_client.fetch_docs("User")
        assert len(result) == 1
        assert result[0]["name"] == "Test User"


def test_upsert_updates_existing_node(conn_conf, test_graph_name, clean_db):
    """Test that upsert updates existing nodes."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"id": "1", "name": "Original Name"}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        docs = [{"id": "1", "name": "Updated Name"}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        result = db_client.fetch_docs("User")
        assert len(result) == 1
        assert result[0]["name"] == "Updated Name"


def test_upsert_batch(conn_conf, test_graph_name, clean_db):
    """Test upserting multiple nodes in a batch."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"id": str(i), "name": f"User {i}"} for i in range(10)]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        result = db_client.fetch_docs("User")
        assert len(result) == 10


def test_insert_edges(conn_conf, test_graph_name, clean_db):
    """Test inserting edges between nodes."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.upsert_docs_batch(
            [{"id": "1", "name": "User 1"}], "User", match_keys=["id"]
        )
        db_client.upsert_docs_batch(
            [{"id": "2", "name": "User 2"}], "User", match_keys=["id"]
        )

        edge_docs = [[{"id": "1"}, {"id": "2"}, {"since": "2024-01-01"}]]
        db_client.insert_edges_batch(
            edge_docs,
            source_class="User",
            target_class="User",
            relation_name="FOLLOWS",
            match_keys_source=("id",),
            match_keys_target=("id",),
        )

        result = db_client.execute(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN count(r) AS count"
        )
        assert result.to_list()[0]["count"] == 1


def test_fetch_docs_with_filter(conn_conf, test_graph_name, clean_db):
    """Test fetching documents with filters."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [
            {"id": "1", "name": "Alix", "age": 25},
            {"id": "2", "name": "Gus", "age": 30},
            {"id": "3", "name": "Jules", "age": 25},
        ]
        db_client.upsert_docs_batch(docs, "Person", match_keys=["id"])

        result = db_client.fetch_docs("Person", filters=["==", 25, "age"])
        assert len(result) == 2


def test_fetch_docs_with_limit(conn_conf, test_graph_name, clean_db):
    """Test fetching documents with limit."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"id": str(i), "name": f"User {i}"} for i in range(10)]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        result = db_client.fetch_docs("User", limit=5)
        assert len(result) == 5


def test_fetch_docs_with_projection(conn_conf, test_graph_name, clean_db):
    """Test fetching documents with key projection."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"id": "1", "name": "Test", "email": "test@test.com", "age": 30}]
        db_client.upsert_docs_batch(docs, "User", match_keys=["id"])

        result = db_client.fetch_docs("User", return_keys=["name", "email"])
        assert len(result) == 1
        assert "name" in result[0]
        assert "email" in result[0]
        assert "age" not in result[0]
