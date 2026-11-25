"""Grafeo graph creation and deletion, and the shared connection and CRUD cases."""

from graflo.db.manager import ConnectionManager
from test.db.cypher_cases.lifecycle import (
    test_connection_initialization,
    test_fetch_docs_with_filter,
    test_fetch_docs_with_limit,
    test_fetch_docs_with_projection,
    test_insert_edges,
    test_upsert_single_node,
    test_upsert_updates_existing_node,
)
from test.db.cypher_cases.lifecycle import (
    test_delete_graph_structure as test_delete_graph,
)

__all__ = [
    "test_connection_initialization",
    "test_delete_graph",
    "test_fetch_docs_with_filter",
    "test_fetch_docs_with_limit",
    "test_fetch_docs_with_projection",
    "test_insert_edges",
    "test_upsert_single_node",
    "test_upsert_updates_existing_node",
]


def test_a_created_graph_is_listed_and_a_deleted_one_is_gone(
    conn_conf, test_graph_name
):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.create_database(test_graph_name)
        assert test_graph_name in db_client.db.list_graphs()
        db_client.delete_database(test_graph_name)
        assert test_graph_name not in db_client.db.list_graphs()
