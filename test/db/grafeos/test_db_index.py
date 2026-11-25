"""Grafeo index creation: per-graph vertex indexes; no edge indexes."""

from graflo.db.manager import ConnectionManager
from test.db.cypher_cases.lifecycle import test_create_vertex_index

__all__ = ["test_create_vertex_index"]


def test_each_vertex_identity_is_indexed(conn_conf, test_graph_name, schema_obj):
    schema_o = schema_obj("review")
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indexes(schema_o.core_schema.vertex_config, schema_o)
        indexed = {
            (row["label"], row["property"])
            for row in db_client.execute("SHOW INDEXES").rows
        }
    assert {("Author", "id"), ("Author", "full_name"), ("ResearchField", "id")} <= (
        indexed
    )


def test_edge_indexes_build_nothing(conn_conf, test_graph_name, schema_obj):
    schema_o = schema_obj("review")
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indexes(
            list(schema_o.core_schema.edge_config.values()), schema=schema_o
        )
        assert db_client.execute("SHOW INDEXES").rows == []
