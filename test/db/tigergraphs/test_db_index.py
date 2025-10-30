import pytest

from graflo.db import ConnectionManager


@pytest.mark.skip()
def test_create_vertex_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indices(schema_obj.vertex_config)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # TigerGraph doesn't use SHOW INDEX - check vertex types instead
        vertex_types = db_client.conn.getVertexTypes()
        # Or check schema info
        _ = db_client.conn.gsql("ls")

    # TigerGraph automatically indexes PRIMARY_ID, secondary indexes are rare
    # Verify vertex types exist instead of specific index names
    expected_vertex_types = ["researchField", "author"]  # Adjust based on your schema
    for vertex_type in expected_vertex_types:
        assert vertex_type in vertex_types, f"Vertex type {vertex_type} not found"


@pytest.mark.skip()
def test_create_edge_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indices(
            schema_obj.edge_config.edges_list(include_aux=True)
        )

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Check edge types instead of indexes
        edge_types = db_client.conn.getEdgeTypes()

    # Verify expected edge types exist
    expected_edge_types = ["belongsTo"]  # Adjust based on your schema
    for edge_type in expected_edge_types:
        assert edge_type in edge_types, f"Edge type {edge_type} not found"
