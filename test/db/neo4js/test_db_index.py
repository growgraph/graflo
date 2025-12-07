from graflo.db import ConnectionManager


def test_create_vertex_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indices(schema_obj.vertex_config)
    with ConnectionManager(connection_config=conn_conf) as db_client:
        q = "SHOW INDEX;"
        cursor = db_client.execute(q)
        data = cursor.data()
    assert any([item["name"] == "researchField_id" for item in data]) & any(
        [item["name"] == "author_id_full_name" for item in data]
    )


def test_create_edge_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indices(
            schema_obj.edge_config.edges_list(include_aux=True)
        )
    with ConnectionManager(connection_config=conn_conf) as db_client:
        q = "SHOW INDEX;"
        cursor = db_client.execute(q)
        data = cursor.data()
    print([item["name"] for item in data])
    assert any([item["name"] == "belongsTo_t_obs" for item in data])
