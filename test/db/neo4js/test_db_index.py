from graflo.db import ConnectionManager


def test_create_vertex_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indexes(
            schema_obj.core_schema.vertex_config, schema=schema_obj
        )
    with ConnectionManager(connection_config=conn_conf) as db_client:
        q = "SHOW INDEX;"
        cursor = db_client.execute(q)
        data = cursor.data()
    # Storage names from review schema: author->Author, researchField->ResearchField
    assert any([item["name"] == "ResearchField_id" for item in data]) and any(
        [item["name"] == "Author_id_full_name" for item in data]
    )


def test_create_edge_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indexes(
            list(schema_obj.core_schema.edge_config.values()),
            schema=schema_obj,
        )
    with ConnectionManager(connection_config=conn_conf) as db_client:
        idx_cursor = db_client.execute("SHOW INDEXES;")
        idx_names = {row.get("name") for row in idx_cursor.data()}
        con_cursor = db_client.execute("SHOW CONSTRAINTS;")
        con_names = {row.get("name") for row in con_cursor.data()}
    assert "belongsTo_t_obs" in idx_names or "belongsTo_t_obs" in con_names
