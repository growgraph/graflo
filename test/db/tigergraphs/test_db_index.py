import pytest

from graflo.db import ConnectionManager


def test_create_vertex_index(conn_conf, schema_obj, test_graph_name):
    """Test creating vertex indexes using GSQL CREATE INDEX."""
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Initialize database with schema
        db_client.init_db(schema_obj, clean_start=True)

        # Define vertex indexes (indexes should be created in init_db, but call explicitly)
        db_client.define_vertex_indices(schema_obj.vertex_config)

    # Verify indexes were created by attempting to create them again
    # If they already exist, we'll get an "already exists" error which confirms creation
    # Note: TigerGraph only supports indexes on a single field, so multi-field indexes are skipped
    # Expected indexes:
    # - Author: skipped (multi-field index on id, full_name - not supported)
    # - ResearchField: "ResearchField_id_index" on (id) - single field, will be created
    # Note: We use dbnames (Author, ResearchField) not vertex names (author, researchField)
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify vertex types exist (using dbnames)
        vertex_types = db_client.conn.getVertexTypes(force=True)
        assert "Author" in vertex_types, "Vertex type 'Author' not found"
        assert "ResearchField" in vertex_types, "Vertex type 'ResearchField' not found"

        try:
            # Try creating ResearchField index job again (with graph context)
            # This is a single-field index, so it should have been created
            # Using dbname "ResearchField" instead of vertex name "researchField"
            create_research_job = (
                "USE GLOBAL\n"
                "CREATE GLOBAL SCHEMA_CHANGE job add_ResearchField_id_index "
                "{ALTER VERTEX ResearchField ADD INDEX ResearchField_id_index ON (id);}"
            )
            result = db_client.conn.gsql(create_research_job)
            result_str = str(result).lower()
            assert (
                "already exists" in result_str
                or "duplicate" in result_str
                or "used by another object" in result_str
            ), f"ResearchField index job should already exist, got: {result}"

        except Exception as e:
            # If we get an exception, check if it's an "already exists" error
            error_str = str(e).lower()
            if "already exists" in error_str or "duplicate" in error_str:
                # This is actually good - it means the index exists
                pass
            else:
                pytest.fail(f"Failed to verify vertex indexes: {e}")


def test_create_edge_index(conn_conf, schema_obj, test_graph_name):
    """Test creating edge indexes using GSQL CREATE INDEX."""
    schema_obj = schema_obj("review")
    schema_obj.general.name = test_graph_name

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Initialize database with schema
        db_client.init_db(schema_obj, clean_start=True)

        # Define edge indexes
        db_client.define_edge_indices(
            schema_obj.edge_config.edges_list(include_aux=True)
        )

    # Verify indexes were created by attempting to create them again
    # Expected index: belongsTo with field t_obs -> "belongsTo_t_obs_index"
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify edge types exist
        edge_types = db_client.conn.getEdgeTypes(force=True)
        expected_edge_types = ["belongsTo"]
        for edge_type in expected_edge_types:
            assert edge_type in edge_types, f"Edge type {edge_type} not found"

        # Try to create the index again using schema change job - if it exists, we should get "already exists"
        try:
            # Try creating belongsTo index job again (with graph context)
            create_edge_job = (
                "USE GLOBAL\n"
                "CREATE GLOBAL SCHEMA_CHANGE job add_belongsTo_t_obs_index "
                "{ALTER EDGE belongsTo ADD INDEX belongsTo_t_obs_index ON (t_obs);}"
            )
            result = db_client.conn.gsql(create_edge_job)
            result_str = str(result).lower()
            assert (
                "already exists" in result_str
                or "duplicate" in result_str
                or "used by another object" in result_str
            ), f"belongsTo index job should already exist, got: {result}"

        except Exception as e:
            # If we get an exception, check if it's an "already exists" error
            error_str = str(e).lower()
            if "already exists" in error_str or "duplicate" in error_str:
                # This is actually good - it means the index exists
                pass
            else:
                pytest.fail(f"Failed to verify edge indexes: {e}")
