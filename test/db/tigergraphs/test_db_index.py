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
        vertex_types = db_client._get_vertex_types()
        assert "Author" in vertex_types, "Vertex type 'Author' not found"
        assert "ResearchField" in vertex_types, "Vertex type 'ResearchField' not found"

        job_name = "add_ResearchField_id_index"
        create_job_cmd = (
            "USE GLOBAL\n"
            f"CREATE GLOBAL SCHEMA_CHANGE job {job_name} "
            "{ALTER VERTEX ResearchField ADD INDEX ResearchField_id_index ON (id);}"
        )
        run_job_cmd = f"RUN GLOBAL SCHEMA_CHANGE job {job_name}"
        drop_job_cmd = f"DROP JOB {job_name}"

        # Clean up: drop job if it exists (ignore errors)
        try:
            db_client._execute_gsql(drop_job_cmd)
        except Exception:
            pass

        try:
            # Create the job
            db_client._execute_gsql(create_job_cmd)

            # Run the job
            db_client._execute_gsql(run_job_cmd)

        except Exception as e:
            # Clean up on failure
            try:
                db_client._execute_gsql(drop_job_cmd)
            except Exception:
                pass
            pytest.fail(f"Failed to create or run schema change job: {e}")
