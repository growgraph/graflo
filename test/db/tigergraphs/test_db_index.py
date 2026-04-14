import pytest
import uuid

from graflo.db import ConnectionManager


def test_create_vertex_index(conn_conf, schema_obj, test_graph_name):
    """Test creating vertex indexes using GSQL CREATE INDEX."""
    schema_obj = schema_obj("review")
    schema_obj.metadata.name = test_graph_name

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Initialize database with schema
        db_client.init_db(schema_obj, recreate_schema=True)

        # Define vertex indexes (indexes should be created in init_db, but call explicitly)
        db_client.define_vertex_indexes(
            schema_obj.core_schema.vertex_config, schema=schema_obj
        )

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

        job_name = f"add_ResearchField_id_index_{uuid.uuid4().hex[:8]}"
        create_job_cmd = (
            "USE GLOBAL\n"
            f"CREATE GLOBAL SCHEMA_CHANGE job {job_name} "
            "{ALTER VERTEX ResearchField ADD INDEX ResearchField_id_index ON (id);}"
        )
        run_job_cmd = f"RUN GLOBAL SCHEMA_CHANGE job {job_name}"
        drop_job_cmd = f"DROP JOB {job_name}"

        # Cleanup stale job name if supported by server.
        try:
            db_client._execute_gsql(drop_job_cmd)
        except Exception as drop_error:
            if "not exist" not in str(drop_error).lower():
                raise

        try:
            # Create the job
            db_client._execute_gsql(create_job_cmd)

            # Run the job
            db_client._execute_gsql(run_job_cmd)

        except Exception as e:
            # Clean up on failure
            try:
                db_client._execute_gsql(drop_job_cmd)
            except Exception as drop_error:
                if "not exist" not in str(drop_error).lower():
                    raise
            pytest.fail(f"Failed to create or run schema change job: {e}")
        finally:
            try:
                db_client._execute_gsql(drop_job_cmd)
            except Exception as drop_error:
                if "not exist" not in str(drop_error).lower():
                    raise
