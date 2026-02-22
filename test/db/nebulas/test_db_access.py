"""Tests for NebulaGraph connection, space management, and schema initialisation."""

import pytest

from graflo.db import ConnectionManager

pytestmark = pytest.mark.nebula


def test_connection_initialization(conn_conf):
    """Test that NebulaGraph connection can be initialised."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert db_client is not None


def test_create_space(conn_conf, test_space_name):
    """Test that a space can be created and deleted."""
    conn_conf.schema_name = test_space_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.create_database(test_space_name)
        assert db_client._space_name == test_space_name
        db_client.delete_database(test_space_name)


def test_init_db(conn_conf, test_space_name, schema_obj):
    """Test init_db creates tags and edge types."""
    conn_conf.schema_name = test_space_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(schema_obj, recreate_schema=True)

        rs = db_client.execute("SHOW TAGS")
        tag_names = {r.get("Name", r.get("name", "")) for r in rs.rows_as_dicts()}
        assert "Person" in tag_names
        assert "City" in tag_names

        rs = db_client.execute("SHOW EDGES")
        edge_names = {r.get("Name", r.get("name", "")) for r in rs.rows_as_dicts()}
        assert "lives_in" in edge_names
        assert "knows" in edge_names

        db_client.delete_database(test_space_name)


def test_clear_data(nebula_db):
    """Test that clear_data removes vertices while keeping the schema."""
    from graflo.architecture.schema import Schema
    from test.db.nebulas.conftest import MINI_SCHEMA_DICT

    schema = Schema.from_dict(MINI_SCHEMA_DICT)

    nebula_db.upsert_docs_batch(
        [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
        "Person",
        match_keys=["name"],
    )
    assert len(nebula_db.fetch_docs("Person")) == 2

    nebula_db.clear_data(schema)

    rs = nebula_db.execute("SHOW TAGS")
    tag_names = {r.get("Name", r.get("name", "")) for r in rs.rows_as_dicts()}
    assert "Person" in tag_names


def test_delete_graph_structure(conn_conf, test_space_name, schema_obj):
    """Test that delete_graph_structure with delete_all drops the space."""
    conn_conf.schema_name = test_space_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(schema_obj, recreate_schema=True)

        db_client.delete_graph_structure([], [], delete_all=True)

        spaces = db_client.execute("SHOW SPACES")
        space_names = {r.get("Name", r.get("name", "")) for r in spaces.rows_as_dicts()}
        assert test_space_name not in space_names
