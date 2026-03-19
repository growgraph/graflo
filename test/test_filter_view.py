"""Tests for SelectSpec and TableConnector view (type_lookup)."""

from __future__ import annotations

import os
import tempfile
from unittest.mock import MagicMock

from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.view import SelectSpec
from graflo.architecture.contract.bindings import TableConnector


def _setup_test_db() -> str:
    """Create SQLite DB with generic entity/relation tables."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn_str = f"sqlite:///{path}"
    from sqlalchemy import create_engine, text

    engine = create_engine(conn_str)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE entity_types (
                    entity_id TEXT PRIMARY KEY,
                    type_name TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO entity_types (entity_id, type_name) VALUES
                    ('a1', 'project'),
                    ('a2', 'task'),
                    ('a3', 'project'),
                    ('a4', 'task'),
                    ('a5', 'milestone')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE entity_links (
                    id INTEGER PRIMARY KEY,
                    parent TEXT NOT NULL,
                    child TEXT NOT NULL,
                    link_type TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO entity_links (id, parent, child, link_type) VALUES
                    (1, 'a1', 'a2', 'contains'),
                    (2, 'a3', 'a4', 'contains'),
                    (3, 'a1', 'a5', 'depends_on')
                """
            )
        )
        conn.commit()
    return conn_str


class TestSelectSpecTypeLookup:
    """SelectSpec with kind=type_lookup."""

    def test_from_dict_type_lookup(self):
        """SelectSpec.from_dict parses type_lookup shorthand."""
        spec = SelectSpec.from_dict(
            {
                "kind": "type_lookup",
                "table": "entity_types",
                "identity": "entity_id",
                "type_column": "type_name",
                "source": "parent",
                "target": "child",
                "relation": "link_type",
            }
        )
        assert spec.kind == "type_lookup"
        assert spec.table == "entity_types"
        assert spec.identity == "entity_id"
        assert spec.type_column == "type_name"
        assert spec.source == "parent"
        assert spec.target == "child"
        assert spec.relation == "link_type"

    def test_build_sql_type_lookup_structure(self):
        """type_lookup build_sql produces correct SQL structure."""
        spec = SelectSpec.from_dict(
            {
                "kind": "type_lookup",
                "table": "entity_types",
                "identity": "entity_id",
                "type_column": "type_name",
                "source": "parent",
                "target": "child",
                "relation": "link_type",
            }
        )
        sql = spec.build_sql(schema="main", base_table="entity_links")

        assert "SELECT" in sql
        assert "source_id" in sql
        assert "source_type" in sql
        assert "target_id" in sql
        assert "target_type" in sql
        assert "relation" in sql
        assert "LEFT JOIN" in sql
        assert "entity_types" in sql
        assert "entity_links" in sql
        assert "IS NOT NULL" in sql
        assert "parent" in sql
        assert "child" in sql

    def test_build_sql_type_lookup_executes_on_sqlite(self):
        """type_lookup build_sql produces executable SQL on SQLite."""
        conn_str = _setup_test_db()
        spec = SelectSpec.from_dict(
            {
                "kind": "type_lookup",
                "table": "entity_types",
                "identity": "entity_id",
                "type_column": "type_name",
                "source": "parent",
                "target": "child",
                "relation": "link_type",
            }
        )
        sql = spec.build_sql(schema="main", base_table="entity_links")

        # SQLite ignores schema prefix; strip it for compatibility
        sql_sqlite = sql.replace('"main".', "").replace("main.", "")

        ds = SQLDataSource(
            config=SQLConfig(
                connection_string=conn_str,
                query=sql_sqlite,
                pagination=False,
            )
        )
        rows = list(ds)

        assert len(rows) == 3
        assert "source_id" in rows[0]
        assert "source_type" in rows[0]
        assert "target_id" in rows[0]
        assert "target_type" in rows[0]
        assert "relation" in rows[0]
        assert rows[0]["source_type"] == "project"
        assert rows[0]["target_type"] == "task"
        assert rows[0]["relation"] == "contains"


class TestTableConnectorWithView:
    """TableConnector with view=SelectSpec."""

    def test_table_connector_build_query_uses_view(self):
        """TableConnector.build_query when view is set delegates to view.build_sql."""
        connector = TableConnector(
            table_name="entity_links",
            schema_name="public",
            view={
                "kind": "type_lookup",
                "table": "entity_types",
                "identity": "entity_id",
                "type_column": "type_name",
                "source": "parent",
                "target": "child",
                "relation": "link_type",
            },
        )
        sql = connector.build_query("public")

        assert "source_id" in sql
        assert "source_type" in sql
        assert "target_id" in sql
        assert "target_type" in sql
        assert "entity_links" in sql
        assert "entity_types" in sql

    def test_table_connector_view_executes_on_sqlite(self):
        """TableConnector with view produces executable SQL on SQLite."""
        conn_str = _setup_test_db()
        connector = TableConnector(
            table_name="entity_links",
            schema_name="main",
            view={
                "kind": "type_lookup",
                "table": "entity_types",
                "identity": "entity_id",
                "type_column": "type_name",
                "source": "parent",
                "target": "child",
                "relation": "link_type",
            },
        )
        sql = connector.build_query("main")
        sql_sqlite = sql.replace('"main".', "").replace("main.", "")

        ds = SQLDataSource(
            config=SQLConfig(
                connection_string=conn_str,
                query=sql_sqlite,
                pagination=False,
            )
        )
        rows = list(ds)

        assert len(rows) == 3
        assert rows[0]["source_type"] == "project"
        assert rows[0]["target_type"] == "task"


class TestCreateBindingsTypeLookup:
    """create_bindings_from_postgres with type_lookup_overrides."""

    def test_type_lookup_overrides_sets_view_on_connector(self):
        """type_lookup_overrides sets view on matching edge table connectors."""
        from graflo.db import PostgresConnection
        from graflo.hq.resource_mapper import ResourceMapper

        # Mock introspection to return one edge table
        mock_conn = MagicMock(spec=PostgresConnection)
        mock_result = MagicMock()
        mock_result.schema_name = "public"
        mock_result.vertex_tables = []
        mock_edge = MagicMock()
        mock_edge.name = "entity_links"
        mock_result.edge_tables = [mock_edge]
        mock_conn.introspect_schema.return_value = mock_result
        mock_conn.config = MagicMock()

        mapper = ResourceMapper()
        bindings = mapper.create_bindings_from_postgres(
            conn=mock_conn,
            schema_name="public",
            type_lookup_overrides={
                "entity_links": {
                    "table": "entity_types",
                    "identity": "entity_id",
                    "type_column": "type_name",
                    "source": "parent",
                    "target": "child",
                    "relation": "link_type",
                },
            },
        )

        tp = bindings.table_connectors["entity_links"]
        assert tp.view is not None
        assert tp.view.kind == "type_lookup"
        assert tp.view.table == "entity_types"
        assert tp.view.identity == "entity_id"
