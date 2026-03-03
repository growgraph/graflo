"""Integration tests for resource-level query enhancements with mock SQL.

Tests use SQLite file-based (via SQLAlchemy) to exercise:
- Case 1: Filtered vertex resources (same table, different WHERE)
- Case 2: Edge resource with auto-JOIN and dynamic vertex types
"""

from __future__ import annotations

import tempfile
import os

from sqlalchemy import create_engine, text

from graflo.architecture.schema import Schema
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.util.onto import TablePattern
from graflo.hq.auto_join import enrich_edge_pattern_with_joins
from graflo.util.onto import Patterns


# ---------------------------------------------------------------
# Helper: create a fresh SQLite file-based DB with test data
# ---------------------------------------------------------------


def _setup_db() -> str:
    """Create a SQLite file DB with CMDB-like tables and return the connection string.

    Uses a temporary file so that multiple SQLAlchemy engines can share it
    (in-memory SQLite is connection-private).
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn_str = f"sqlite:///{path}"
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE classes (
                    id TEXT PRIMARY KEY,
                    class_name TEXT NOT NULL,
                    description TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO classes (id, class_name, description) VALUES
                    ('1', 'server', 'Web Server'),
                    ('2', 'database', 'PostgreSQL'),
                    ('3', 'server', 'App Server'),
                    ('4', 'database', 'MySQL'),
                    ('5', 'network', 'Router')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE relations (
                    id INTEGER PRIMARY KEY,
                    parent TEXT NOT NULL,
                    child TEXT NOT NULL,
                    type_display TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO relations (id, parent, child, type_display) VALUES
                    (1, '1', '2', 'runs_on'),
                    (2, '3', '4', 'runs_on'),
                    (3, '1', '5', 'connects_to')
                """
            )
        )
        conn.commit()
    return conn_str


# ---------------------------------------------------------------
# Case 1: Filtered vertex resources
# ---------------------------------------------------------------


class TestFilteredVertexResources:
    """Two Resources read the same table with different filter predicates."""

    def test_filtered_queries_produce_correct_subsets(self):
        """Each Resource's generated query returns only its filtered rows."""
        conn_str = _setup_db()

        # Resource "server" -> classes WHERE class_name = 'server'
        f_server = FilterExpression(
            kind="leaf",
            field="class_name",
            cmp_operator=ComparisonOperator.EQ,
            value=["server"],
        )
        tp_server = TablePattern(
            table_name="classes",
            filters=[f_server],
        )

        # Resource "database" -> classes WHERE class_name = 'database'
        f_db = FilterExpression(
            kind="leaf",
            field="class_name",
            cmp_operator=ComparisonOperator.EQ,
            value=["database"],
        )
        tp_db = TablePattern(
            table_name="classes",
            filters=[f_db],
        )

        # SQLite doesn't use schemas, so we use "main" as effective_schema
        # but build_query quotes it -- SQLite ignores schema prefix on tables
        # so we pass None and rely on the default "public" which SQLite also ignores.
        # We'll just use the raw query from build_where_clause instead.
        query_server = "SELECT * FROM classes"
        where_server = tp_server.build_where_clause()
        if where_server:
            query_server += f" WHERE {where_server}"

        query_db = "SELECT * FROM classes"
        where_db = tp_db.build_where_clause()
        if where_db:
            query_db += f" WHERE {where_db}"

        # Execute the queries
        ds_server = SQLDataSource(
            config=SQLConfig(
                connection_string=conn_str,
                query=query_server,
                pagination=False,
            )
        )
        ds_db = SQLDataSource(
            config=SQLConfig(
                connection_string=conn_str,
                query=query_db,
                pagination=False,
            )
        )

        server_rows = list(ds_server)
        db_rows = list(ds_db)

        # server has 2 rows (id 1, 3)
        assert len(server_rows) == 2
        assert all(r["class_name"] == "server" for r in server_rows)

        # database has 2 rows (id 2, 4)
        assert len(db_rows) == 2
        assert all(r["class_name"] == "database" for r in db_rows)

    def test_build_query_filter_sql_renders_correctly(self):
        """Verify build_where_clause() renders FilterExpression filters."""
        f = FilterExpression(
            kind="leaf",
            field="class_name",
            cmp_operator=ComparisonOperator.EQ,
            value=["server"],
        )
        tp = TablePattern(table_name="classes", filters=[f])
        where = tp.build_where_clause()
        assert "\"class_name\" = 'server'" in where


# ---------------------------------------------------------------
# Case 2: Edge resource with auto-JOIN and dynamic vertex types
# ---------------------------------------------------------------


class TestEdgeResourceAutoJoin:
    """Edge resource with JOINs and dynamic vertex types through full pipeline."""

    def _build_schema(self) -> Schema:
        return Schema.model_validate(
            {
                "general": {"name": "test", "version": "0.0.1"},
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "server",
                            "fields": ["id", "class_name", "description"],
                        },
                        {
                            "name": "database",
                            "fields": ["id", "class_name", "description"],
                        },
                        {
                            "name": "network",
                            "fields": ["id", "class_name", "description"],
                        },
                    ],
                },
                "edge_config": {
                    "edges": [
                        {"source": "server", "target": "database"},
                        {"source": "server", "target": "network"},
                    ],
                },
                "resources": [
                    {
                        "resource_name": "relations",
                        "pipeline": [
                            {
                                "vertex_router": {
                                    "type_field": "s__class_name",
                                    "prefix": "s__",
                                }
                            },
                            {
                                "vertex_router": {
                                    "type_field": "t__class_name",
                                    "prefix": "t__",
                                }
                            },
                            {
                                "edge": {
                                    "from": "server",
                                    "to": "database",
                                    "match_source": "parent",
                                    "match_target": "child",
                                    "relation_field": "type_display",
                                }
                            },
                        ],
                    },
                ],
            }
        )

    def test_auto_join_query_generates_correct_sql(self):
        """Verify the auto-JOIN + build_query produces valid SQL structure."""

        schema = self._build_schema()
        resource = schema.fetch_resource("relations")

        tp_edge = TablePattern(table_name="relations", schema_name="main")
        patterns_table = {
            "server": TablePattern(table_name="classes", schema_name="main"),
            "database": TablePattern(table_name="classes", schema_name="main"),
            "network": TablePattern(table_name="classes", schema_name="main"),
            "relations": tp_edge,
        }

        patterns = Patterns(table_patterns=patterns_table)

        enrich_edge_pattern_with_joins(
            resource=resource,
            pattern=tp_edge,
            patterns=patterns,
            vertex_config=schema.vertex_config,
        )

        q = tp_edge.build_query("main")
        # Structure checks
        assert "LEFT JOIN" in q
        assert "IS NOT NULL" in q
        assert '"main"."relations"' in q
        assert '"main"."classes"' in q

    def test_auto_join_query_executes_on_sqlite(self):
        """Run the generated JOIN query against a real SQLite DB."""
        from graflo.hq.auto_join import enrich_edge_pattern_with_joins
        from graflo.util.onto import Patterns

        conn_str = _setup_db()
        schema = self._build_schema()
        resource = schema.fetch_resource("relations")

        tp_edge = TablePattern(table_name="relations")
        patterns_table = {
            "server": TablePattern(table_name="classes"),
            "database": TablePattern(table_name="classes"),
            "network": TablePattern(table_name="classes"),
            "relations": tp_edge,
        }
        patterns = Patterns(table_patterns=patterns_table)

        enrich_edge_pattern_with_joins(
            resource=resource,
            pattern=tp_edge,
            patterns=patterns,
            vertex_config=schema.vertex_config,
        )

        # SQLite doesn't use schema prefixes, so build query manually
        # mimicking what build_query does but without schema quoting
        base = "relations r"
        join_parts = []
        for jc in tp_edge.joins:
            alias = jc.alias
            join_parts.append(
                f"{jc.join_type} JOIN classes {alias} ON r.{jc.on_self} = {alias}.{jc.on_other}"
            )

        # SELECT with aliased columns to simulate the prefix convention
        select_cols = [
            "r.*",
            's.id AS "s__id"',
            's.class_name AS "s__class_name"',
            's.description AS "s__description"',
            't.id AS "t__id"',
            't.class_name AS "t__class_name"',
            't.description AS "t__description"',
        ]
        query = f"SELECT {', '.join(select_cols)} FROM {base} {' '.join(join_parts)}"
        query += " WHERE s.id IS NOT NULL AND t.id IS NOT NULL"

        ds = SQLDataSource(
            config=SQLConfig(
                connection_string=conn_str,
                query=query,
                pagination=False,
            )
        )
        rows = list(ds)

        # We inserted 3 relations; all should have valid source/target
        assert len(rows) == 3
        # Each row should have the aliased columns
        assert "s__class_name" in rows[0]
        assert "t__class_name" in rows[0]

    def test_pipeline_contains_vertex_router_actors(self):
        """Pipeline with vertex_router steps produces VertexRouterActor instances."""
        from graflo.architecture.actor import VertexRouterActor

        schema = self._build_schema()
        resource = schema.fetch_resource("relations")

        all_actors = resource.root.collect_actors()
        router_actors = [a for a in all_actors if isinstance(a, VertexRouterActor)]

        # Should have 2 routers (source and target)
        assert len(router_actors) == 2
        type_fields = {a.type_field for a in router_actors}
        assert type_fields == {"s__class_name", "t__class_name"}

        # Routers lazily create wrappers on first use.
        for ra in router_actors:
            assert set(ra._vertex_actors.keys()) == set()

    def test_vertex_router_extract_sub_doc_strips_prefix(self):
        """VertexRouterActor._extract_sub_doc strips prefix from field keys."""
        from graflo.architecture.actor import VertexRouterActor
        from graflo.architecture.actor_config import VertexRouterActorConfig

        config = VertexRouterActorConfig(type_field="s__class_name", prefix="s__")
        router = VertexRouterActor(config)

        doc = {
            "parent": "1",
            "child": "2",
            "type_display": "runs_on",
            "s__id": "1",
            "s__class_name": "server",
            "s__description": "Web Server",
            "t__id": "2",
            "t__class_name": "database",
            "t__description": "PostgreSQL",
        }

        sub_doc = router._extract_sub_doc(doc)

        # Only s__-prefixed keys extracted, with prefix stripped
        assert sub_doc == {
            "id": "1",
            "class_name": "server",
            "description": "Web Server",
        }

    def test_vertex_router_extract_sub_doc_with_field_map(self):
        """VertexRouterActor._extract_sub_doc applies field_map when set."""
        from graflo.architecture.actor import VertexRouterActor
        from graflo.architecture.actor_config import VertexRouterActorConfig

        config = VertexRouterActorConfig(
            type_field="src_type",
            field_map={"src_id": "id", "src_name": "class_name"},
        )
        router = VertexRouterActor(config)

        doc = {
            "src_type": "server",
            "src_id": "1",
            "src_name": "server",
            "extra": "ignored",
        }
        sub_doc = router._extract_sub_doc(doc)

        assert sub_doc == {"id": "1", "class_name": "server"}

    def test_full_resource_call_produces_vertices_and_edges(self):
        """Resource.__call__ with dynamic types creates vertices and edges."""
        schema = self._build_schema()
        resource = schema.fetch_resource("relations")

        doc = {
            "parent": "1",
            "child": "2",
            "type_display": "runs_on",
            "s__id": "1",
            "s__class_name": "server",
            "s__description": "Web Server",
            "t__id": "2",
            "t__class_name": "database",
            "t__description": "PostgreSQL",
        }

        result = resource(doc)

        # Should have vertices
        vertex_keys = [k for k in result if isinstance(k, str)]
        assert "server" in vertex_keys
        assert "database" in vertex_keys

        # server should have the routed vertex doc
        server_docs = result["server"]
        assert len(server_docs) >= 1
        assert any(d.get("id") == "1" for d in server_docs)

        db_docs = result["database"]
        assert len(db_docs) >= 1
        assert any(d.get("id") == "2" for d in db_docs)

    def test_vertex_router_registers_wrappers_lazily(self):
        """VertexRouterActor creates only wrappers used by routed documents."""
        from graflo.architecture.actor import VertexRouterActor

        schema = self._build_schema()
        resource = schema.fetch_resource("relations")
        router_actors = [
            a
            for a in resource.root.collect_actors()
            if isinstance(a, VertexRouterActor)
        ]
        assert len(router_actors) == 2
        assert all(not a._vertex_actors for a in router_actors)

        resource(
            {
                "parent": "1",
                "child": "2",
                "type_display": "runs_on",
                "s__id": "1",
                "s__class_name": "server",
                "s__description": "Web Server",
                "t__id": "2",
                "t__class_name": "database",
                "t__description": "PostgreSQL",
            }
        )
        assert set(router_actors[0]._vertex_actors.keys()) == {"server"}
        assert set(router_actors[1]._vertex_actors.keys()) == {"database"}
