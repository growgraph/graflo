"""Tests for SelectSpec / TableConnector views and EdgeRouterActor row contract."""

from __future__ import annotations

import os
import tempfile
from collections import Counter
from unittest.mock import MagicMock

from graflo.architecture.contract.bindings import TableConnector
from graflo.architecture.graph_types import ExtractionContext, LocationIndex
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.config import EdgeRouterActorConfig
from graflo.architecture.pipeline.runtime.actor.edge_router import EdgeRouterActor
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Field as VertexField
from graflo.architecture.schema.vertex import Vertex, VertexConfig
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.filter.select import SelectSpec
from graflo.hq.resource_mapper import ResourceMapper


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


def _sql_for_sqlite(sql: str) -> str:
    """Strip schema prefix for SQLite (mirrors resource loaders)."""
    return sql.replace('"main".', "").replace("main.", "")


def _fetch_rows(conn_str: str, sql: str) -> list[dict[str, object]]:
    ds = SQLDataSource(
        config=SQLConfig(
            connection_string=conn_str,
            query=sql,
            pagination=False,
        )
    )
    return list(ds)


def _vertex_config_fixture() -> VertexConfig:
    id_field = VertexField(name="id")
    return VertexConfig(
        vertices=[
            Vertex(name="project", fields=[id_field], identity=["id"]),
            Vertex(name="task", fields=[id_field], identity=["id"]),
            Vertex(name="milestone", fields=[id_field], identity=["id"]),
        ]
    )


def _symmetric_edge_router() -> EdgeRouterActor:
    cfg = EdgeRouterActorConfig(
        source_type_field="source_type",
        target_type_field="target_type",
        source_fields={"id": "source_id"},
        target_fields={"id": "target_id"},
        relation_field="relation",
    )
    router = EdgeRouterActor.from_config(cfg)
    router.finish_init(
        ActorInitContext(
            vertex_config=_vertex_config_fixture(),
            edge_config=EdgeConfig(),
            transforms={},
        )
    )
    return router


def _run_router_on_rows(
    router: EdgeRouterActor, rows: list[dict[str, object]]
) -> list[tuple[str, str, str | None]]:
    ctx = ExtractionContext()
    base = LocationIndex()
    for i, row in enumerate(rows):
        router(ctx, base.extend((i,)), doc=dict(row))
    return [(edge.source, edge.target, edge.relation) for edge, _ in ctx.edge_requests]


_TYPE_LOOKUP_VIEW: dict[str, object] = {
    "kind": "type_lookup",
    "table": "entity_types",
    "identity": "entity_id",
    "type_column": "type_name",
    "source": "parent",
    "target": "child",
    "relation": "link_type",
}


class TestSelectSpecFromDict:
    """Parsing SelectSpec from declarative dicts."""

    def test_type_lookup_from_dict(self):
        """type_lookup fields round-trip via from_dict."""
        spec = SelectSpec.from_dict(_TYPE_LOOKUP_VIEW)
        assert spec.kind == "type_lookup"
        assert spec.table == "entity_types"
        assert spec.identity == "entity_id"
        assert spec.type_column == "type_name"
        assert spec.source == "parent"
        assert spec.target == "child"
        assert spec.relation == "link_type"


class TestTypeLookupSqlFeedsEdgeRouter:
    """type_lookup SQL rows match EdgeRouterActor field mapping (symmetric types)."""

    def test_type_lookup_rows_produce_expected_edges(self):
        """Rows from type_lookup view are consumed directly by edge_router config."""
        conn_str = _setup_test_db()
        spec = SelectSpec.from_dict(_TYPE_LOOKUP_VIEW)
        sql = _sql_for_sqlite(spec.build_sql(schema="main", base_table="entity_links"))
        rows = _fetch_rows(conn_str, sql)

        assert len(rows) == 3
        first = rows[0]
        assert first["source_type"] == "project"
        assert first["target_type"] == "task"
        assert first["relation"] == "contains"

        triples = _run_router_on_rows(_symmetric_edge_router(), rows)
        assert Counter(triples) == Counter(
            [
                ("project", "task", "contains"),
                ("project", "task", "contains"),
                ("project", "milestone", "depends_on"),
            ]
        )


class TestTableConnectorViewFeedsEdgeRouter:
    """TableConnector.view build_query produces the same edge contract."""

    def test_table_connector_query_rows_match_edge_router(self):
        connector = TableConnector(
            table_name="entity_links",
            schema_name="main",
            view=_TYPE_LOOKUP_VIEW,
        )
        conn_str = _setup_test_db()
        sql = _sql_for_sqlite(connector.build_query("main"))
        rows = _fetch_rows(conn_str, sql)

        triples = _run_router_on_rows(_symmetric_edge_router(), rows)
        assert Counter(triples) == Counter(
            [
                ("project", "task", "contains"),
                ("project", "task", "contains"),
                ("project", "milestone", "depends_on"),
            ]
        )


class TestSelectKindSelectAsymmetricLookup:
    """kind=select with a single type join: static source vertex, dynamic target type."""

    def test_single_join_select_static_source_dynamic_target(self):
        """Only target side uses entity_types join; source type is fixed in SQL + config."""
        conn_str = _setup_test_db()
        spec = SelectSpec(
            kind="select",
            from_="entity_links",
            joins=[
                {
                    "table": "entity_types",
                    "alias": "t",
                    "on_self": "child",
                    "on_other": "entity_id",
                    "join_type": "LEFT",
                }
            ],
            select=[
                'r."parent" AS source_id',
                "'project' AS source_type",
                'r."child" AS target_id',
                't."type_name" AS target_type',
                'r."link_type" AS relation',
            ],
            where=FilterExpression(
                kind="leaf",
                field="t.entity_id",
                cmp_operator=ComparisonOperator.IS_NOT_NULL,
                value=[],
            ),
        )
        sql = _sql_for_sqlite(spec.build_sql(schema="main", base_table="entity_links"))
        rows = _fetch_rows(conn_str, sql)

        assert len(rows) == 3
        assert rows[0]["target_type"] == "task"

        cfg = EdgeRouterActorConfig(
            source="project",
            target_type_field="target_type",
            source_fields={"id": "source_id"},
            target_fields={"id": "target_id"},
            relation_field="relation",
        )
        router = EdgeRouterActor.from_config(cfg)
        router.finish_init(
            ActorInitContext(
                vertex_config=_vertex_config_fixture(),
                edge_config=EdgeConfig(),
                transforms={},
            )
        )
        triples = _run_router_on_rows(router, rows)
        assert Counter(triples) == Counter(
            [
                ("project", "task", "contains"),
                ("project", "task", "contains"),
                ("project", "milestone", "depends_on"),
            ]
        )


class TestResourceMapperTypeLookupOverride:
    """type_lookup_overrides attach a view to introspected edge tables."""

    def test_type_lookup_overrides_sets_view_on_connector(self):
        from graflo.db import PostgresConnection

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
