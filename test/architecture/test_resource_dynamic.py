"""Integration tests for resource-level query enhancements with mock SQL.

Tests use SQLite file-based (via SQLAlchemy) to exercise:
- Case 1: Filtered vertex resources (same table, different WHERE)
- Case 2: Edge resource with auto-JOIN (EdgeActor) and dynamic vertex types
- Case 3: Polymorphic relations + objects lookup via ``SelectSpec`` ``type_lookup``
  on ``TableConnector.view`` for :class:`~graflo.architecture.pipeline.runtime.actor.edge_router.EdgeRouterActor`
  (``enrich_edge_connector_with_joins`` does not apply; use declarative view instead)
"""

from __future__ import annotations

import tempfile
import os

from sqlalchemy import create_engine, text

from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.schema import Schema
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.architecture.contract.bindings import TableConnector
from graflo.filter.select import SelectSpec
from graflo.hq.auto_join import enrich_edge_connector_with_joins
from graflo.architecture.contract.bindings import Bindings, ResourceConnectorBinding

_INGESTION_BY_SCHEMA_ID: dict[int, IngestionModel] = {}


def _bound_ingestion_model(schema: Schema) -> IngestionModel:
    ingestion_model = _INGESTION_BY_SCHEMA_ID.get(id(schema))
    assert ingestion_model is not None
    return ingestion_model


def _build_bound_schema(
    *,
    name: str,
    vertex_config: dict,
    edge_config: dict,
    resources: list[dict],
    db_profile: dict | None = None,
) -> Schema:
    schema = Schema.model_validate(
        {
            "metadata": {"name": name, "version": "0.0.1"},
            "core_schema": {
                "vertex_config": vertex_config,
                "edge_config": edge_config,
            },
            "db_profile": db_profile or {},
        }
    )
    ingestion_model = IngestionModel.model_validate({"resources": resources})
    ingestion_model.finish_init(schema.core_schema)
    _INGESTION_BY_SCHEMA_ID[id(schema)] = ingestion_model
    return schema


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


def _setup_polymorphic_objects_relations_db() -> str:
    """SQLite DB: mixed ``relations`` rows + ``objects`` lookup by id (type per row).

    Mirrors a common CMDB pattern: one relation table, one polymorphic object table.
    Used with :class:`SelectSpec` ``kind="type_lookup"`` on ``TableConnector.view``,
    then :class:`~graflo.architecture.pipeline.runtime.actor.edge_router.EdgeRouterActor`
    with ``source_type_field`` / ``target_type_field`` matching generated aliases
    ``source_type`` and ``target_type``.
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn_str = f"sqlite:///{path}"
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE objects (
                    id TEXT PRIMARY KEY,
                    "type" TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO objects (id, "type") VALUES
                    ('id123', 'Car'),
                    ('id456', 'Car'),
                    ('id923', 'Teacher'),
                    ('id226', 'Person')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE relations (
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    relation TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO relations (source_id, target_id, relation) VALUES
                    ('id123', 'id456', 'contains'),
                    ('id923', 'id226', 'subclass'),
                    ('id923', 'id226', 'friends')
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
        tp_server = TableConnector(
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
        tp_db = TableConnector(
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
            )
        )
        ds_db = SQLDataSource(
            config=SQLConfig(
                connection_string=conn_str,
                query=query_db,
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
        tp = TableConnector(table_name="classes", filters=[f])
        where = tp.build_where_clause()
        assert "\"class_name\" = 'server'" in where


# ---------------------------------------------------------------
# Case 2: Edge resource with auto-JOIN and dynamic vertex types
# ---------------------------------------------------------------


class TestEdgeResourceAutoJoin:
    """Edge resource with JOINs and dynamic vertex types through full pipeline."""

    def _build_schema(self) -> Schema:
        return _build_bound_schema(
            name="test",
            vertex_config={
                "vertices": [
                    {
                        "name": "server",
                        "properties": ["id", "class_name", "description"],
                    },
                    {
                        "name": "database",
                        "properties": ["id", "class_name", "description"],
                    },
                    {
                        "name": "network",
                        "properties": ["id", "class_name", "description"],
                    },
                ],
            },
            edge_config={
                "edges": [
                    {"source": "server", "target": "database"},
                    {"source": "server", "target": "network"},
                ],
            },
            resources=[
                {
                    "name": "relations",
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
        )

    def test_auto_join_query_generates_correct_sql(self):
        """Verify the auto-JOIN + build_query produces valid SQL structure."""

        schema = self._build_schema()
        resource = _bound_ingestion_model(schema).fetch_resource("relations")

        tp_edge = TableConnector(
            name="relations_connector", table_name="relations", schema_name="main"
        )
        tp_classes = TableConnector(
            name="classes_connector", table_name="classes", schema_name="main"
        )
        patterns_table = {
            "server": tp_classes,
            "database": tp_classes,
            "network": tp_classes,
            "relations": tp_edge,
        }

        patterns = Bindings(
            connectors=[tp_classes, tp_edge],
            resource_connector=[
                ResourceConnectorBinding(
                    resource=resource_name,
                    connector="relations_connector"
                    if resource_name == "relations"
                    else "classes_connector",
                )
                for resource_name in patterns_table
            ],
        )

        enrich_edge_connector_with_joins(
            resource=resource,
            connector=tp_edge,
            bindings=patterns,
            vertex_config=schema.core_schema.vertex_config,
        )

        q = tp_edge.build_query("main")
        # Structure checks
        assert "LEFT JOIN" in q
        assert "IS NOT NULL" in q
        assert '"main"."relations"' in q
        assert '"main"."classes"' in q

    def test_auto_join_query_executes_on_sqlite(self):
        """Run the generated JOIN query against a real SQLite DB."""
        from graflo.hq.auto_join import enrich_edge_connector_with_joins
        from graflo.architecture.contract.bindings import Bindings

        conn_str = _setup_db()
        schema = self._build_schema()
        resource = _bound_ingestion_model(schema).fetch_resource("relations")

        tp_edge = TableConnector(name="relations_connector", table_name="relations")
        tp_classes = TableConnector(name="classes_connector", table_name="classes")
        patterns_table = {
            "server": tp_classes,
            "database": tp_classes,
            "network": tp_classes,
            "relations": tp_edge,
        }
        bindings = Bindings(
            connectors=[tp_classes, tp_edge],
            resource_connector=[
                ResourceConnectorBinding(
                    resource=resource_name,
                    connector="relations_connector"
                    if resource_name == "relations"
                    else "classes_connector",
                )
                for resource_name in patterns_table
            ],
        )

        enrich_edge_connector_with_joins(
            resource=resource,
            connector=tp_edge,
            bindings=bindings,
            vertex_config=schema.core_schema.vertex_config,
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
        from graflo.architecture.pipeline.runtime.actor import VertexRouterActor

        schema = self._build_schema()
        resource = _bound_ingestion_model(schema).fetch_resource("relations")

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
        from graflo.architecture.pipeline.runtime.actor import VertexRouterActor
        from graflo.architecture.pipeline.runtime.actor.config import (
            VertexRouterActorConfig,
        )

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
        from graflo.architecture.pipeline.runtime.actor import VertexRouterActor
        from graflo.architecture.pipeline.runtime.actor.config import (
            VertexRouterActorConfig,
        )

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
        resource = _bound_ingestion_model(schema).fetch_resource("relations")

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
        from graflo.architecture.pipeline.runtime.actor import VertexRouterActor

        schema = self._build_schema()
        resource = _bound_ingestion_model(schema).fetch_resource("relations")
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

    def test_vertex_router_with_vertex_from_map_maps_doc_fields_to_vertex_fields(
        self,
    ):
        """VertexRouterActor with vertex_from_map projects doc fields to vertex fields."""
        schema = _build_bound_schema(
            name="test",
            vertex_config={
                "vertices": [
                    {
                        "name": "person",
                        "properties": ["id", "name"],
                        "identity": ["id"],
                    },
                    {"name": "org", "properties": ["id", "name"], "identity": ["id"]},
                ],
            },
            edge_config={"edges": []},
            resources=[
                {
                    "name": "mixed",
                    "pipeline": [
                        {
                            "vertex_router": {
                                "type_field": "kind",
                                "type_map": {"Person": "person", "Org": "org"},
                                "vertex_from_map": {
                                    "person": {
                                        "id": "user_id",
                                        "name": "user_name",
                                    },
                                    "org": {"id": "org_id", "name": "org_name"},
                                },
                            }
                        },
                    ],
                },
            ],
        )
        resource = _bound_ingestion_model(schema).fetch_resource("mixed")

        doc = {"kind": "Person", "user_id": "u1", "user_name": "Alice"}
        result = resource(doc)

        assert "person" in result
        person_docs = result["person"]
        assert len(person_docs) >= 1
        assert any(
            d.get("id") == "u1" and d.get("name") == "Alice" for d in person_docs
        )

        doc_org = {"kind": "Org", "org_id": "o1", "org_name": "Acme"}
        result_org = resource(doc_org)
        assert "org" in result_org
        org_docs = result_org["org"]
        assert len(org_docs) >= 1
        assert any(d.get("id") == "o1" and d.get("name") == "Acme" for d in org_docs)

    def test_vertex_router_with_transform_consumes_transform_output(self):
        """TransformActor runs before VertexRouterActor; routed VertexActor consumes buffer_transforms."""
        schema = _build_bound_schema(
            name="test",
            vertex_config={
                "vertices": [
                    {
                        "name": "item",
                        "properties": ["id", "label"],
                        "identity": ["id"],
                    },
                ],
            },
            edge_config={"edges": []},
            resources=[
                {
                    "name": "transform_then_router",
                    "pipeline": [
                        {
                            "transform": {
                                "rename": {"raw_id": "id", "raw_label": "label"}
                            }
                        },
                        {
                            "vertex_router": {
                                "type_field": "kind",
                                "type_map": {"Item": "item"},
                            }
                        },
                    ],
                },
            ],
        )
        resource = _bound_ingestion_model(schema).fetch_resource(
            "transform_then_router"
        )

        doc = {"kind": "Item", "raw_id": "x1", "raw_label": "Transformed"}
        result = resource(doc)

        assert "item" in result
        item_docs = result["item"]
        assert len(item_docs) >= 1
        assert any(
            d.get("id") == "x1" and d.get("label") == "Transformed" for d in item_docs
        )


# ---------------------------------------------------------------
# Case 3: EdgeRouterActor + SelectSpec type_lookup (no auto_join)
# ---------------------------------------------------------------


class TestEdgeRouterWithTypeLookupView:
    """Polymorphic relations + object lookup via ``TableConnector.view`` (``SelectSpec``).

    Use this pattern when the pipeline uses ``EdgeRouterActor`` (not ``EdgeActor``).
    ``enrich_edge_connector_with_joins`` only collects ``EdgeActor`` steps; for
    mixed relation tables, declare ``kind: type_lookup`` (or ``kind: select``)
    on the edge resource's ``TableConnector`` so ``build_query`` emits JOINs and
    columns ``source_type`` / ``target_type`` expected by ``edge_router``.

    **YAML sketch** (edge resource ``relations`` + connector; adjust bindings names)::

        ingestion_model:
          resources:
            - name: relations
              pipeline:
                - edge_router:
                    source_type_field: source_type
                    target_type_field: target_type
                    source_fields: { id: source_id }
                    target_fields: { id: target_id }
                    relation_field: relation
                    type_map: { Car: car, Teacher: teacher, Person: person }

        # On the TableConnector bound to ``relations``:
        connectors:
          - name: relations_polymorphic
            table_name: relations
            schema_name: main   # or your schema
            view:
              kind: type_lookup
              table: objects
              identity: id
              type_column: type
              source: source_id
              target: target_id
              relation: relation

    Asymmetric lookups (different tables or keys per side) use ``source_table`` /
    ``target_table`` / ``source_identity`` / ``target_identity`` / etc. on the
    same ``type_lookup`` block—see ``SelectSpec`` in ``graflo/filter/select.py``.
    """

    @staticmethod
    def _relations_type_lookup_connector() -> TableConnector:
        """Declarative view: same semantics as YAML ``view: { kind: type_lookup, ... }``."""
        view = SelectSpec(
            kind="type_lookup",
            table="objects",
            identity="id",
            type_column="type",
            source="source_id",
            target="target_id",
            relation="relation",
        )
        return TableConnector(
            name="relations_polymorphic",
            table_name="relations",
            schema_name="main",
            view=view,
        )

    @staticmethod
    def _build_schema_edge_router() -> Schema:
        return _build_bound_schema(
            name="polymorphic_relations",
            vertex_config={
                "vertices": [
                    {
                        "name": "car",
                        "properties": ["id"],
                        "identity": ["id"],
                    },
                    {
                        "name": "teacher",
                        "properties": ["id"],
                        "identity": ["id"],
                    },
                    {
                        "name": "person",
                        "properties": ["id"],
                        "identity": ["id"],
                    },
                ],
            },
            edge_config={"edges": []},
            resources=[
                {
                    "name": "relations",
                    "pipeline": [
                        {
                            "edge_router": {
                                "source_type_field": "source_type",
                                "target_type_field": "target_type",
                                "source_fields": {"id": "source_id"},
                                "target_fields": {"id": "target_id"},
                                "relation_field": "relation",
                                "type_map": {
                                    "Car": "car",
                                    "Teacher": "teacher",
                                    "Person": "person",
                                },
                            }
                        },
                    ],
                },
            ],
        )

    def test_type_lookup_build_query_joins_lookup_table_twice(self):
        """``SelectSpec`` ``type_lookup`` expands to JOINs and fixed output aliases."""
        connector = self._relations_type_lookup_connector()
        q = connector.build_query("main")
        assert "LEFT JOIN" in q
        assert '"main"."relations"' in q or '"relations"' in q
        assert '"main"."objects"' in q
        assert "AS source_type" in q
        assert "AS target_type" in q
        assert "AS relation" in q

    def test_type_lookup_query_runs_on_sqlite(self):
        """Generated SQL executes; rows expose fields the edge router reads."""
        conn_str = _setup_polymorphic_objects_relations_db()
        connector = self._relations_type_lookup_connector()
        query = connector.build_query("main")

        ds = SQLDataSource(
            config=SQLConfig(
                connection_string=conn_str,
                query=query,
            )
        )
        rows = list(ds)
        assert len(rows) == 3
        row = rows[0]
        assert set(row.keys()) >= {
            "source_id",
            "source_type",
            "target_id",
            "target_type",
            "relation",
        }

    def test_edge_router_resource_accepts_type_lookup_row_shape(self):
        """Pipeline ``edge_router`` consumes documents shaped like ``type_lookup`` output."""
        schema = self._build_schema_edge_router()
        resource = _bound_ingestion_model(schema).fetch_resource("relations")

        doc = {
            "source_id": "id923",
            "target_id": "id226",
            "relation": "subclass",
            "source_type": "Teacher",
            "target_type": "Person",
        }

        result = resource(doc)

        assert "teacher" in result
        assert "person" in result
        teacher_docs = result["teacher"]
        assert any(d.get("id") == "id923" for d in teacher_docs)
        person_docs = result["person"]
        assert any(d.get("id") == "id226" for d in person_docs)

    def test_pipeline_collects_edge_router_not_edge_actor(self):
        from graflo.architecture.pipeline.runtime.actor import (
            EdgeActor,
            EdgeRouterActor,
        )

        schema = self._build_schema_edge_router()
        resource = _bound_ingestion_model(schema).fetch_resource("relations")
        actors = resource.root.collect_actors()
        assert any(isinstance(a, EdgeRouterActor) for a in actors)
        assert not any(isinstance(a, EdgeActor) for a in actors)
