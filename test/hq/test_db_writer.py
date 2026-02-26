from __future__ import annotations

import asyncio

from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.onto import GraphContainer
from graflo.architecture.schema import Schema, SchemaMetadata
from graflo.architecture.vertex import Vertex, VertexConfig, Field
from graflo.db.connection import ArangoConfig
from graflo.hq.db_writer import DBWriter
from graflo.onto import DBType


class _FakeDB:
    def __init__(self):
        self.upsert_calls: list[tuple[list[dict], str, list[str]]] = []

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        self.upsert_calls.append((docs, class_name, list(match_keys)))

    def insert_return_batch(self, docs, class_name):
        raise AssertionError("insert_return_batch must not be used for blank vertices")


class _FakeConnectionManager:
    db = _FakeDB()

    def __init__(self, connection_config):
        self.connection_config = connection_config

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc, tb):
        return False


def _build_schema() -> Schema:
    vertex_config = VertexConfig(
        vertices=[
            Vertex(name="blank_v", fields=[], identity=[]),
            Vertex(name="target_v", fields=[Field(name="id")], identity=["id"]),
        ],
        blank_vertices=["blank_v"],
        db_flavor=DBType.NEO4J,
    )
    edge_config = EdgeConfig(edges=[Edge(source="blank_v", target="target_v")])
    return Schema(
        general=SchemaMetadata(name="test"),
        vertex_config=vertex_config,
        edge_config=edge_config,
        resources=[],
    )


def test_push_vertices_blank_uses_python_generated_identity(monkeypatch):
    schema = _build_schema()
    writer = DBWriter(schema=schema, dry=False, max_concurrent=1)
    gc = GraphContainer(vertices={"blank_v": [{}]}, edges={}, linear=[])

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _FakeConnectionManager)

    conn_conf = ArangoConfig(uri="http://localhost:8529", username="root", password="x")
    asyncio.run(writer._push_vertices(gc, conn_conf))

    assert "_key" in gc.vertices["blank_v"][0]
    assert isinstance(gc.vertices["blank_v"][0]["_key"], str)
    assert gc.vertices["blank_v"][0]["_key"]


def test_resolve_blank_edges_prefers_identity_join_over_zip():
    schema = _build_schema()
    writer = DBWriter(schema=schema, dry=False, max_concurrent=1)
    gc = GraphContainer(
        vertices={
            "blank_v": [{"id": "b-2"}, {"id": "b-1"}],
            "target_v": [{"id": "b-1"}, {"id": "b-2"}],
        },
        edges={},
        linear=[],
    )

    writer._resolve_blank_edges(gc)
    edge_id = ("blank_v", "target_v", None)
    pairs = gc.edges[edge_id]

    assert len(pairs) == 2
    assert pairs[0][0]["id"] == pairs[0][1]["id"]
    assert pairs[1][0]["id"] == pairs[1][1]["id"]


def test_blank_vertex_default_identity_depends_on_db_flavor():
    arango_cfg = VertexConfig(
        vertices=[Vertex(name="blank_v", fields=[], identity=[])],
        blank_vertices=["blank_v"],
        db_flavor=DBType.ARANGO,
    )
    neo4j_cfg = VertexConfig(
        vertices=[Vertex(name="blank_v", fields=[], identity=[])],
        blank_vertices=["blank_v"],
        db_flavor=DBType.NEO4J,
    )

    assert arango_cfg["blank_v"].identity == ["_key"]
    assert neo4j_cfg["blank_v"].identity == ["id"]
