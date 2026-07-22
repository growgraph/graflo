"""Tests for FieldType.UUID, assigned identity mode, and UUID helpers."""

from __future__ import annotations

import asyncio
import re

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.graph_types import (
    AssemblyContext,
    ExtractionContext,
    GraphContainer,
    LocationIndex,
    VertexRep,
)
from graflo.architecture.pipeline.runtime.actor.wrapper import ActorWrapper
from graflo.architecture.pipeline.runtime.assemble import assemble_edges
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db.connection import Neo4jConfig
from graflo.db.field_type_support import tigergraph_type_for_field
from graflo.db.identity_uuid import (
    UUID_PATTERN,
    ensure_assigned_uuid,
    ensure_assigned_uuids_in_acc_vertex,
    validate_uuid_typed_identity_fields,
    validate_uuid_value,
)
from graflo.db.nebula.util import nebula_type
from graflo.hq.db_writer import DBWriter
from graflo.hq.document_caster import filter_graph_container_drop_empty_identity_inplace
from graflo.onto import DBType


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def test_field_type_uuid_is_scalar() -> None:
    field = Field(name="id", type=FieldType.UUID)
    assert field.type == FieldType.UUID
    listed = Field(name="ids", type=FieldType.LIST, item_type=FieldType.UUID)
    assert listed.item_type == FieldType.UUID


def test_tigergraph_and_nebula_map_uuid_to_string() -> None:
    assert tigergraph_type_for_field(Field(name="id", type=FieldType.UUID)) == "STRING"
    assert (
        tigergraph_type_for_field(
            Field(name="ids", type=FieldType.LIST, item_type=FieldType.UUID)
        )
        == "LIST<STRING>"
    )
    assert nebula_type(FieldType.UUID) == "string"


def test_assigned_identity_mode_and_normalization() -> None:
    vertex = Vertex(name="event", properties=[], assigned=True)
    config = VertexConfig(vertices=[vertex])
    assert config.vertices[0].identity_mode == "assigned"
    assert config.identity_fields("event") == ["id"]
    assert "event" in config.assigned_vertices
    assert config.vertices_by_identity_mode("assigned") == ["event"]
    id_field = next(f for f in config.vertices[0].properties if f.name == "id")
    assert id_field.type == FieldType.UUID


def test_blank_and_assigned_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        Vertex(name="x", properties=[], blank=True, assigned=True)


def test_assigned_and_hash_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        Vertex(
            name="x",
            properties=[Field(name="a")],
            assigned=True,
            hash_identity_properties=["a"],
        )


def test_validate_uuid_value_and_ensure_assigned() -> None:
    assert validate_uuid_value("550e8400-e29b-41d4-a716-446655440000")
    with pytest.raises(ValueError, match="invalid UUID"):
        validate_uuid_value("not-a-uuid")

    doc: dict = {}
    ensure_assigned_uuid(doc, "id")
    assert _UUID_RE.match(doc["id"])
    kept = doc["id"]
    ensure_assigned_uuid(doc, "id")
    assert doc["id"] == kept

    bad = {"id": "nope"}
    with pytest.raises(ValueError, match="invalid UUID"):
        ensure_assigned_uuid(bad, "id")


def test_validate_uuid_typed_natural_identity_leaves_empty() -> None:
    vertex = Vertex(
        name="user",
        properties=[
            Field(name="external_id", type=FieldType.UUID),
            Field(name="email", type=FieldType.STRING),
        ],
        identity=["external_id"],
    )
    doc: dict = {"email": "a@b.c"}
    validate_uuid_typed_identity_fields(doc, vertex)
    assert "external_id" not in doc or doc.get("external_id") in (None, "")

    doc2 = {"external_id": "550e8400-e29b-41d4-a716-446655440000"}
    validate_uuid_typed_identity_fields(doc2, vertex)
    assert doc2["external_id"] == "550e8400-e29b-41d4-a716-446655440000"

    doc3 = {"external_id": "bad"}
    with pytest.raises(ValueError, match="invalid UUID"):
        validate_uuid_typed_identity_fields(doc3, vertex)


def test_ensure_assigned_uuids_in_acc_vertex_before_edges() -> None:
    vc = VertexConfig(
        vertices=[
            Vertex(name="event", properties=[], assigned=True),
            Vertex(
                name="user",
                properties=[Field(name="id", type=FieldType.STRING)],
                identity=["id"],
            ),
        ]
    )
    edge = Edge(source="event", target="user", relation="by")
    edge.finish_init(vc)

    ext = ExtractionContext()
    loc = LocationIndex(())
    ext.acc_vertex["event"][loc] = [VertexRep(vertex={"payload": "x"})]
    ext.acc_vertex["user"][loc] = [VertexRep(vertex={"id": "u1"})]
    from graflo.architecture.graph_types import EdgeIntent

    ext.edge_intents.append(EdgeIntent(edge=edge, location=loc, derivation=None))

    asm = AssemblyContext.from_extraction(ext)
    ensure_assigned_uuids_in_acc_vertex(asm.acc_vertex, vc)
    event_id = asm.acc_vertex["event"][loc][0].vertex["id"]
    assert _UUID_RE.match(event_id)

    assemble_edges(
        ctx=asm,
        vertex_config=vc,
        edge_config=EdgeConfig(edges=[edge]),
        infer_edges=False,
    )
    edge_key = ("event", "user", "by")
    edge_docs = list(asm.acc_global[edge_key])
    assert edge_docs, "expected assembled edge documents"
    source_proj, target_proj, _weight = edge_docs[0]
    assert source_proj["id"] == event_id
    assert target_proj["id"] == "u1"


def test_actor_wrapper_assemble_mints_assigned() -> None:
    from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
    from graflo.architecture.pipeline.runtime.actor.config import VertexActorConfig
    from graflo.architecture.graph_types import EdgeIntent

    vc = VertexConfig(
        vertices=[
            Vertex(name="event", properties=[], assigned=True),
            Vertex(
                name="user",
                properties=[Field(name="id")],
                identity=["id"],
            ),
        ]
    )
    wrapper = ActorWrapper.from_config(VertexActorConfig(type="vertex", vertex="event"))
    edge = Edge(source="event", target="user", relation="by")
    edge.finish_init(vc)
    wrapper.finish_init(
        ActorInitContext(
            vertex_config=vc,
            edge_config=EdgeConfig(edges=[edge]),
            transforms={},
            infer_edges=False,
        )
    )

    ext = ExtractionContext()
    loc = LocationIndex(())
    ext.acc_vertex["event"][loc] = [VertexRep(vertex={"payload": "x"})]
    ext.acc_vertex["user"][loc] = [VertexRep(vertex={"id": "u1"})]
    ext.edge_intents.append(EdgeIntent(edge=edge, location=loc, derivation=None))

    result = wrapper.assemble(ext)
    event_docs = result.get("event", [])
    assert event_docs
    assert _UUID_RE.match(event_docs[0]["id"])
    edge_docs = result.get(("event", "user", "by"), [])
    assert edge_docs
    assert edge_docs[0][0]["id"] == event_docs[0]["id"]


def test_drop_empty_identity_exempts_assigned() -> None:
    vc = VertexConfig(vertices=[Vertex(name="event", properties=[], assigned=True)])
    gc = GraphContainer(vertices={"event": [{}]}, edges={}, linear=[])
    filter_graph_container_drop_empty_identity_inplace(gc, vertex_config=vc)
    assert gc.vertices["event"] == [{}]


def test_assign_assigned_vertex_ids_writer_net(monkeypatch) -> None:
    class _FakeDB:
        def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
            return None

    class _FakeConnectionManager:
        db = _FakeDB()

        def __init__(self, connection_config):
            self.connection_config = connection_config

        def __enter__(self):
            return self.db

        def __exit__(self, exc_type, exc, tb):
            return False

    vertex_config = VertexConfig(
        vertices=[Vertex(name="event", properties=[], assigned=True)]
    )
    schema = Schema(
        metadata=GraphMetadata(name="test"),
        core_schema=CoreSchema(
            vertex_config=vertex_config, edge_config=EdgeConfig(edges=[])
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )
    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=ingestion_model, dry=False)
    known = "550e8400-e29b-41d4-a716-446655440000"
    gc = GraphContainer(
        vertices={
            "event": [
                {},
                {"id": known},
            ]
        },
        edges={},
        linear=[],
    )
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _FakeConnectionManager)
    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")
    asyncio.run(writer._push_vertices(gc, conn_conf))

    minted = gc.vertices["event"][0]["id"]
    assert _UUID_RE.match(minted)
    assert gc.vertices["event"][1]["id"] == known

    with pytest.raises(ValueError, match="invalid UUID"):
        writer._assign_assigned_vertex_ids("event", [{"id": "bad"}], conn_conf)


def test_assigned_vertices_not_in_blank_edge_resolution() -> None:
    vertex_config = VertexConfig(
        vertices=[
            Vertex(name="event", properties=[], assigned=True),
            Vertex(
                name="user",
                properties=[Field(name="id")],
                identity=["id"],
            ),
        ]
    )
    schema = Schema(
        metadata=GraphMetadata(name="test"),
        core_schema=CoreSchema(
            vertex_config=vertex_config,
            edge_config=EdgeConfig(
                edges=[Edge(source="event", target="user", relation="by")]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )
    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=ingestion_model, dry=False)
    gc = GraphContainer(
        vertices={
            "event": [{"id": "e1"}, {"id": "e2"}],
            "user": [{"id": "e1"}, {"id": "e2"}],
        },
        edges={},
        linear=[],
    )
    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")
    writer._resolve_blank_edges(gc, conn_conf)
    assert gc.edges == {}


def test_natural_uuid_validate_on_writer(monkeypatch) -> None:
    class _FakeDB:
        def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
            return None

    class _FakeConnectionManager:
        db = _FakeDB()

        def __init__(self, connection_config):
            self.connection_config = connection_config

        def __enter__(self):
            return self.db

        def __exit__(self, exc_type, exc, tb):
            return False

    vertex_config = VertexConfig(
        vertices=[
            Vertex(
                name="user",
                properties=[Field(name="external_id", type=FieldType.UUID)],
                identity=["external_id"],
            )
        ]
    )
    schema = Schema(
        metadata=GraphMetadata(name="test"),
        core_schema=CoreSchema(
            vertex_config=vertex_config, edge_config=EdgeConfig(edges=[])
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )
    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=ingestion_model, dry=False)
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _FakeConnectionManager)
    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")

    good = GraphContainer(
        vertices={
            "user": [
                {"external_id": "550e8400-e29b-41d4-a716-446655440000"},
                {},
            ]
        },
        edges={},
        linear=[],
    )
    asyncio.run(writer._push_vertices(good, conn_conf))
    assert (
        good.vertices["user"][1] == {}
        or "external_id" not in good.vertices["user"][1]
        or good.vertices["user"][1].get("external_id") in (None, "")
    )

    bad = GraphContainer(
        vertices={"user": [{"external_id": "nope"}]},
        edges={},
        linear=[],
    )
    with pytest.raises(ValueError, match="invalid UUID"):
        asyncio.run(writer._push_vertices(bad, conn_conf))


def test_uuid_pattern_shared_with_identity_inference() -> None:
    from graflo.db.identity_inference import _UUID_PATTERN

    assert _UUID_PATTERN is UUID_PATTERN
