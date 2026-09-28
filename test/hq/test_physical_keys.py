"""Stored property names are applied at the database boundary, not in the model.

Documents stay keyed by logical names through the whole pipeline; ``DBWriter``
hands every backend call stored names and translates back what it reads.
"""

from __future__ import annotations

import asyncio
from typing import Any

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.connections.onto import Neo4jConfig, TigergraphConfig
from graflo.db.conn import Connection
from graflo.filter.onto import FilterExpression
from graflo.hq.db_writer import DBWriter
from graflo.hq.sanitizer import Sanitizer
from graflo.onto import DBType


class _RecordingDB:
    def __init__(self) -> None:
        self.upserts: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        self.upserts.append(
            {"docs": list(docs), "class_name": class_name, "match_keys": match_keys}
        )

    def insert_edges_batch(self, docs_edges, **kwargs):
        self.edges.append({"docs": list(docs_edges), **kwargs})


def _recording_manager(db: _RecordingDB):
    class _Manager:
        def __init__(self, connection_config):
            self.connection_config = connection_config

        def __enter__(self):
            return db

        def __exit__(self, exc_type, exc, tb):
            return False

    return _Manager


def _schema(flavor: DBType = DBType.TIGERGRAPH) -> Schema:
    """``from`` and ``type`` are TigerGraph reserved words; ``to`` is too."""
    return Schema(
        metadata=GraphMetadata(name="stored_names"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="route",
                        properties=[
                            Field(name="from", type=FieldType.STRING),
                            Field(name="name"),
                        ],
                        identity=["from"],
                    ),
                    Vertex(name="stop", properties=[Field(name="id")], identity=["id"]),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source="route",
                        target="stop",
                        relation="serves",
                        properties=[Field(name="type")],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=flavor),
    )


def _write(schema: Schema, gc: GraphContainer, conn_conf) -> _RecordingDB:
    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=ingestion_model)
    db = _RecordingDB()
    import graflo.hq.db_writer as db_writer_module

    original = db_writer_module.ConnectionManager
    db_writer_module.ConnectionManager = _recording_manager(db)  # type: ignore[misc]
    try:
        asyncio.run(writer.write(gc=gc, conn_conf=conn_conf, resource_name=None))
    finally:
        db_writer_module.ConnectionManager = original  # type: ignore[misc]
    return db


def _tigergraph() -> TigergraphConfig:
    return TigergraphConfig(uri="http://localhost:14240", username="u", password="p")


def _container() -> GraphContainer:
    route = {"from": "A", "name": "first"}
    stop = {"id": "s1"}
    return GraphContainer(
        vertices={"route": [route], "stop": [stop]},
        edges={("route", "stop", "serves"): [(route, stop, {"type": "bus"})]},
        linear=[],
    )


def test_writer_keys_documents_by_stored_names():
    gc = _container()

    db = _write(_schema(), gc, _tigergraph())

    route_upsert = next(u for u in db.upserts if u["class_name"] == "route")
    assert route_upsert["docs"] == [{"from_attr": "A", "name": "first"}]
    assert list(route_upsert["match_keys"]) == ["from_attr"]
    (edge_call,) = db.edges
    source_doc, target_doc, weights = edge_call["docs"][0]
    assert target_doc == {"id": "s1"}
    assert source_doc == {"from_attr": "A", "name": "first"}
    assert weights["type_attr"] == "bus"
    assert "type" not in weights
    assert edge_call["match_keys_source"] == ("from_attr",)


def test_writer_leaves_the_callers_documents_logical():
    gc = _container()

    _write(_schema(), gc, _tigergraph())

    assert gc.vertices["route"] == [{"from": "A", "name": "first"}]


def test_writer_is_a_no_op_translation_where_names_are_valid():
    """Neo4j has no reserved-word list, so every name is stored as-is."""
    gc = _container()

    db = _write(
        _schema(DBType.NEO4J),
        gc,
        Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p"),
    )

    route_upsert = next(u for u in db.upserts if u["class_name"] == "route")
    assert route_upsert["docs"] == [{"from": "A", "name": "first"}]


def test_migrated_data_lands_on_the_stored_attributes():
    """Migrating into TigerGraph sanitizes the target schema, not the exported data.

    The exported container is keyed by the source's names. Before names lived in
    the profile, sanitizing renamed the schema's properties while the data kept
    the old keys, so the renamed property was never written and a renamed
    identity could not be read off the documents.
    """
    schema = _schema(DBType.NEO4J)
    manifest = GraphManifest(
        graph_schema=schema, ingestion_model=IngestionModel(resources=[])
    )
    schema.db_profile.db_flavor = DBType.TIGERGRAPH
    Sanitizer(DBType.TIGERGRAPH).sanitize_manifest(manifest)
    target = manifest.require_schema()

    db = _write(target, _container(), _tigergraph())

    route_upsert = next(u for u in db.upserts if u["class_name"] == "route")
    assert route_upsert["docs"] == [{"from_attr": "A", "name": "first"}]
    assert list(route_upsert["match_keys"]) == ["from_attr"]


def test_physical_keys_round_trip_a_vertex_document():
    profile = DatabaseProfile(vertex_property_names={"route": {"from": "from_attr"}})
    keys = PhysicalKeys(profile)
    doc = {"from": "A", "name": "first"}

    stored = keys.vertex_doc("route", doc)

    assert stored == {"from_attr": "A", "name": "first"}
    assert keys.logical_vertex_doc("route", stored) == doc


def test_physical_keys_return_the_input_when_nothing_is_renamed():
    keys = PhysicalKeys(DatabaseProfile())
    docs = [{"a": 1}]

    assert keys.active is False
    assert keys.vertex_docs("route", docs) is docs


def test_extra_weights_project_fields_and_map_like_the_pipeline():
    from graflo.architecture.graph_types import Weight
    from graflo.hq.db_writer import _weight_attributes, _weight_source_fields

    weight = Weight(name="route", fields=["name"], map={"from": "origin"})
    doc = {"name": "first", "from": "A", "other": 1}

    assert _weight_source_fields(weight) == ["name", "from"]
    assert _weight_attributes(weight, doc) == {"route@name": "first", "origin": "A"}


# -- schema-aware reads: logical names in, logical names out ---------------------


class _RecordingReader:
    """Borrows ``Connection``'s read wrapper; records what the backend hook sees."""

    graph_neighbors = Connection.graph_neighbors
    traverse = Connection.traverse
    _stored_view = Connection._stored_view

    def __init__(self, flavor: DBType, reached: GraphContainer):
        self.flavor = flavor
        self.reached = reached
        self.calls: list[dict[str, Any]] = []

    def _graph_neighbors(self, vertex_type, key, **kwargs):
        self.calls.append({"vertex_type": vertex_type, "key": key, **kwargs})
        return self.reached


def _schema_with_inverse(flavor: DBType = DBType.TIGERGRAPH) -> Schema:
    schema = _schema(flavor)
    return Schema.model_validate(
        {
            **schema.to_dict(skip_defaults=False),
            "core_schema": {
                **schema.core_schema.to_dict(skip_defaults=False),
                "edge_config": {
                    **schema.core_schema.edge_config.to_dict(skip_defaults=False),
                    "inverses": [{"relation": "serves", "inverse": "served_by"}],
                },
            },
        }
    )


def _stored_reach() -> GraphContainer:
    return GraphContainer(
        vertices={
            "route": [{"from_attr": "A", "name": "first"}],
            "stop": [{"id": "s1"}],
        },
        edges={
            ("route", "stop", "serves"): [
                {"type_attr": "bus", "source": "A", "target": "s1"}
            ],
            ("stop", "route", "served_by"): [
                {"type_attr": "bus", "source": "s1", "target": "A"}
            ],
        },
        linear=[],
    )


def test_graph_neighbors_speaks_logical_names_on_both_sides():
    reader = _RecordingReader(DBType.TIGERGRAPH, _stored_reach())

    reached = reader.graph_neighbors(
        "route",
        {"from": "A"},
        filters=FilterExpression(
            kind="leaf", field="type", cmp_operator="==", value="bus"
        ),
        schema=_schema_with_inverse(),
    )

    (call,) = reader.calls
    assert call["key"] == {"from_attr": "A"}
    assert call["filters"].field == "type_attr"
    assert call["schema"].core_schema.vertex_config["route"].identity == ["from_attr"]
    assert reached.vertices["route"] == [{"from": "A", "name": "first"}]
    assert reached.edges[("route", "stop", "serves")][0]["type"] == "bus"
    # Read through the declared inverse: translated with the stored edge's names.
    assert reached.edges[("stop", "route", "served_by")][0]["type"] == "bus"


def test_traverse_resolves_the_stored_view_once_for_all_seeds():
    from graflo.architecture.query.models import TraverseQuery

    reader = _RecordingReader(DBType.TIGERGRAPH, _stored_reach())
    query = TraverseQuery(
        seeds=[
            {"vertex_type": "route", "key": {"from": "A"}},
            {"vertex_type": "route", "key": {"from": "B"}},
        ],
    )

    reached = reader.traverse(query, schema=_schema_with_inverse())

    assert [call["key"] for call in reader.calls] == [
        {"from_attr": "A"},
        {"from_attr": "B"},
    ]
    assert reader.calls[0]["schema"] is reader.calls[1]["schema"]
    assert reached.vertices["route"] == [{"from": "A", "name": "first"}]


def test_reads_cost_nothing_where_every_name_is_stored_as_is():
    """No naming rules and no maps: the backend gets the caller's schema and
    the caller gets the backend's container, untouched."""
    schema = _schema(DBType.NEO4J)
    stored = GraphContainer(vertices={"route": [{"from": "A"}]}, edges={}, linear=[])
    reader = _RecordingReader(DBType.NEO4J, stored)

    reached = reader.graph_neighbors("route", {"from": "A"}, schema=schema)

    assert reader.calls[0]["schema"] is schema
    assert reached is stored
