"""Live TigerGraph checks for how graflo names what it stores.

Two contracts:

- A relation may span vertices whose identities differ in name and type.
  TigerGraph edge DDL (``FROM A, TO B``) names no identity field, and the writer
  passes each edge's own endpoint keys, so graflo does not rename identities to
  make them agree.
- A property TigerGraph cannot store under its logical name (a reserved word) is
  stored under the name the profile records, filled in at define/write time when
  the manifest was never sanitized.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.db_writer import DBWriter
from graflo.hq.graph_engine import GraphEngine
from graflo.onto import DBType

# Every test here needs a live TigerGraph, whose schema DDL runs 15-40s per graph.
pytestmark = pytest.mark.tigergraph


def _define_and_write(
    schema: Schema, gc: GraphContainer, conn_conf, graph_name: str
) -> None:
    manifest = GraphManifest(
        graph_schema=schema, ingestion_model=IngestionModel(resources=[])
    )
    GraphEngine(target_db_flavor=DBType.TIGERGRAPH).define_schema(
        manifest=manifest,
        target_db_config=conn_conf,
        recreate_schema=True,
        graph_target_namespace=graph_name,
    )
    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=ingestion_model)
    asyncio.run(writer.write(gc=gc, conn_conf=conn_conf, resource_name=None))


def test_relation_spanning_different_identities_ingests(conn_conf, test_graph_name):
    """``contains`` joins box (id: INT) and container (name: STRING) as sources."""
    schema = Schema(
        metadata=GraphMetadata(name=test_graph_name),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="parcel",
                        properties=[Field(name="id", type=FieldType.INT)],
                        identity=["id"],
                    ),
                    Vertex(
                        name="box",
                        properties=[Field(name="id", type=FieldType.INT)],
                        identity=["id"],
                    ),
                    Vertex(
                        name="container",
                        properties=[Field(name="name", type=FieldType.STRING)],
                        identity=["name"],
                    ),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(source="box", target="parcel", relation="contains"),
                    Edge(source="container", target="box", relation="contains"),
                ]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.TIGERGRAPH),
    )
    box, parcel, container = {"id": 1}, {"id": 10}, {"name": "c1"}
    gc = GraphContainer(
        vertices={"box": [box], "parcel": [parcel], "container": [container]},
        edges={
            ("box", "parcel", "contains"): [(box, parcel, {})],
            ("container", "box", "contains"): [(container, box, {})],
        },
        linear=[],
    )

    _define_and_write(schema, gc, conn_conf, test_graph_name)

    with ConnectionManager(connection_config=conn_conf) as db:
        assert len(db.fetch_docs("container")) == 1
        from_container = db.fetch_edges(
            from_type="container", from_id="c1", edge_type="contains"
        )
        from_box = db.fetch_edges(from_type="box", from_id="1", edge_type="contains")
    assert len(from_container) == 1
    assert len(from_box) == 1


def test_reserved_names_are_stored_under_profile_names(conn_conf, test_graph_name):
    """``from`` and ``type`` are GSQL keywords; the manifest was never sanitized."""
    schema = Schema(
        metadata=GraphMetadata(name=test_graph_name),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="route",
                        properties=[
                            Field(name="from", type=FieldType.STRING),
                            Field(name="name", type=FieldType.STRING),
                        ],
                        identity=["from"],
                    ),
                    Vertex(
                        name="stop",
                        properties=[Field(name="id", type=FieldType.STRING)],
                        identity=["id"],
                    ),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source="route",
                        target="stop",
                        relation="serves",
                        properties=[Field(name="type", type=FieldType.STRING)],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.TIGERGRAPH),
    )
    route = {"from": "A", "name": "first"}
    stop = {"id": "s1"}
    gc = GraphContainer(
        vertices={"route": [route], "stop": [stop]},
        edges={("route", "stop", "serves"): [(route, stop, {"type": "bus"})]},
        linear=[],
    )

    _define_and_write(schema, gc, conn_conf, test_graph_name)

    with ConnectionManager(connection_config=conn_conf) as db:
        (stored_route,) = db.fetch_docs("route")
        edges = db.fetch_edges(from_type="route", from_id="A", edge_type="serves")
    assert stored_route["from_attr"] == "A"
    assert stored_route["name"] == "first"
    assert len(edges) == 1
    assert "bus" in str(edges[0])
    # The manifest's own schema still speaks logical names.
    assert schema.core_schema.vertex_config["route"].identity == ["from"]
