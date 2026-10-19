"""Read guards and union-merged labels against a live Neo4j.

A diamond whose short path crosses a hidden vertex: ``a -> h -> d`` and
``a -> b -> c -> d``, with ``h`` in a zone the reader does not open.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.db_writer import DBWriter
from graflo.onto import DBType

OPEN, HIDDEN = 1, 2


def zones_guard(zones: set[int]) -> FilterExpression:
    """A read guard: the element shares a zone with *zones*."""
    return FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[sorted(zones)],
    )


def _zones(merge: str) -> Field:
    return Field(
        name="access_zones",
        type="LIST",
        item_type="INT",
        reserved=True,
        merge=merge,  # ty: ignore[invalid-argument-type]
    )


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="access_guard"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="Node",
                        properties=[Field(name="key"), _zones("union")],
                        identity=["key"],
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source="Node",
                        target="Node",
                        relation="next",
                        properties=[_zones("replace")],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )


def _write(conn_conf, schema: Schema, keys: list[str], edges, zone: int) -> None:
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=model, dry=False, max_concurrent=1)
    gc = GraphContainer(
        vertices={"Node": [{"key": key} for key in keys]},
        edges={
            ("Node", "Node", "next"): [({"key": s}, {"key": t}, {}) for s, t in edges]
        },
    )
    asyncio.run(writer.write(gc, conn_conf, None, stamp={"access_zones": [zone]}))


@pytest.fixture
def diamond(conn_conf, clean_db) -> Schema:
    schema = _schema()
    with ConnectionManager(connection_config=conn_conf) as db:
        db.define_schema(schema)
    _write(
        conn_conf,
        schema,
        ["a", "b", "c", "d"],
        [("a", "b"), ("b", "c"), ("c", "d")],
        OPEN,
    )
    _write(conn_conf, schema, ["h"], [("a", "h"), ("h", "d")], HIDDEN)
    return schema


def _reached(conn_conf, schema: Schema, hops: int, guard) -> set[str]:
    with ConnectionManager(connection_config=conn_conf) as db:
        found = db.graph_neighbors(
            "Node",
            "a",
            hops=hops,
            direction=EdgeDirection.OUT,
            schema=schema,
            guard=guard,
        )
    return {doc["key"] for doc in found.vertices.get("Node", [])}


def test_the_short_path_through_a_hidden_vertex_is_not_taken(conn_conf, diamond):
    guard = zones_guard({OPEN})

    assert _reached(conn_conf, diamond, 2, guard) == {"b", "c"}
    assert _reached(conn_conf, diamond, 3, guard) == {"b", "c", "d"}
    assert _reached(conn_conf, diamond, 2, None) == {"b", "c", "h", "d"}


def test_a_hidden_vertex_is_never_matched(conn_conf, diamond):
    with ConnectionManager(connection_config=conn_conf) as db:
        found = db.resolve_vertices(
            "Node", [{"key": "h"}], ("key",), ("key",), guard=zones_guard({OPEN})
        )

    assert found == {}


def test_a_union_field_holds_every_writer_s_zones(conn_conf, diamond):
    _write(conn_conf, diamond, ["d"], [], HIDDEN)

    with ConnectionManager(connection_config=conn_conf) as db:
        (doc,) = db.fetch_docs("Node", filters=["==", "d", "key"])

    assert sorted(doc["access_zones"]) == [OPEN, HIDDEN]
