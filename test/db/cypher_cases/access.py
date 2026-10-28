"""Read guards and scoped deletes, shared by the Neo4j and FalkorDB suites.

A diamond whose short path crosses a hidden vertex: ``a -> h -> d`` and
``a -> b -> c -> d``, with ``h`` in a zone the reader does not open.

Not collected here: each backend's ``test_access_guard.py`` imports the cases
and the ``diamond`` fixture, and defines ``zones_merge`` — ``union`` where the
backend supports it, ``replace`` otherwise.
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

OPEN, HIDDEN = 1, 2
NEXT = ("Node", "Node", "next")


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


def access_schema(conn_conf, zones_merge: str) -> Schema:
    """One vertex type and one edge, both zoned and scoped by ``installation``."""
    return Schema(
        metadata=GraphMetadata(name="access_guard"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="Node",
                        properties=[
                            Field(name="key"),
                            Field(name="installation"),
                            _zones(zones_merge),
                        ],
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
                        properties=[Field(name="installation"), _zones("replace")],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=conn_conf.connection_type),
    )


def fresh_schema(conn_conf, zones_merge: str) -> Schema:
    """The schema, defined on a database holding no ``Node`` yet.

    Cleared here rather than by ``clean_db``, which on Neo4j removes nothing.
    """
    schema = access_schema(conn_conf, zones_merge)
    with ConnectionManager(connection_config=conn_conf) as db:
        db.delete_graph_structure(vertex_types=["Node"])
        db.define_schema(schema)
    return schema


def _writer(schema: Schema) -> DBWriter:
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    return DBWriter(schema=schema, ingestion_model=model, dry=False, max_concurrent=1)


def write_zone(conn_conf, schema: Schema, keys: list[str], edges, zone: int) -> None:
    gc = GraphContainer(
        vertices={"Node": [{"key": key} for key in keys]},
        edges={NEXT: [({"key": s}, {"key": t}, {}) for s, t in edges]},
    )
    asyncio.run(
        _writer(schema).write(gc, conn_conf, None, stamp={"access_zones": [zone]})
    )


@pytest.fixture
def diamond(conn_conf, clean_db, zones_merge) -> Schema:
    schema = fresh_schema(conn_conf, zones_merge)
    write_zone(
        conn_conf,
        schema,
        ["a", "b", "c", "d"],
        [("a", "b"), ("b", "c"), ("c", "d")],
        OPEN,
    )
    write_zone(conn_conf, schema, ["h"], [("a", "h"), ("h", "d")], HIDDEN)
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


def test_a_scoped_delete_removes_exactly_its_scope(conn_conf, clean_db, zones_merge):
    schema = fresh_schema(conn_conf, zones_merge)
    gc = GraphContainer(
        vertices={
            "Node": [
                {"key": "a", "installation": "T1"},
                {"key": "b", "installation": "T1"},
                {"key": "c", "installation": "T2"},
            ]
        },
        edges={
            NEXT: [
                ({"key": "a"}, {"key": "b"}, {"installation": "T1"}),
                ({"key": "b"}, {"key": "c"}, {"installation": "T2"}),
                ({"key": "c"}, {"key": "a"}, {"installation": "T2"}),
            ]
        },
    )
    writer = _writer(schema)
    asyncio.run(writer.write(gc, conn_conf, None, stamp={"access_zones": [OPEN]}))

    writer.delete_scoped(conn_conf, "installation", "T1", edges=[NEXT])
    with ConnectionManager(connection_config=conn_conf) as db:
        kept = db.fetch_all_edges("Node", "Node", "next")
    assert sorted((s["key"], t["key"]) for s, t, _ in kept) == [("b", "c"), ("c", "a")]

    writer.delete_scoped(conn_conf, "installation", "T1", vertices=["Node"])
    with ConnectionManager(connection_config=conn_conf) as db:
        nodes = db.fetch_all_docs("Node")
        kept = db.fetch_all_edges("Node", "Node", "next")
    assert [doc["key"] for doc in nodes] == ["c"]
    assert kept == []
