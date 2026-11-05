"""Read guards, scoped deletes and union-merged zones, shared by the Neo4j and
FalkorDB suites.

A diamond whose short path crosses a hidden vertex: ``a -> h -> d`` and
``a -> b -> c -> d``, with ``h`` in a zone the reader does not open. A second
relation, ``rated``, is keyed by its ``kind`` beside its endpoints.

Not collected here: each backend's ``test_access_guard.py`` imports the cases
and the ``diamond`` fixture, and defines ``zones_merge`` — ``union`` where the
backend supports it, ``replace`` otherwise. The union case needs ``union``.
"""

from __future__ import annotations

import asyncio
from typing import Any

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
RATED = ("Node", "Node", "rated")


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
    """One vertex type and two edges, zoned by *zones_merge*; ``next`` is scoped by
    ``installation``, ``rated`` keyed by ``kind``."""
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
                        properties=[Field(name="installation"), _zones(zones_merge)],
                    ),
                    Edge(
                        source="Node",
                        target="Node",
                        relation="rated",
                        properties=[Field(name="kind"), _zones(zones_merge)],
                        identities=[["kind"]],
                    ),
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


def test_a_union_field_holds_every_writer_s_zones(conn_conf, diamond):
    write_zone(conn_conf, diamond, ["d"], [], HIDDEN)

    with ConnectionManager(connection_config=conn_conf) as db:
        (doc,) = db.fetch_docs("Node", filters=["==", "d", "key"])

    assert sorted(doc["access_zones"]) == [OPEN, HIDDEN]


def _edge_zones(conn_conf, relation: str) -> list[tuple[str, str, Any, list[int]]]:
    with ConnectionManager(connection_config=conn_conf) as db:
        rows = db.fetch_all_edges("Node", "Node", relation)
    # An edge without ``kind`` sorts after the keyed ones; ``None`` does not compare.
    return sorted(
        (
            (s["key"], t["key"], props.get("kind"), sorted(props["access_zones"]))
            for s, t, props in rows
        ),
        key=lambda row: (row[0], row[1], row[2] is None, row[2] or ""),
    )


def test_an_edge_written_twice_holds_both_writers_zones(conn_conf, diamond):
    write_zone(conn_conf, diamond, [], [("a", "b")], HIDDEN)

    assert [row for row in _edge_zones(conn_conf, "next") if row[:2] == ("a", "b")] == [
        ("a", "b", None, [OPEN, HIDDEN])
    ]


def test_a_keyed_edge_unions_its_zones_with_and_without_its_key(conn_conf, diamond):
    for zone in (OPEN, HIDDEN):
        gc = GraphContainer(
            edges={
                RATED: [
                    ({"key": "a"}, {"key": "b"}, {"kind": "good"}),
                    ({"key": "a"}, {"key": "b"}, {}),
                    ({"key": "a"}, {"key": "b"}, {}),
                ]
            }
        )
        asyncio.run(
            _writer(diamond).write(gc, conn_conf, None, stamp={"access_zones": [zone]})
        )

    assert _edge_zones(conn_conf, "rated") == [
        ("a", "b", "good", [OPEN, HIDDEN]),
        ("a", "b", None, [OPEN, HIDDEN]),
    ]


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
