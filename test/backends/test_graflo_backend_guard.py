"""The file backend reads as a database would: guards, upserts and unions.

A diamond whose short path crosses a hidden vertex: ``a -> h -> d`` and
``a -> b -> c -> d``, with ``h`` and its edges in a zone the reader does not
open. The same scenario runs live against Neo4j in the access-guard suite.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.db_writer import DBWriter
from graflo.onto import DBType

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


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="access_guard"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="Node",
                        properties=[
                            Field(name="key"),
                            Field(name="label"),
                            _zones("union"),
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
                        properties=[Field(name="weight"), _zones("replace")],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.GRAFLO_BACKEND),
    )


def _write(
    conf: GraFloBackendConfig,
    schema: Schema,
    nodes: list[dict[str, Any]],
    edges: list[tuple[str, str, dict[str, Any]]],
    zone: int,
) -> None:
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=model, dry=False, max_concurrent=1)
    gc = GraphContainer(
        vertices={"Node": nodes},
        edges={NEXT: [({"key": s}, {"key": t}, w) for s, t, w in edges]},
    )
    asyncio.run(writer.write(gc, conf, None, stamp={"access_zones": [zone]}))


def _keys(*keys: str) -> list[dict[str, Any]]:
    return [{"key": key} for key in keys]


@pytest.fixture
def conf(tmp_path: Path) -> GraFloBackendConfig:
    return GraFloBackendConfig(output_dir=tmp_path)


@pytest.fixture
def diamond(conf: GraFloBackendConfig) -> Schema:
    schema = _schema()
    with ConnectionManager(connection_config=conf) as db:
        db.init_db(schema, recreate_schema=True)
    _write(
        conf,
        schema,
        _keys("a", "b", "c", "d"),
        [("a", "b", {}), ("b", "c", {}), ("c", "d", {})],
        OPEN,
    )
    _write(conf, schema, _keys("h"), [("a", "h", {}), ("h", "d", {})], HIDDEN)
    return schema


def _walk(
    conf: GraFloBackendConfig, schema: Schema, hops: int, guard, anchor: str = "a"
) -> GraphContainer:
    with ConnectionManager(connection_config=conf) as db:
        return db.graph_neighbors(
            "Node",
            anchor,
            hops=hops,
            direction=EdgeDirection.OUT,
            schema=schema,
            guard=guard,
        )


def _reached(found: GraphContainer) -> set[str]:
    return {doc["key"] for doc in found.vertices.get("Node", [])}


def _edges(found: GraphContainer) -> set[tuple[str, str]]:
    return {(row["source"], row["target"]) for row in found.edges.get(NEXT, [])}


def test_the_short_path_through_a_hidden_vertex_is_not_taken(conf, diamond):
    guard = zones_guard({OPEN})

    assert _reached(_walk(conf, diamond, 2, guard)) == {"b", "c"}
    assert _reached(_walk(conf, diamond, 3, guard)) == {"b", "c", "d"}
    assert _reached(_walk(conf, diamond, 2, None)) == {"b", "c", "h", "d"}


def test_an_edge_is_reported_only_when_it_and_both_its_ends_pass(conf, diamond):
    guarded = _walk(conf, diamond, 3, zones_guard({OPEN}))
    unguarded = _walk(conf, diamond, 3, None)

    assert _edges(guarded) == {("a", "b"), ("b", "c"), ("c", "d")}
    assert _edges(unguarded) == {
        ("a", "b"),
        ("b", "c"),
        ("c", "d"),
        ("a", "h"),
        ("h", "d"),
    }


def test_a_hidden_anchor_reaches_nothing(conf, diamond):
    found = _walk(conf, diamond, 2, zones_guard({OPEN}), anchor="h")

    assert found.vertices == {}
    assert found.edges == {}


def test_a_hidden_vertex_is_never_matched(conf, diamond):
    with ConnectionManager(connection_config=conf) as db:
        found = db.resolve_vertices(
            "Node", [{"key": "h"}], ("key",), ("key",), guard=zones_guard({OPEN})
        )

    assert found == {}


def test_a_guarded_fetch_leaves_the_hidden_out(conf, diamond):
    with ConnectionManager(connection_config=conf) as db:
        docs = db.fetch_docs("Node", guard=zones_guard({OPEN}))

    assert sorted(doc["key"] for doc in docs) == ["a", "b", "c", "d"]


def test_a_union_field_holds_every_writer_s_zones(conf, diamond):
    _write(conf, diamond, _keys("d"), [], HIDDEN)

    with ConnectionManager(connection_config=conf) as db:
        (doc,) = db.fetch_docs("Node", filters=["==", "d", "key"])

    assert doc["access_zones"] == [OPEN, HIDDEN]


def test_an_upsert_replaces_in_place(conf, diamond):
    _write(conf, diamond, [{"key": "b", "label": "first"}], [], OPEN)
    _write(conf, diamond, [{"key": "b", "label": "second"}], [], OPEN)

    with ConnectionManager(connection_config=conf) as db:
        docs = db.fetch_docs("Node")

    assert sorted(doc["key"] for doc in docs) == ["a", "b", "c", "d", "h"]
    (b,) = [doc for doc in docs if doc["key"] == "b"]
    assert b["label"] == "second"


def test_an_edge_written_twice_is_one_edge(conf, diamond):
    _write(conf, diamond, [], [("a", "b", {"weight": 1})], OPEN)
    _write(conf, diamond, [], [("a", "b", {"weight": 2})], HIDDEN)

    with ConnectionManager(connection_config=conf) as db:
        rows = db.fetch_all_edges("Node", "Node", "next")
        found = db.graph_neighbors("Node", "a", hops=1, schema=diamond)

    ab = [row for row in rows if row[0]["key"] == "a" and row[1]["key"] == "b"]
    assert len(ab) == 1
    assert ab[0][2]["weight"] == 2
    assert ab[0][2]["access_zones"] == [HIDDEN]
    assert sorted(_edges(found)) == [("a", "b"), ("a", "h")]


def test_a_connection_sees_what_another_published_after_its_first_read(conf, diamond):
    with ConnectionManager(connection_config=conf) as db:
        assert (
            _reached(db.graph_neighbors("Node", "d", hops=1, schema=diamond)) == set()
        )
        _write(conf, diamond, _keys("e"), [("d", "e", {})], OPEN)
        reached = db.graph_neighbors("Node", "d", hops=1, schema=diamond)
        keys = sorted(doc["key"] for doc in db.fetch_docs("Node"))

    assert _reached(reached) == {"e"}
    assert keys == ["a", "b", "c", "d", "e", "h"]


def test_the_backend_declares_what_it_applies(conf):
    with ConnectionManager(connection_config=conf) as db:
        assert db.supports_read_guard
        assert db.supports_union_merge
