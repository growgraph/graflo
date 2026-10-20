"""The writer stamps reserved fields and writes ``merge: union`` lists as a union."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.graph_types.merge import fuse_doc_basis
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.onto import ArangoConfig, Neo4jConfig
from graflo.db.manager import ConnectionManager
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.hq.db_writer import DBWriter, apply_stamp, refuse_reserved_attributes
from graflo.onto import DBType

ZONES = "access_zones"


def _zones(merge: str) -> Field:
    return Field(name=ZONES, type="LIST", item_type="INT", reserved=True, merge=merge)  # ty: ignore[invalid-argument-type]


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="support"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="Ticket",
                        properties=[Field(name="key"), _zones("replace")],
                        identity=["key"],
                    ),
                    Vertex(
                        name="Product",
                        properties=[Field(name="sku"), _zones("union")],
                        identity=["sku"],
                    ),
                    Vertex(name="Note", properties=[Field(name="id")], identity=["id"]),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source="Ticket",
                        target="Product",
                        relation="about",
                        properties=[_zones("replace")],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )


def _writer(schema: Schema) -> DBWriter:
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    return DBWriter(schema=schema, ingestion_model=model, dry=False, max_concurrent=1)


class _DB:
    def __init__(self) -> None:
        self.upserts: list[tuple[str, list[dict], dict[str, Any]]] = []

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        self.upserts.append((class_name, docs, kwargs))


def _manager(db: _DB) -> type:
    class _Manager:
        require = ConnectionManager.require

        def __init__(self, connection_config):
            pass

        def __enter__(self):
            return db

        def __exit__(self, *exc):
            return False

    return _Manager


NEO4J = Neo4jConfig(uri="bolt://localhost:7687", username="neo4j", password="x")
ARANGO = ArangoConfig(uri="http://localhost:8529", username="root", password="x")


def test_the_stamp_overwrites_what_a_document_carries() -> None:
    gc = GraphContainer(
        vertices={"Ticket": [{"key": "SUP-1", ZONES: [999]}], "Note": [{"id": "n"}]},
        edges={("Ticket", "Product", "about"): [({"key": "SUP-1"}, {"sku": "P"}, {})]},
    )

    apply_stamp(gc, _schema(), {ZONES: [7]})

    assert gc.vertices["Ticket"] == [{"key": "SUP-1", ZONES: [7]}]
    assert gc.vertices["Note"] == [{"id": "n"}]
    ((_, _, attrs),) = gc.edges[("Ticket", "Product", "about")]
    assert attrs == {ZONES: [7]}


def test_a_lifted_type_without_a_stamp_is_refused() -> None:
    gc = GraphContainer(vertices={"Ticket": [{"key": "SUP-1"}]})

    with pytest.raises(ValueError, match="write it with a stamp"):
        apply_stamp(gc, _schema(), None)
    with pytest.raises(ValueError, match="write it with a stamp"):
        apply_stamp(gc, _schema(), {"other": 1})


def test_a_container_without_lifted_types_needs_no_stamp() -> None:
    gc = GraphContainer(vertices={"Note": [{"id": "n"}]})

    apply_stamp(gc, _schema(), None)

    assert gc.vertices["Note"] == [{"id": "n"}]


def test_a_union_field_reaches_the_upsert_on_neo4j(monkeypatch) -> None:
    db = _DB()
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager(db))
    gc = GraphContainer(
        vertices={"Product": [{"sku": "P", ZONES: [1]}, {"sku": "P", ZONES: [2]}]}
    )

    asyncio.run(_writer(_schema()).write(gc, NEO4J, None, stamp={ZONES: [3]}))

    ((label, docs, kwargs),) = db.upserts
    assert label == "Product"
    assert kwargs["union_fields"] == (ZONES,)
    assert docs == [{"sku": "P", ZONES: [3]}]


def test_a_replaced_field_needs_no_union(monkeypatch) -> None:
    db = _DB()
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager(db))
    gc = GraphContainer(vertices={"Ticket": [{"key": "SUP-1"}]})

    asyncio.run(_writer(_schema()).write(gc, NEO4J, None, stamp={ZONES: [3]}))

    ((_, _, kwargs),) = db.upserts
    assert "union_fields" not in kwargs


def test_a_union_field_is_refused_where_the_backend_cannot_write_one(
    monkeypatch,
) -> None:
    db = _DB()
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager(db))
    gc = GraphContainer(vertices={"Product": [{"sku": "P"}]})

    with pytest.raises(ValueError, match="union-merged list fields"):
        asyncio.run(_writer(_schema()).write(gc, ARANGO, None, stamp={ZONES: [3]}))
    assert db.upserts == []


def test_in_batch_fusion_keeps_every_member_of_a_union_field() -> None:
    docs = [
        {"sku": "P", ZONES: [1, 2], "name": "a"},
        {"sku": "P", ZONES: [2, 3], "name": "b"},
    ]

    (fused,) = fuse_doc_basis(docs, ("sku",), (ZONES,))

    assert fused == {"sku": "P", ZONES: [1, 2, 3], "name": "b"}
    assert fuse_doc_basis(docs, ("sku",)) == [{"sku": "P", ZONES: [2, 3], "name": "b"}]


class _Execute:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []

    def execute(self, query: str, **params: Any) -> Any:
        self.sent.append((query, params))
        return type("R", (), {"consume": lambda self: None})()


def test_neo4j_writes_a_union_without_letting_the_row_overwrite_it() -> None:
    fake = _Execute()

    Neo4jConnection.upsert_docs_batch(
        cast(Any, fake),
        [{"sku": "P", ZONES: [3], "name": "pump"}],
        "Product",
        ["sku"],
        union_fields=(ZONES,),
    )

    ((query, params),) = fake.sent
    assert "MERGE (n:Product { sku: row.p.sku })" in query
    assert "SET n += row.p" in query
    assert (
        "n.`access_zones` = reduce(acc = [], x IN [y IN coalesce(n.`access_zones`, []) "
        "+ coalesce(row.u.`access_zones`, []) WHERE y IS NOT NULL]" in query
    )
    assert params["batch"] == [{"p": {"sku": "P", "name": "pump"}, "u": {ZONES: [3]}}]


def test_neo4j_keeps_its_plain_upsert_without_union_fields() -> None:
    fake = _Execute()

    Neo4jConnection.upsert_docs_batch(
        cast(Any, fake), [{"sku": "P"}], "Product", ["sku"]
    )

    ((query, params),) = fake.sent
    assert "ON MATCH set n += row" in query
    assert params["batch"] == [{"sku": "P"}]


def test_the_stamp_updates_edge_attributes_in_place() -> None:
    attrs = {"weight": 2}
    gc = GraphContainer(
        edges={
            ("Ticket", "Product", "about"): [
                ({"key": "SUP-1"}, {"sku": "P"}, attrs),
                ({"key": "SUP-2"}, {"sku": "P"}),
            ]
        }
    )

    apply_stamp(gc, _schema(), {ZONES: [7]})

    rows = gc.edges[("Ticket", "Product", "about")]
    assert rows[0][2] is attrs and attrs == {"weight": 2, ZONES: [7]}
    assert rows[1][2] == {ZONES: [7]}


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (3, "must be a list"),
        (["1"], "must hold integers"),
        ([True], "must hold integers"),
    ],
)
def test_a_stamp_value_of_the_wrong_shape_is_refused(value, message) -> None:
    gc = GraphContainer(vertices={"Ticket": [{"key": "SUP-1"}]})

    with pytest.raises(ValueError, match=message):
        apply_stamp(gc, _schema(), {ZONES: value})


def test_a_missing_edge_stamp_is_refused_before_anything_is_written(
    monkeypatch,
) -> None:
    db = _DB()
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager(db))
    gc = GraphContainer(
        vertices={"Note": [{"id": "n"}]},
        edges={("Ticket", "Product", "about"): [({"key": "SUP-1"}, {"sku": "P"}, {})]},
    )

    with pytest.raises(ValueError, match="write it with a stamp"):
        asyncio.run(_writer(_schema()).write(gc, NEO4J, None))
    assert db.upserts == []


def test_union_is_refused_up_front_on_the_bulk_path(monkeypatch) -> None:
    db = _DB()
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager(db))
    gc = GraphContainer(vertices={"Product": [{"sku": "P"}]})

    with pytest.raises(ValueError, match="union-merged list fields"):
        asyncio.run(
            _writer(_schema()).write(
                gc, ARANGO, None, bulk_session_id="s", stamp={ZONES: [3]}
            )
        )


def test_a_union_field_on_an_edge_is_refused() -> None:
    schema = _schema()
    edge = next(iter(schema.core_schema.edge_config.values()))
    edge.properties.append(
        Field(name="tags", type="LIST", item_type="STRING", merge="union")
    )
    gc = GraphContainer(
        edges={("Ticket", "Product", "about"): [({"key": "SUP-1"}, {"sku": "P"}, {})]}
    )

    with pytest.raises(ValueError, match="not written on edges"):
        asyncio.run(_writer(schema).write(gc, NEO4J, None, stamp={ZONES: [1]}))


def test_a_reserved_field_cannot_be_an_identity() -> None:
    with pytest.raises(ValueError, match="cannot be part of its identity"):
        Vertex(
            name="Bad", properties=[Field(name="tag", reserved=True)], identity=["tag"]
        )


def test_a_name_reserved_on_one_type_and_readable_on_another_is_refused() -> None:
    raw = _schema().to_dict()
    note = next(
        v
        for v in raw["core_schema"]["vertex_config"]["vertices"]
        if v["name"] == "Note"
    )
    note["properties"].append({"name": ZONES, "type": "LIST", "item_type": "INT"})

    with pytest.raises(
        ValueError, match="reserved on .* but readable on vertex 'Note'"
    ):
        Schema.model_validate(raw)


@pytest.mark.parametrize("key", [ZONES, f"Ticket@{ZONES}"])
def test_an_edge_attribute_copying_a_reserved_field_is_refused(key) -> None:
    schema = _schema()
    schema.core_schema.edge_config.edges.append(
        Edge(source="Note", target="Product", relation="mentions")
    )
    schema.finish_init()
    gc = GraphContainer(
        edges={
            ("Note", "Product", "mentions"): [({"id": "n"}, {"sku": "P"}, {key: [9]})]
        }
    )

    with pytest.raises(ValueError, match="a copy of a reserved field"):
        asyncio.run(_writer(schema).write(gc, NEO4J, None, stamp={ZONES: [1]}))


def test_a_lifted_edge_may_carry_its_own_reserved_attribute() -> None:
    gc = GraphContainer(
        edges={
            ("Ticket", "Product", "about"): [
                ({"key": "SUP-1"}, {"sku": "P"}, {ZONES: [9]})
            ]
        }
    )

    refuse_reserved_attributes(gc, _schema())
