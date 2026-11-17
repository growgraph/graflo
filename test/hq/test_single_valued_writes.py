"""Writing ``merge: replace`` and ``exclusive`` edges: one target per source."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.architecture.refusal import WriteRefusal
from graflo.architecture.schema import Schema
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.connections.onto import (
    ArangoConfig,
    DBConfig,
    FalkordbConfig,
    MemgraphConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    TigergraphConfig,
)
from graflo.db.arango.conn import arango_edge_targets_query
from graflo.db.cypher.targets import relationship_targets, relationship_targets_query
from graflo.db.manager import ConnectionManager
from graflo.db.postgres.target_write import select_edge_targets_query
from graflo.hq.db_writer import DBWriter, single_valued_mode
from graflo.hq.graph_engine import GraphEngine
from graflo.onto import DBType

RUNS = ("Unit", "Release", "runs")
MODEL = ("Unit", "Model", "instanceOf")
OWNS = ("Unit", "Part", "has")


def _schema() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "fleet"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Unit",
                            "properties": ["serial"],
                            "identity": ["serial"],
                        },
                        {
                            "name": "Release",
                            "properties": ["release_id"],
                            "identity": ["release_id"],
                        },
                        {"name": "Model", "properties": ["sku"], "identity": ["sku"]},
                        {"name": "Part", "properties": ["pn"], "identity": ["pn"]},
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "Unit",
                            "target": "Release",
                            "relation": "runs",
                            "properties": ["since"],
                            "merge": "replace",
                        },
                        {
                            "source": "Unit",
                            "target": "Model",
                            "relation": "instanceOf",
                            "merge": "replace",
                            "exclusive": True,
                        },
                        {"source": "Unit", "target": "Part", "relation": "has"},
                    ]
                },
            },
        }
    )


def _writer(schema: Schema) -> DBWriter:
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    return DBWriter(schema=schema, ingestion_model=model)


def _container(edges: dict[tuple, list[tuple[str, str, dict]]]) -> GraphContainer:
    """Edges by id as ``(serial, target id, properties)``, with their vertices."""
    field = {"Release": "release_id", "Model": "sku", "Part": "pn"}
    vertices: dict[str, list[dict]] = {"Unit": []}
    rows: dict[tuple, list] = {}
    for edge_id, triples in edges.items():
        target_type = edge_id[1]
        for serial, target, properties in triples:
            vertices["Unit"].append({"serial": serial})
            vertices.setdefault(target_type, []).append({field[target_type]: target})
            rows.setdefault(edge_id, []).append(
                ({"serial": serial}, {field[target_type]: target}, dict(properties))
            )
    return GraphContainer(vertices=vertices, edges=rows)


def _write(writer: DBWriter, conn_conf: DBConfig, edges: dict) -> None:
    asyncio.run(
        writer.write(gc=_container(edges), conn_conf=conn_conf, resource_name=None)
    )


# -- the file backend: folded on read --------------------------------------------


@pytest.fixture
def file_target(tmp_path: Path) -> tuple[DBWriter, GraFloBackendConfig]:
    schema = _schema()
    conn_conf = GraFloBackendConfig(output_dir=tmp_path)
    GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND).define_schema(
        manifest=GraphManifest(
            graph_schema=schema, ingestion_model=IngestionModel(resources=[])
        ),
        target_db_config=conn_conf,
    )
    return _writer(schema), conn_conf


def _stored(conn_conf: DBConfig, edge_id: tuple) -> list[tuple[str, str]]:
    with ConnectionManager(connection_config=conn_conf) as conn:
        records = conn.fetch_all_edges(*edge_id)
    return sorted((s["serial"], next(iter(t.values()))) for s, t, *_ in records)


def test_a_newer_target_replaces_the_stored_one(file_target) -> None:
    writer, conn_conf = file_target
    _write(writer, conn_conf, {RUNS: [("U1", "R1", {"since": 1})]})
    _write(writer, conn_conf, {RUNS: [("U1", "R2", {"since": 2})]})

    assert _stored(conn_conf, RUNS) == [("U1", "R2")]
    with ConnectionManager(connection_config=conn_conf) as conn:
        walked = conn.fetch_edges("Unit", "U1", edge_type="runs", to_type="Release")
        exported = conn.export_graph_container(_schema())
    assert [row["_to_key"] for row in walked] == ["R2"]
    assert [(s["serial"], t["release_id"]) for s, t, *_ in exported.edges[RUNS]] == [
        ("U1", "R2")
    ]


def test_the_last_target_in_a_batch_wins(file_target) -> None:
    writer, conn_conf = file_target
    _write(
        writer,
        conn_conf,
        {
            RUNS: [
                ("U1", "R1", {"since": 1}),
                ("U2", "R1", {"since": 1}),
                ("U1", "R2", {"since": 2}),
            ]
        },
    )

    assert _stored(conn_conf, RUNS) == [("U1", "R2"), ("U2", "R1")]


def test_a_target_written_again_is_an_update(file_target) -> None:
    writer, conn_conf = file_target
    _write(writer, conn_conf, {RUNS: [("U1", "R1", {"since": 1})]})
    _write(writer, conn_conf, {RUNS: [("U1", "R1", {"since": 5})]})

    with ConnectionManager(connection_config=conn_conf) as conn:
        ((_, _, properties),) = conn.fetch_all_edges(*RUNS)
    assert properties["since"] == 5


def test_an_exclusive_edge_refuses_a_second_target(file_target) -> None:
    writer, conn_conf = file_target
    _write(writer, conn_conf, {MODEL: [("U1", "M1", {})]})

    with pytest.raises(WriteRefusal) as refused:
        _write(writer, conn_conf, {MODEL: [("U1", "M2", {}), ("U2", "M2", {})]})

    assert refused.value.check == "exclusive-conflict"
    assert "{'sku': 'M1'}, refused {'sku': 'M2'}" in str(refused.value)
    # The stored target stays; an edge that conflicts with nothing is written.
    assert _stored(conn_conf, MODEL) == [("U1", "M1"), ("U2", "M2")]


def test_an_exclusive_edge_takes_its_own_target_again(file_target) -> None:
    writer, conn_conf = file_target
    _write(writer, conn_conf, {MODEL: [("U1", "M1", {})]})
    _write(writer, conn_conf, {MODEL: [("U1", "M1", {})]})

    assert _stored(conn_conf, MODEL) == [("U1", "M1")]


def test_an_exclusive_edge_keeps_the_first_target_of_a_batch(file_target) -> None:
    writer, conn_conf = file_target

    with pytest.raises(WriteRefusal, match="refused"):
        _write(writer, conn_conf, {MODEL: [("U1", "M1", {}), ("U1", "M2", {})]})

    assert _stored(conn_conf, MODEL) == [("U1", "M1")]


def test_appended_records_fold_to_one_target_per_source(file_target) -> None:
    """Records written past the writer fold on read: last for replace, first for exclusive."""
    _, conn_conf = file_target
    with ConnectionManager(connection_config=conn_conf) as conn:
        conn.insert_edges_batch(
            [({"serial": "U1"}, {"release_id": r}, {}) for r in ("R1", "R2", "R3")],
            "Unit",
            "Release",
            "runs",
            ("serial",),
            ("release_id",),
        )
        conn.insert_edges_batch(
            [({"serial": "U1"}, {"sku": m}, {}) for m in ("M1", "M2")],
            "Unit",
            "Model",
            "instanceOf",
            ("serial",),
            ("sku",),
        )

    assert _stored(conn_conf, RUNS) == [("U1", "R3")]
    assert _stored(conn_conf, MODEL) == [("U1", "M1")]


def test_a_union_edge_still_accumulates(file_target) -> None:
    writer, conn_conf = file_target
    _write(writer, conn_conf, {OWNS: [("U1", "P1", {})]})
    _write(writer, conn_conf, {OWNS: [("U1", "P2", {})]})

    assert _stored(conn_conf, OWNS) == [("U1", "P1"), ("U1", "P2")]


# -- a deleting target -----------------------------------------------------------


class _Graph:
    """Edges by stored type, as ``(source, target, properties)``."""

    def __init__(self) -> None:
        self.edges: dict[tuple, list[tuple[dict, dict, dict]]] = {}
        self.deleted: list[tuple[dict, dict]] = []
        self.reads = 0


def _connection_class(graph: _Graph, *, delete: bool = True, read: bool = True) -> type:
    class _Connection:
        supports_instance_delete = delete
        supports_edge_target_read = read

        def __init__(self, config: Any) -> None:
            self.config = config

        def close(self) -> None:
            pass

        def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs) -> None:
            pass

        def insert_edges_batch(self, docs_edges, source_class, target_class, **kw):
            """An upsert on the endpoints, as a database's edge merge is."""
            stored = graph.edges.setdefault(
                (source_class, target_class, kw["relation_name"]), []
            )
            for s, t, p, *_ in docs_edges:
                stored[:] = [row for row in stored if (row[0], row[1]) != (s, t)]
                stored.append((dict(s), dict(t), dict(p)))

        def fetch_edge_targets(
            self, source_class, target_class, relation_name, sources, ks, kt, **kw
        ):
            graph.reads += 1
            stored = graph.edges.get((source_class, target_class, relation_name), [])
            found: dict[int, list[dict]] = {}
            for position, doc in enumerate(sources):
                for s, t, _ in stored:
                    if all(s[k] == doc[k] for k in ks):
                        found.setdefault(position, []).append({k: t[k] for k in kt})
            return found

        def delete_edges(
            self, source_class, target_class, relation_name, endpoints, ks, kt, **kw
        ):
            key = (source_class, target_class, relation_name)
            graph.deleted.extend(endpoints)
            graph.edges[key] = [
                (s, t, p)
                for s, t, p in graph.edges.get(key, [])
                if not any(
                    all(s[k] == a[k] for k in ks) and all(t[k] == b[k] for k in kt)
                    for a, b in endpoints
                )
            ]

    return _Connection


NEO4J = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")


@pytest.fixture
def graph(monkeypatch) -> _Graph:
    graph = _Graph()
    monkeypatch.setitem(
        ConnectionManager.target_conn_mapping, DBType.NEO4J, _connection_class(graph)
    )
    return graph


def _targets(graph: _Graph, relation: str) -> list[tuple[str, str]]:
    return sorted(
        (s["serial"], next(iter(t.values())))
        for (_, _, rel), rows in graph.edges.items()
        if rel == relation
        for s, t, _ in rows
    )


def test_a_deleting_target_removes_the_old_target_before_the_insert(graph) -> None:
    writer = _writer(_schema())
    _write(writer, NEO4J, {RUNS: [("U1", "R1", {}), ("U2", "R1", {})]})
    _write(writer, NEO4J, {RUNS: [("U1", "R2", {}), ("U2", "R1", {})]})

    assert _targets(graph, "runs") == [("U1", "R2"), ("U2", "R1")]
    assert graph.deleted == [({"serial": "U1"}, {"release_id": "R1"})]
    # One read per batch, for every source in it.
    assert graph.reads == 2


def test_a_deleting_target_keeps_the_last_target_of_a_batch(graph) -> None:
    writer = _writer(_schema())
    _write(writer, NEO4J, {RUNS: [("U1", "R1", {}), ("U1", "R2", {})]})

    assert _targets(graph, "runs") == [("U1", "R2")]


def test_a_deleting_target_refuses_a_second_exclusive_target(graph) -> None:
    writer = _writer(_schema())
    _write(writer, NEO4J, {MODEL: [("U1", "M1", {})]})

    with pytest.raises(WriteRefusal) as refused:
        _write(writer, NEO4J, {MODEL: [("U1", "M2", {})], RUNS: [("U1", "R1", {})]})

    assert refused.value.check == "exclusive-conflict"
    assert refused.value.subjects == (
        (
            "edge ('Unit', 'Model', 'instanceOf'): Unit {'serial': 'U1'} holds "
            "Model {'sku': 'M1'}, refused {'sku': 'M2'}"
        ),
    )
    assert _targets(graph, "instanceOf") == [("U1", "M1")]
    assert graph.deleted == []
    # The rest of the batch is written.
    assert _targets(graph, "runs") == [("U1", "R1")]


def test_a_union_edge_is_neither_read_nor_deleted(graph) -> None:
    writer = _writer(_schema())
    _write(writer, NEO4J, {OWNS: [("U1", "P1", {})]})
    _write(writer, NEO4J, {OWNS: [("U1", "P2", {})]})

    assert _targets(graph, "has") == [("U1", "P1"), ("U1", "P2")]
    assert (graph.reads, graph.deleted) == (0, [])


@pytest.mark.parametrize(
    ("delete", "read"), [(False, False), (True, False), (False, True)]
)
def test_a_target_that_can_neither_delete_nor_fold_refuses_before_writing(
    monkeypatch, delete: bool, read: bool
) -> None:
    graph = _Graph()
    monkeypatch.setitem(
        ConnectionManager.target_conn_mapping,
        DBType.NEO4J,
        _connection_class(graph, delete=delete, read=read),
    )
    writer = _writer(_schema())

    with pytest.raises(ValueError, match="deleting vertices and edges and reading"):
        _write(writer, NEO4J, {RUNS: [("U1", "R1", {})], OWNS: [("U1", "P1", {})]})

    assert graph.edges == {}


def test_union_edges_need_no_capability(monkeypatch) -> None:
    graph = _Graph()
    monkeypatch.setitem(
        ConnectionManager.target_conn_mapping,
        DBType.NEO4J,
        _connection_class(graph, delete=False, read=False),
    )
    _write(_writer(_schema()), NEO4J, {OWNS: [("U1", "P1", {})]})

    assert _targets(graph, "has") == [("U1", "P1")]


# -- what each backend does ------------------------------------------------------


def _edge(relation: str):
    return _schema().core_schema.edge_config.edge_for(
        {"runs": RUNS, "instanceOf": MODEL, "has": OWNS}[relation]
    )


@pytest.mark.parametrize(
    ("conn_conf", "mode"),
    [
        (
            ArangoConfig(uri="http://localhost:8529", username="u", password="p"),
            "delete",
        ),
        (NEO4J, "delete"),
        (MemgraphConfig(uri="bolt://localhost:7687"), "delete"),
        (FalkordbConfig(uri="redis://localhost:6379"), "delete"),
        (
            PostgresConfig(
                uri="postgresql://localhost:5432/db",
                username="u",
                password="p",
                database="db",
            ),
            "delete",
        ),
        (GraFloBackendConfig(output_dir=Path("unused")), "fold"),
    ],
)
def test_how_each_target_keeps_a_replace_edge(conn_conf: DBConfig, mode: str) -> None:
    assert single_valued_mode(_edge("runs"), conn_conf) == mode
    assert single_valued_mode(_edge("instanceOf"), conn_conf) == mode
    assert single_valued_mode(_edge("has"), conn_conf) is None


@pytest.mark.parametrize(
    "conn_conf",
    [
        NebulaConfig(uri="http://localhost:9669"),
        TigergraphConfig(uri="http://localhost:14240"),
    ],
)
def test_targets_that_cannot_read_edge_targets_refuse_replace_edges(
    conn_conf: DBConfig,
) -> None:
    with pytest.raises(ValueError, match="merge 'replace' holds one target per source"):
        single_valued_mode(_edge("runs"), conn_conf)
    assert single_valued_mode(_edge("has"), conn_conf) is None


# -- the reads behind it -----------------------------------------------------------


def test_the_cypher_read() -> None:
    assert relationship_targets_query(
        "Unit", "Release", "runs", ["serial"], ["release_id", "channel"]
    ) == (
        "UNWIND $data AS row MATCH (s:`Unit`)-[r:`runs`]->(t:`Release`) "
        "WHERE s.`serial` = row.s0 "
        "RETURN row.i AS i, t.`release_id` AS t0, t.`channel` AS t1"
    )


def test_the_cypher_read_maps_rows_back_to_their_sources() -> None:
    calls: list[list[dict]] = []

    def read(query: str, rows: list[dict]) -> list[list[Any]]:
        calls.append(rows)
        return [[row["i"], f"R-{row['s0']}"] for row in rows]

    found = relationship_targets(
        read,
        "Unit",
        "Release",
        "runs",
        [{"serial": "U1"}, {"name": "no key"}, {"serial": "U3"}],
        ["serial"],
        ["release_id"],
        chunk_size=1,
    )

    assert found == {0: [{"release_id": "R-U1"}], 2: [{"release_id": "R-U3"}]}
    assert calls == [[{"i": 0, "s0": "U1"}], [{"i": 2, "s0": "U3"}]]


def test_the_arango_read() -> None:
    assert arango_edge_targets_query(
        "Unit_Release_edges", "Unit", ["serial"], with_relation=True
    ) == (
        "FOR row IN @rows FOR s IN Unit FILTER s.`serial` == row.s.`serial` "
        "FOR e IN Unit_Release_edges FILTER e._from == s._id AND e.relation == @relation "
        "FILTER IS_SAME_COLLECTION(@target, e._to) "
        "LET t = DOCUMENT(e._to) FILTER t != null "
        "RETURN {i: row.i, t: KEEP(t, @target_keys)}"
    )


def test_the_postgres_read() -> None:
    assert select_edge_targets_query(
        "public", "unit_runs_release", ["source_id"], ["target_id"]
    ) == (
        'SELECT "source_id", "target_id" FROM "public"."unit_runs_release" '
        'WHERE ("source_id") IN (VALUES %s)'
    )


def test_the_file_backend_reads_targets_by_source(file_target) -> None:
    writer, conn_conf = file_target
    _write(writer, conn_conf, {OWNS: [("U1", "P1", {}), ("U1", "P2", {})]})

    with ConnectionManager(connection_config=conn_conf) as conn:
        found = conn.fetch_edge_targets(
            "Unit",
            "Part",
            "has",
            [{"serial": "U9"}, {"serial": "U1"}],
            ("serial",),
            ("pn",),
        )
    assert found == {1: [{"pn": "P1"}, {"pn": "P2"}]}


def test_the_direction_read_is_unchanged(file_target) -> None:
    writer, conn_conf = file_target
    _write(writer, conn_conf, {RUNS: [("U1", "R1", {})]})
    with ConnectionManager(connection_config=conn_conf) as conn:
        rows = conn.fetch_edges(
            "Release", "R1", edge_type="runs", direction=EdgeDirection.IN
        )
    assert [row["_from_key"] for row in rows] == ["U1"]
