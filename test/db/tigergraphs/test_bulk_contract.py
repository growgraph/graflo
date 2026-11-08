"""The bulk contract on a live TigerGraph, resume, and edge types sharing a relation.

TigerGraph's upsert writes no ``merge: union`` field, so the contract manifest
is run without the union.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.connections.onto import TigergraphConfig
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.db.manager import ConnectionManager
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.write_prepare import WriteContext, project_edges
from test.bulk import contract
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest
from test.db.backends import config_for
from test.db.tigergraphs.conftest import (
    TEST_GRAPH_PREFIX,
    _delete_test_graph_best_effort,
)

# Every test here needs a live TigerGraph, whose schema DDL runs 15-40s per graph.
pytestmark = pytest.mark.tigergraph


@pytest.fixture(autouse=True)
def tigergraph_contract(monkeypatch) -> None:
    monkeypatch.setattr(contract, "MANIFEST", contract.without_union(contract.MANIFEST))


@pytest.fixture
def fresh_graphs(
    tg_test_graph_registry: set[str],
) -> Iterator[Callable[[str], TigergraphConfig]]:
    """New, empty graphs, one per label, deleted after the test."""
    made: list[TigergraphConfig] = []

    def fresh(label: str) -> TigergraphConfig:
        name = f"{TEST_GRAPH_PREFIX}bulk_{label}_{uuid.uuid4().hex[:8]}"
        conf = config_for("tigergraph", space=name)
        if not conf.password:
            conf.password = os.environ.get("GSQL_PASSWORD", "tigergraph")
        tg_test_graph_registry.add(name)
        made.append(conf)
        return conf

    yield fresh
    for conf in made:
        assert conf.database is not None
        _delete_test_graph_best_effort(conf, conf.database)


def test_a_bulk_load_holds_the_graph_an_upsert_holds(
    fresh_graphs: Callable[[str], TigergraphConfig], tmp_path: Path
) -> None:
    graph = assert_bulk_equals_upsert(fresh_graphs, tmp_path / "staging")
    assert len(graph[str(("Person", "City", "lives_in"))]) == 5


def test_an_interrupted_bulk_write_resumes_to_the_same_graph(
    fresh_graphs: Callable[[str], TigergraphConfig],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upsert_conf = fresh_graphs("upsert")
    ingest(upsert_conf, IngestionParams(batch_size=2))
    bulk_conf = fresh_graphs("bulk")
    params = IngestionParams(
        mode="bulk", staging_dir=tmp_path / "staging", batch_size=2
    )

    real = TigerGraphConnection.bulk_write_edges

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(TigerGraphConnection, "bulk_write_edges", _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(bulk_conf, params)
    monkeypatch.setattr(TigerGraphConnection, "bulk_write_edges", real)
    ingest(bulk_conf, params)

    assert export(bulk_conf) == export(upsert_conf)


# -- edge types sharing a relation --------------------------------------------

SHARED: dict[str, Any] = {
    "schema": {
        "metadata": {"name": "bulk_shared", "version": "1.0.0"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {"name": "Person", "properties": ["pid", "name"], "identity": ["pid"]},
                    {"name": "Robot", "properties": ["rid", "model"], "identity": ["rid"]},
                    {"name": "City", "properties": ["cid"], "identity": ["cid"]},
                ]
            },
            "edge_config": {
                "edges": [
                    {"source": "Person", "target": "City", "relation": "lives_in",
                     "properties": ["since"]},
                    {"source": "Robot", "target": "City", "relation": "lives_in",
                     "properties": ["unit"]},
                ]
            },
        },
    },
    "ingestion_model": {
        "resources": [
            {
                "name": "people",
                "apply": [
                    {"vertex": "Person", "from": {"pid": "pid", "name": "name"}},
                    {"vertex": "City", "from": {"cid": "cid"}},
                    {"edge": {"from": "Person", "to": "City", "relation": "lives_in",
                              "properties": ["since"]}},
                ],
            },
            {
                "name": "robots",
                "apply": [
                    {"vertex": "Robot", "from": {"rid": "rid", "model": "model"}},
                    {"vertex": "City", "from": {"cid": "cid"}},
                    {"edge": {"from": "Robot", "to": "City", "relation": "lives_in",
                              "properties": ["unit"]}},
                ],
            },
        ],
        "transforms": [],
    },
}  # fmt: skip

PEOPLE = [
    {"pid": "p1", "name": "Ann", "cid": "c1", "since": "2001"},
    {"pid": "p2", "name": "Bo", "cid": "c2", "since": "2010"},
]
ROBOTS = [
    {"rid": "r1", "model": "T1", "cid": "c1", "unit": "u7"},
    {"rid": "r2", "model": "T2", "cid": "c1", "unit": "u8"},
    {"rid": "r3", "model": "T2", "cid": "c3", "unit": "u9"},
]


def _shared_manifest() -> GraphManifest:
    out = GraphManifest.from_config(json.loads(json.dumps(SHARED)))
    out.finish_init()
    return out


def _ingest_shared(conf: TigergraphConfig, params: IngestionParams) -> None:
    registry = DataSourceRegistry()
    for name, rows in (("people", PEOPLE), ("robots", ROBOTS)):
        registry.register(
            DataSourceFactory.create_in_memory_data_source(
                data=[dict(r) for r in rows]
            ),
            resource_name=name,
        )
    engine = GraphEngine(target_db_flavor=conf.connection_type)
    manifest = _shared_manifest()
    if params.mode != "bulk":
        engine.define_schema(manifest, conf)
    engine.ingest(manifest, conf, params, data_source_registry=registry)


def _export_shared(conf: TigergraphConfig) -> dict[str, list[str]]:
    schema = _shared_manifest().require_schema()
    ctx = WriteContext.for_flavor(schema, conf.connection_type)
    out: dict[str, list[str]] = {}
    with ConnectionManager(connection_config=conf) as db:
        for vertex in schema.core_schema.vertex_config.vertex_set:
            docs = db.fetch_all_docs(ctx.db_aware.vertex_config.vertex_dbname(vertex))
            out[vertex] = sorted(json.dumps(d, sort_keys=True) for d in docs)
        for edge in schema.core_schema.edge_config.values():
            stored = project_edges(ctx, edge, edge.relation, [])
            rows = db.fetch_all_edges(
                stored.source_class,
                stored.target_class,
                stored.relation_name,
                collection_name=stored.collection_name,
            )
            out[str(edge.edge_id)] = sorted(json.dumps(r, sort_keys=True) for r in rows)
    return out


def test_edge_types_sharing_a_relation_load_per_endpoint_pair(
    fresh_graphs: Callable[[str], TigergraphConfig], tmp_path: Path
) -> None:
    upsert_conf = fresh_graphs("upsert")
    _ingest_shared(upsert_conf, IngestionParams(batch_size=2))
    upserted = _export_shared(upsert_conf)

    bulk_conf = fresh_graphs("bulk")
    _ingest_shared(
        bulk_conf,
        IngestionParams(mode="bulk", staging_dir=tmp_path / "staging", batch_size=2),
    )
    assert _export_shared(bulk_conf) == upserted

    people = [json.loads(r) for r in upserted[str(("Person", "City", "lives_in"))]]
    robots = [json.loads(r) for r in upserted[str(("Robot", "City", "lives_in"))]]
    assert sorted(e[2]["since"] for e in people) == ["2001", "2010"]
    assert sorted(e[2]["unit"] for e in robots) == ["u7", "u8", "u9"]


def test_a_bulk_load_refuses_a_graph_that_holds_a_schema(
    fresh_graphs: Callable[[str], TigergraphConfig], tmp_path: Path
) -> None:
    conf = fresh_graphs("held")
    ingest(conf, IngestionParams())
    with pytest.raises(Exception, match="already exists"):
        ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))
