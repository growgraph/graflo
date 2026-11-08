"""Record sync, end to end on an in-memory graph: update(A→B) equals rebuild(B).

Release B differs from A by a removed reference, a nulled field, a dropped
author, a changed author record, a new work, a record repeated in a later batch
and a work left as it was. Syncing B onto a load of A must give the graph a
bulk load of B gives, sync hashes included; syncing B again rewrites nothing.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects import plan_lift
from graflo.connections.onto import Neo4jConfig
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.db.manager import ConnectionManager
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.onto import DBType
from test.sync.memory_graph import MemoryGraph

MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": "works", "version": "1.0.0"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Work",
                        "properties": ["wid", "title"],
                        "identity": ["wid"],
                    },
                    {"name": "Topic", "properties": ["tid"], "identity": ["tid"]},
                    {
                        "name": "Author",
                        "properties": ["aid", "name"],
                        "identity": ["aid"],
                    },
                    {"name": "Place", "properties": ["pid"], "identity": ["pid"]},
                ]
            },
            "edge_config": {
                "edges": [
                    {"source": "Work", "target": "Topic", "relation": "about"},
                    {"source": "Author", "target": "Work", "relation": "wrote"},
                    {
                        "source": "Author",
                        "target": "Place",
                        "relation": "at",
                        "properties": ["work"],
                        "identities": [["source", "target", "relation", "work"]],
                    },
                ]
            },
        },
    },
    "ingestion_model": {
        "resources": [
            {
                "name": "works",
                "infer_edges": False,
                "apply": [
                    {"vertex": "Work", "from": {"wid": "wid", "title": "title"}},
                    {
                        "descend": {
                            "key": "topics",
                            "pipeline": [{"vertex": "Topic", "from": {"tid": "tid"}}],
                        }
                    },
                    {
                        "descend": {
                            "key": "authorships",
                            "pipeline": [
                                {"vertex": "Author", "from": {"aid": "aid"}},
                                {"vertex": "Place", "from": {"pid": "pid"}},
                                {
                                    "edge": {
                                        "from": "Author",
                                        "to": "Place",
                                        "relation": "at",
                                        "properties": ["work"],
                                    }
                                },
                            ],
                        }
                    },
                    {"edge": {"from": "Work", "to": "Topic", "relation": "about"}},
                    {"edge": {"from": "Author", "to": "Work", "relation": "wrote"}},
                ],
                "sync": {
                    "owns": "Work",
                    "edges": [
                        {"source": "Work", "target": "Topic", "relation": "about"},
                        {"source": "Author", "target": "Work", "relation": "wrote"},
                        {
                            "source": "Author",
                            "target": "Place",
                            "relation": "at",
                            "owner_field": "work",
                        },
                    ],
                },
            },
            {
                "name": "authors",
                "infer_edges": False,
                "apply": [{"vertex": "Author", "from": {"aid": "aid", "name": "name"}}],
                "sync": {"owns": "Author"},
            },
        ],
        "transforms": [],
    },
}


def _work(
    wid: str, title: str | None, topics: list[str], authors: list[tuple[str, str]]
):
    return {
        "wid": wid,
        "title": title,
        "topics": [{"tid": t} for t in topics],
        "authorships": [{"aid": a, "pid": p, "work": wid} for a, p in authors],
    }


RELEASE_A: dict[str, list[dict[str, Any]]] = {
    "works": [
        _work("w1", "One", ["t1", "t2"], [("a1", "p1"), ("a2", "p1")]),
        _work("w2", "Two", ["t1"], [("a1", "p2")]),
        _work("w3", "Three", ["t3"], [("a3", "p3")]),
    ],
    "authors": [
        {"aid": "a1", "name": "Ann"},
        {"aid": "a2", "name": "Bo"},
        {"aid": "a3", "name": "Cy"},
    ],
}


def derive_release_b(
    a: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Release B from A: every kind of change a sync must replay exactly."""
    b = copy.deepcopy(a)
    works = {w["wid"]: w for w in b["works"]}
    works["w1"]["topics"] = [{"tid": "t1"}]  # a removed reference
    works["w1"]["authorships"] = works["w1"]["authorships"][:1]  # a dropped author
    works["w2"]["title"] = None  # a nulled field
    stale = copy.deepcopy(works["w2"])
    stale["title"] = "Two (stale)"
    # The record's earlier version comes first, the newer one in a later batch.
    b["works"] = [stale, works["w1"], works["w3"], works["w2"]]
    b["works"].append(_work("w4", "Four", ["t2"], [("a2", "p3")]))  # a new work
    b["authors"][1]["name"] = "Bob"  # a changed author record
    return b


@pytest.fixture
def memory(monkeypatch) -> None:
    MemoryGraph.graphs.clear()
    MemoryGraph.writes.clear()
    monkeypatch.setitem(
        ConnectionManager.target_conn_mapping, DBType.NEO4J, MemoryGraph
    )


def _manifest() -> GraphManifest:
    manifest = GraphManifest.from_config(copy.deepcopy(MANIFEST))
    manifest.finish_init()
    lifted = apply_evolution(manifest, plan_lift(manifest, aspects=["sync"]))
    lifted.finish_init()
    return lifted


def _run(uri: str, release: dict[str, list[dict[str, Any]]], params: IngestionParams):
    registry = DataSourceRegistry()
    for resource, rows in release.items():
        registry.register(
            DataSourceFactory.create_in_memory_data_source(data=copy.deepcopy(rows)),
            resource_name=resource,
        )
    conf = Neo4jConfig(uri=uri)
    GraphEngine(target_db_flavor=DBType.NEO4J).ingest(
        _manifest(), conf, params, data_source_registry=registry
    )
    return MemoryGraph.export(uri)


def _bulk(uri: str, release: dict, staging: Path):
    return _run(
        uri, release, IngestionParams(mode="bulk", staging_dir=staging, batch_size=2)
    )


def _sync(uri: str, release: dict, staging: Path):
    MemoryGraph.writes.clear()
    return _run(
        uri, release, IngestionParams(mode="sync", staging_dir=staging, batch_size=2)
    )


def test_a_sync_of_b_onto_a_equals_a_bulk_load_of_b(memory, tmp_path) -> None:
    b = derive_release_b(RELEASE_A)
    rebuilt = _bulk("bolt://rebuild:7687", b, tmp_path / "rebuild")

    _bulk("bolt://update:7687", RELEASE_A, tmp_path / "load")
    updated = _sync("bolt://update:7687", b, tmp_path / "sync")

    assert updated == rebuilt
    # w3 and authors a1, a3 did not change: they are not rewritten.
    assert sorted(MemoryGraph.writes) == sorted(
        ["Work:('w1',)", "Work:('w2',)", "Work:('w4',)", "Author:('a2',)"]
    )


def test_a_second_sync_writes_nothing(memory, tmp_path) -> None:
    b = derive_release_b(RELEASE_A)
    _bulk("bolt://g:7687", RELEASE_A, tmp_path / "load")
    first = _sync("bolt://g:7687", b, tmp_path / "sync1")

    again = _sync("bolt://g:7687", b, tmp_path / "sync2")

    assert again == first
    assert MemoryGraph.writes == []


def test_the_bulk_load_stores_every_owners_hash(memory, tmp_path) -> None:
    graph = _bulk("bolt://g:7687", RELEASE_A, tmp_path / "load")

    owners = [n for n in graph["nodes"] if n.startswith(('["Work"', '["Author"'))]
    assert owners and all('"sync_hash": "' in n for n in owners)
    assert not any("sync_hash" in n for n in graph["nodes"] if n.startswith('["Topic"'))
