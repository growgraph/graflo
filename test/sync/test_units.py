"""Record sync pieces: the hash, the statements of an owner chunk, the refusals."""

from __future__ import annotations

import copy
import datetime as dt

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects import plan_lift
from graflo.connections.onto import MemgraphConfig, Neo4jConfig
from graflo.db.cypher.sync import (
    changed_owners,
    delete_owned_query,
    owner_statements,
    replace_nodes_query,
    set_hash_query,
)
from graflo.db.manager import ConnectionManager
from graflo.db.sync import SyncEdgeType, SyncOwner, SyncOwnerChunk
from graflo.hq.bulk.run import open_staging
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.sync.hash import sync_hash
from graflo.hq.sync.refusals import sync_refusals
from graflo.onto import DBType
from test.sync.memory_graph import MemoryGraph
from test.sync.test_update_equals_rebuild import MANIFEST, RELEASE_A, _bulk, _sync

# -- the hash -----------------------------------------------------------------

EDGE = {"edge": ["W", "T", "about"], "source": {"w": 1}, "target": {"t": 2}, "doc": {}}


def test_the_hash_ignores_edge_order_and_the_fields_it_skips() -> None:
    other = {**EDGE, "target": {"t": 3}}
    doc = {"w": 1, "title": "x", "h": "old"}

    assert sync_hash(doc, [EDGE, other], skip=["h"]) == sync_hash(
        {**doc, "h": "new"}, [other, EDGE], skip=["h"]
    )


@pytest.mark.parametrize(
    ("doc", "edges"),
    [
        ({"w": 1, "title": "y"}, [EDGE]),
        ({"w": 1, "title": None}, [EDGE]),
        ({"w": 1}, [EDGE]),
        ({"w": 1, "title": "x"}, []),
        ({"w": 1, "title": "x"}, [{**EDGE, "doc": {"since": 2}}]),
        ({"w": 1, "title": "x"}, [{**EDGE, "edge": ["W", "T", "cites"]}]),
    ],
)
def test_any_change_to_the_owner_or_its_edges_moves_the_hash(doc, edges) -> None:
    assert sync_hash(doc, edges) != sync_hash({"w": 1, "title": "x"}, [EDGE])


def test_the_hash_reads_typed_values() -> None:
    a = sync_hash({"d": dt.date(2020, 1, 2)}, [])
    assert a == sync_hash({"d": dt.date(2020, 1, 2)}, [])
    assert a != sync_hash({"d": "2020-01-02"}, [])


# -- the statements of an owner chunk ------------------------------------------


def _chunk() -> SyncOwnerChunk:
    edge_types = [
        SyncEdgeType(
            source_class="Work",
            target_class="Topic",
            relation_name="about",
            match_keys_source=["wid"],
            match_keys_target=["tid"],
            side="source",
        ),
        SyncEdgeType(
            source_class="Author",
            target_class="Place",
            relation_name="at",
            match_keys_source=["aid"],
            match_keys_target=["pid"],
            side="field",
            owner_property="work",
        ),
    ]
    return SyncOwnerChunk(
        chunk_id="c",
        class_name="Work",
        match_keys=["wid"],
        hash_field="h",
        edge_types=edge_types,
        owners=[
            SyncOwner(
                doc={"wid": "w1", "title": "T", "h": "new"},
                edges=[[({"wid": "w1"}, {"tid": "t1"}, {})], []],
            ),
            SyncOwner(doc={"wid": "w2", "h": "same"}, edges=[[], []]),
        ],
    )


def test_only_owners_whose_stored_hash_differs_are_rewritten() -> None:
    chunk = _chunk()
    assert changed_owners(chunk, {1: "same"}) == [0]
    assert changed_owners(chunk, {0: "new", 1: "same"}) == []
    assert changed_owners(chunk, {0: None, 1: "same"}) == [0]


def test_a_rewrite_replaces_deletes_creates_and_stores_the_hash_last() -> None:
    chunk = _chunk()
    statements = owner_statements(chunk, [0])
    queries = [q for q, _ in statements]

    assert queries[0] == replace_nodes_query("Work", ["wid"])
    assert statements[0][1] == {"rows": [{"wid": "w1", "title": "T"}]}
    assert queries[1] == delete_owned_query(chunk.edge_types[0], ["wid"])
    assert statements[1][1] == {"rows": [{"wid": "w1"}]}
    assert "CREATE (source)-[r:about]->(target)" in queries[2]
    # The edge owned by a property is deleted by the owner's identity value;
    # the owner has none of it now, so none is created.
    assert queries[3] == delete_owned_query(chunk.edge_types[1], ["wid"])
    assert statements[3][1] == {"rows": ["w1"]}
    assert queries[-1] == set_hash_query("Work", ["wid"], "h")
    assert statements[-1][1] == {"rows": [{"key": {"wid": "w1"}, "h": "new"}]}
    assert owner_statements(chunk, []) == []


def test_owned_edges_are_found_by_their_owner() -> None:
    chunk = _chunk()
    assert delete_owned_query(chunk.edge_types[0], ["wid"]) == (
        "UNWIND $rows AS row MATCH (n:Work {`wid`: row.wid})-[r:about]->(:Topic) DELETE r"
    )
    assert delete_owned_query(chunk.edge_types[1], ["wid"]) == (
        "UNWIND $rows AS row MATCH (:Author)-[r:at]->(:Place) "
        "WHERE r.work = row DELETE r"
    )


# -- refusals -----------------------------------------------------------------


def _manifest(config: dict, *, lift: bool = True) -> GraphManifest:
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    if lift:
        manifest = apply_evolution(manifest, plan_lift(manifest, aspects=["sync"]))
        manifest.finish_init()
    return manifest


def _refusals(manifest: GraphManifest, resources: list[str] | None = None) -> list[str]:
    return sync_refusals(
        manifest.require_schema(), manifest.require_ingestion_model(), resources
    )


def test_a_consistent_manifest_can_sync() -> None:
    assert _refusals(_manifest(copy.deepcopy(MANIFEST))) == []


def test_an_owned_type_without_a_hash_field_is_refused() -> None:
    reasons = _refusals(_manifest(copy.deepcopy(MANIFEST), lift=False))
    lift = "has no field set by sync; lift the sync aspect (graflo lift --aspect sync)"
    assert reasons == [f"works: 'Work' {lift}", f"authors: 'Author' {lift}"]


def test_an_edge_no_sync_block_owns_is_refused() -> None:
    config = copy.deepcopy(MANIFEST)
    config["ingestion_model"]["resources"][0]["sync"]["edges"].pop(0)
    reasons = _refusals(_manifest(config))
    assert reasons == [
        "works: writes edge ('Work', 'Topic', 'about'), which no sync block owns"
    ]


def test_a_run_owning_nothing_is_refused() -> None:
    config = copy.deepcopy(MANIFEST)
    del config["ingestion_model"]["resources"][1]["sync"]
    assert _refusals(_manifest(config), ["authors"]) == [
        "no resource in the run declares a sync block"
    ]


def test_a_target_without_a_sync_writer_is_refused() -> None:
    with pytest.raises(ValueError, match="does not support record sync"):
        GraphEngine(target_db_flavor=DBType.MEMGRAPH).ingest(
            _manifest(copy.deepcopy(MANIFEST)),
            MemgraphConfig(uri="bolt://localhost:7687"),
            IngestionParams(mode="sync", staging_dir="unused"),
        )


def test_a_staging_directory_serves_one_mode(tmp_path) -> None:
    open_staging(tmp_path / "s", "bulk")
    with pytest.raises(ValueError, match="holds a bulk run"):
        open_staging(tmp_path / "s", "sync")


# -- resume -------------------------------------------------------------------


@pytest.fixture
def memory(monkeypatch) -> None:
    MemoryGraph.graphs.clear()
    MemoryGraph.writes.clear()
    monkeypatch.setitem(
        ConnectionManager.target_conn_mapping, DBType.NEO4J, MemoryGraph
    )


def test_a_stopped_sync_resumes_to_the_same_graph(
    memory, monkeypatch, tmp_path
) -> None:
    from test.sync.test_update_equals_rebuild import derive_release_b

    b = derive_release_b(RELEASE_A)
    expected = _bulk("bolt://rebuild:7687", b, tmp_path / "rebuild")
    _bulk("bolt://g:7687", RELEASE_A, tmp_path / "load")
    real = MemoryGraph.sync_write_owners

    def fail(self, schema, chunk):
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(MemoryGraph, "sync_write_owners", fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        _sync("bolt://g:7687", b, tmp_path / "sync")
    monkeypatch.setattr(MemoryGraph, "sync_write_owners", real)

    assert _sync("bolt://g:7687", b, tmp_path / "sync") == expected


def test_a_sync_into_an_unowned_type_with_properties_is_refused(
    memory, tmp_path
) -> None:
    config = copy.deepcopy(MANIFEST)
    config["ingestion_model"]["resources"][0]["apply"][2]["descend"]["pipeline"][0][
        "from"
    ]["name"] = "aname"
    del config["ingestion_model"]["resources"][1]["sync"]
    config["ingestion_model"]["resources"].pop(1)
    manifest = _manifest(config)
    release = copy.deepcopy(RELEASE_A["works"])
    for work in release:
        for authorship in work["authorships"]:
            authorship["aname"] = "N"

    from graflo.data_source.factory import DataSourceFactory
    from graflo.data_source.registry import DataSourceRegistry
    from graflo.hq.sync.run import SyncKeyOnlyError

    registry = DataSourceRegistry()
    registry.register(
        DataSourceFactory.create_in_memory_data_source(data=release),
        resource_name="works",
    )
    with pytest.raises(SyncKeyOnlyError, match=r"Author: \['name'\]"):
        GraphEngine(target_db_flavor=DBType.NEO4J).ingest(
            manifest,
            Neo4jConfig(uri="bolt://g:7687"),
            IngestionParams(mode="sync", staging_dir=tmp_path / "s"),
            data_source_registry=registry,
        )


def test_the_live_scenario_holds_on_the_in_memory_graph(
    memory, monkeypatch, tmp_path
) -> None:
    from test.sync.live import assert_update_equals_rebuild

    def fresh(label: str) -> Neo4jConfig:
        MemoryGraph.graphs.pop("bolt://live:7687", None)
        return Neo4jConfig(uri="bolt://live:7687")

    assert_update_equals_rebuild(fresh, tmp_path, MemoryGraph, monkeypatch)
