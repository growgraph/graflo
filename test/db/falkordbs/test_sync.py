"""Record sync on FalkorDB: a sync of B onto A equals a bulk load of B."""

from __future__ import annotations

from graflo.connections.onto import FalkordbConfig
from graflo.db.falkordb.conn import FalkordbConnection
from test.sync.live import (
    assert_sync_after_upsert_rewrites_every_owner,
    assert_update_equals_rebuild,
)


def _fresh(label: str) -> FalkordbConfig:
    conf = FalkordbConfig.from_docker_env()
    conf.database = f"gf_sync_{label}"
    conn = FalkordbConnection(conf)
    try:
        if conn._graph_exists(conf.database):
            conn.delete_database(conf.database)
    finally:
        conn.close()
    return conf


def test_update_equals_rebuild_and_a_second_sync_writes_nothing(
    tmp_path, monkeypatch
) -> None:
    assert_update_equals_rebuild(_fresh, tmp_path, FalkordbConnection, monkeypatch)


def test_a_sync_onto_an_upserted_graph_rewrites_it_to_b(tmp_path) -> None:
    assert_sync_after_upsert_rewrites_every_owner(_fresh, tmp_path)
