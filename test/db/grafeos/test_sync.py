"""Record sync on Grafeo: a sync of B onto A equals a bulk load of B."""

from __future__ import annotations

from graflo.connections.grafeo import GrafeoConfig
from graflo.db.grafeo.conn import GrafeoConnection
from test.sync.live import (
    assert_sync_after_upsert_rewrites_every_owner,
    assert_update_equals_rebuild,
)


def _fresh(label: str) -> GrafeoConfig:
    conf = GrafeoConfig(database=f"gf_sync_{label}")
    GrafeoConnection(conf).delete_database(conf.database or "")
    return conf


def test_update_equals_rebuild_and_a_second_sync_writes_nothing(
    tmp_path, monkeypatch
) -> None:
    assert_update_equals_rebuild(_fresh, tmp_path, GrafeoConnection, monkeypatch)


def test_a_sync_onto_an_upserted_graph_rewrites_it_to_b(tmp_path) -> None:
    assert_sync_after_upsert_rewrites_every_owner(_fresh, tmp_path)
