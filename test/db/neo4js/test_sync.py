"""Record sync on Neo4j: a sync of B onto A equals a bulk load of B.

Neo4j Community has one database, so every target is that database, emptied.
"""

from __future__ import annotations

from graflo.connections.onto import Neo4jConfig
from graflo.db.manager import ConnectionManager
from graflo.db.neo4j.conn import Neo4jConnection
from test.sync.live import (
    assert_sync_after_upsert_rewrites_every_owner,
    assert_update_equals_rebuild,
)


def _fresh(label: str) -> Neo4jConfig:
    conf = Neo4jConfig.from_docker_env()
    with ConnectionManager(connection_config=conf) as db:
        db.delete_graph_structure(delete_all=True)
    return conf


def test_update_equals_rebuild_and_a_second_sync_writes_nothing(
    tmp_path, monkeypatch
) -> None:
    assert_update_equals_rebuild(_fresh, tmp_path, Neo4jConnection, monkeypatch)


def test_a_sync_onto_an_upserted_graph_rewrites_it_to_b(tmp_path) -> None:
    assert_sync_after_upsert_rewrites_every_owner(_fresh, tmp_path)
