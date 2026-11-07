"""The bulk contract on Neo4j, and resume after an interrupted edge write.

Neo4j Community has one database, so every target is that database, emptied.
"""

from __future__ import annotations

import pytest

from graflo.connections.onto import Neo4jConfig
from graflo.db.manager import ConnectionManager
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest


def _fresh(label: str) -> Neo4jConfig:
    conf = Neo4jConfig.from_docker_env()
    with ConnectionManager(connection_config=conf) as db:
        db.delete_graph_structure(delete_all=True)
    return conf


def test_a_bulk_load_holds_the_graph_an_upsert_holds(tmp_path) -> None:
    assert_bulk_equals_upsert(_fresh, tmp_path / "staging")


def test_an_interrupted_bulk_write_resumes_to_the_same_graph(
    tmp_path, monkeypatch
) -> None:
    conf = _fresh("upsert")
    ingest(conf, IngestionParams(batch_size=2))
    upserted = export(conf)
    conf = _fresh("bulk")
    params = IngestionParams(
        mode="bulk", staging_dir=tmp_path / "staging", batch_size=2
    )
    real = Neo4jConnection.bulk_write_edges

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(Neo4jConnection, "bulk_write_edges", _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(conf, params)
    monkeypatch.setattr(Neo4jConnection, "bulk_write_edges", real)
    ingest(conf, params)

    assert export(conf) == upserted
