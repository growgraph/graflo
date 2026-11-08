"""The bulk contract on Memgraph, and resume after an interrupted edge write.

Memgraph has one database, so every target is that database, emptied. Its
upsert writes no ``merge: union`` field, so the contract manifest is run
without the union.
"""

from __future__ import annotations

import pytest

from graflo.connections.onto import MemgraphConfig
from graflo.db.manager import ConnectionManager
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk import contract
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest


@pytest.fixture(autouse=True)
def memgraph_contract(monkeypatch) -> None:
    monkeypatch.setattr(contract, "MANIFEST", contract.without_union(contract.MANIFEST))


def _fresh(label: str) -> MemgraphConfig:
    conf = MemgraphConfig.from_docker_env()
    with ConnectionManager(connection_config=conf) as db:
        db.delete_graph_structure(delete_all=True)
    return conf


def test_a_bulk_load_holds_the_graph_an_upsert_holds(tmp_path) -> None:
    graph = assert_bulk_equals_upsert(_fresh, tmp_path / "staging")

    assert len(graph[str(("Person", "City", "lives_in"))]) == 5


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
    real = MemgraphConnection.bulk_write_edges

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(MemgraphConnection, "bulk_write_edges", _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(conf, params)
    monkeypatch.setattr(MemgraphConnection, "bulk_write_edges", real)
    ingest(conf, params)

    assert export(conf) == upserted
