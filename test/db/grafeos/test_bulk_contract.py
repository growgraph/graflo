"""The bulk contract on Grafeo, and resume after an interrupted write."""

from __future__ import annotations

import pytest

from graflo.connections.grafeo import GrafeoConfig
from graflo.db.grafeo.conn import GrafeoConnection
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest


def _fresh(label: str) -> GrafeoConfig:
    conf = GrafeoConfig(database=f"gf_bulk_{label}")
    GrafeoConnection(conf).delete_database(conf.database or "")
    return conf


def test_a_bulk_load_holds_the_graph_an_upsert_holds(tmp_path) -> None:
    assert_bulk_equals_upsert(_fresh, tmp_path / "staging")


@pytest.mark.parametrize("phase", ["bulk_write_vertices", "bulk_write_edges"])
def test_an_interrupted_bulk_write_resumes_to_the_same_graph(
    tmp_path, monkeypatch, phase
) -> None:
    upsert_conf = _fresh("upsert")
    ingest(upsert_conf, IngestionParams(batch_size=2))
    bulk_conf = _fresh("bulk")
    params = IngestionParams(
        mode="bulk", staging_dir=tmp_path / "staging", batch_size=2
    )
    real = getattr(GrafeoConnection, phase)

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(GrafeoConnection, phase, _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(bulk_conf, params)
    monkeypatch.setattr(GrafeoConnection, phase, real)
    ingest(bulk_conf, params)

    assert export(bulk_conf) == export(upsert_conf)
