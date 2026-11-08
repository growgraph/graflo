"""The bulk contract on the file backend, and resume after an interrupted write."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.graflo_backend.connection import GraFloBackendConnection
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest


def _targets(tmp_path: Path):
    def fresh(label: str) -> GraFloBackendConfig:
        return GraFloBackendConfig(output_dir=tmp_path / label)

    return fresh


def test_a_bulk_load_holds_the_graph_an_upsert_holds(tmp_path) -> None:
    graph = assert_bulk_equals_upsert(_targets(tmp_path), tmp_path / "staging")

    people = [json.loads(d) for d in graph["Person"]]
    ann = next(p for p in people if p["pid"] == "p1")
    # The last row of p1 maps no name: the field is null, not absent.
    assert ann["name"] is None and ann["nick"] is None
    assert ann["tags"] == ["x", "z"]
    assert len(graph[str(("Person", "City", "lives_in"))]) == 5


def test_an_interrupted_bulk_write_resumes_to_the_same_graph(
    tmp_path, monkeypatch
) -> None:
    upsert_conf = GraFloBackendConfig(output_dir=tmp_path / "upsert")
    ingest(upsert_conf, IngestionParams(batch_size=2))
    bulk_conf = GraFloBackendConfig(output_dir=tmp_path / "bulk")
    params = IngestionParams(
        mode="bulk", staging_dir=tmp_path / "staging", batch_size=2
    )

    real = GraFloBackendConnection.bulk_write_edges

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(GraFloBackendConnection, "bulk_write_edges", _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(bulk_conf, params)
    monkeypatch.setattr(GraFloBackendConnection, "bulk_write_edges", real)
    ingest(bulk_conf, params)

    assert export(bulk_conf) == export(upsert_conf)


def test_a_bulk_load_refuses_a_target_that_holds_a_schema(tmp_path) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path / "db")
    ingest(conf, IngestionParams())
    with pytest.raises(Exception, match="already exists"):
        ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))
