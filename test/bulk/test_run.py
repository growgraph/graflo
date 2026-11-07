"""The bulk orchestrator: resume, replay of the chunk in doubt, count verification.

A fake target keeps what it is given in memory. A plain write appends, so a
chunk written twice without ``replay`` would double its rows and fail the
verification; a replayed chunk is merged.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, ClassVar

import pytest

from graflo.architecture.graph_types import GraphContainer
from graflo.connections.bulk import BulkConfig
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.bulk import BulkCounts, BulkEdgeChunk, BulkPlan, BulkVertexChunk
from graflo.hq.bulk.ledger import Phase, RunLedger
from graflo.hq.bulk.run import (
    BulkVerifyError,
    fold_staged,
    open_staging,
    write_folded,
)
from graflo.hq.bulk.stage import Stager
from graflo.hq.write_prepare import WriteContext
from graflo.onto import DBType
from test.bulk.test_fold import EDGE, SCHEMA

CFG = BulkConfig(chunk_size=2, parallelism=3)


class _Crash(RuntimeError):
    pass


class _Target:
    """What a fake bulk target holds, shared by every connection to it."""

    def __init__(self, *, crash_after: int | None = None, drop_one: bool = False):
        self.lock = threading.Lock()
        self.vertices: dict[str, list[dict[str, Any]]] = {}
        self.edges: dict[tuple, list[tuple]] = {}
        self.writes: list[str] = []
        self.replays: list[str] = []
        self.begun: list[tuple[bool, bool]] = []
        self.prepared = 0
        self.crash_after = crash_after
        self.drop_one = drop_one

    def _record(self, chunk: Any) -> None:
        with self.lock:
            self.writes.append(chunk.chunk_id)
            if chunk.replay:
                self.replays.append(chunk.chunk_id)
            if self.crash_after is not None and len(self.writes) > self.crash_after:
                self.crash_after = None
                # Written, then lost before the commit is recorded.
                raise _Crash(chunk.chunk_id)

    def write_vertices(self, chunk: BulkVertexChunk) -> None:
        rows = self.vertices.setdefault(chunk.class_name, [])
        with self.lock:
            for row in chunk.rows[1:] if self.drop_one else chunk.rows:
                if chunk.replay and row in rows:
                    continue
                rows.append(row)
            self.drop_one = False
        self._record(chunk)

    def write_edges(self, chunk: BulkEdgeChunk) -> None:
        rows = self.edges.setdefault(chunk.stored_id, [])
        with self.lock:
            for row in chunk.rows:
                if chunk.replay and row in rows:
                    continue
                rows.append(row)
        self._record(chunk)


class _FakeBulkConnection:
    supports_bulk_load: ClassVar[bool] = True
    bulk_dense_ids: ClassVar[bool] = False
    target: ClassVar[_Target]

    def bulk_begin(self, schema, cfg, *, recreate=False, resume=False) -> BulkPlan:
        self.target.begun.append((recreate, resume))
        return BulkPlan()

    def bulk_write_vertices(self, chunk: BulkVertexChunk) -> None:
        self.target.write_vertices(chunk)

    def bulk_prepare_edges(self, schema) -> None:
        self.target.prepared += 1

    def bulk_write_edges(self, chunk: BulkEdgeChunk) -> None:
        self.target.write_edges(chunk)

    def bulk_finish(self, schema) -> BulkCounts:
        return BulkCounts(
            vertices={k: len(v) for k, v in self.target.vertices.items()},
            edges={k: len(v) for k, v in self.target.edges.items()},
        )


def _manager(target: _Target) -> type:
    class _Conn(_FakeBulkConnection):
        pass

    _Conn.target = target

    class _Manager:
        @classmethod
        def require(cls, conn_conf, capability):
            return _Conn

        def __init__(self, connection_config) -> None:
            pass

        def __enter__(self) -> _Conn:
            return _Conn()

        def __exit__(self, *exc) -> bool:
            return False

    return _Manager


def _staged(root: Path) -> RunLedger:
    ledger = open_staging(root)
    stager = Stager(
        SCHEMA, WriteContext.for_flavor(SCHEMA, DBType.GRAFLO_BACKEND), root
    )
    stager.stage(
        GraphContainer(
            vertices={"V": [{"id": i, "a": i} for i in range(7)]},
            edges={EDGE: [({"id": i}, {"id": (i + 1) % 7}, {}) for i in range(7)]},
        ),
        (0, 0, 0, 0),
    )
    stager.stage(
        GraphContainer(vertices={"V": [{"id": 3, "a": "later"}]}), (0, 0, 1, 0)
    )
    ledger.mark(Phase.STAGED)
    fold_staged(root, ledger, CFG, dense_ids=False)
    return ledger


def _run(root: Path, target: _Target, monkeypatch) -> BulkCounts:
    monkeypatch.setattr("graflo.hq.bulk.run.ConnectionManager", _manager(target))
    conf = GraFloBackendConfig(output_dir=root / "unused")
    return write_folded(SCHEMA, conf, root, RunLedger(root), CFG)


def test_a_run_writes_every_folded_row_once_and_verifies(tmp_path, monkeypatch) -> None:
    _staged(tmp_path)
    target = _Target()

    counts = _run(tmp_path, target, monkeypatch)

    assert counts.vertices == {"V": 7}
    assert sum(counts.edges.values()) == 7
    (vertex_rows,) = target.vertices.values()
    assert {"id": 3, "a": "later"} in vertex_rows
    assert target.begun == [(False, False)] and target.prepared == 1
    assert RunLedger(tmp_path).has(Phase.FINISHED)


def test_a_resumed_run_skips_committed_chunks_and_replays_the_one_in_doubt(
    tmp_path, monkeypatch
) -> None:
    _staged(tmp_path)
    target = _Target(crash_after=2)

    with pytest.raises(_Crash) as crashed:
        _run(tmp_path, target, monkeypatch)
    in_doubt = str(crashed.value)
    written_before = set(target.writes)
    counts = _run(tmp_path, target, monkeypatch)

    assert counts.vertices == {"V": 7} and sum(counts.edges.values()) == 7
    assert target.replays == [in_doubt]
    assert target.begun == [(False, False), (False, True)]
    rewritten = [c for c in target.writes[len(written_before) :] if c in written_before]
    assert rewritten == [in_doubt]


def test_a_count_the_target_does_not_hold_fails_the_run(tmp_path, monkeypatch) -> None:
    _staged(tmp_path)

    with pytest.raises(BulkVerifyError, match="vertex V: expected 7, target holds 6"):
        _run(tmp_path, _Target(drop_one=True), monkeypatch)
    assert not RunLedger(tmp_path).has(Phase.FINISHED)


def test_a_directory_with_other_files_is_refused(tmp_path) -> None:
    (tmp_path / "notes.txt").write_text("mine")
    with pytest.raises(ValueError, match="not empty and holds no bulk run"):
        open_staging(tmp_path)


def test_a_run_stopped_while_staging_stages_again(tmp_path) -> None:
    open_staging(tmp_path)
    Stager(
        SCHEMA, WriteContext.for_flavor(SCHEMA, DBType.GRAFLO_BACKEND), tmp_path
    ).stage(GraphContainer(vertices={"V": [{"id": 1}]}), (0, 0, 0, 0))
    assert (tmp_path / "stage").exists()

    ledger = open_staging(tmp_path)

    assert not (tmp_path / "stage").exists() and not ledger.has(Phase.STAGED)
