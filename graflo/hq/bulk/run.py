"""Run a bulk load: open the staging directory, fold, write, verify.

Casting into the staging directory is the caster's (it hands each batch to a
:class:`~graflo.hq.bulk.stage.Stager`); this module does the rest. Every phase
is recorded in the run ledger, so a run stopped at any point resumes from the
last thing that completed: staging is redone, a fold is kept, written chunks
are skipped and the one in doubt is replayed.
"""

from __future__ import annotations

import logging
import shutil
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Self

from graflo.architecture.evolution.sanitize import physical_schema
from graflo.architecture.schema import Schema
from graflo.connections.bulk import BulkConfig
from graflo.connections.onto import DBConfig
from graflo.db.bulk import BulkCounts, BulkEdgeChunk, BulkVertexChunk, StoredEdgeId
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager
from graflo.hq.bulk import _deps
from graflo.hq.bulk.encode import read_rows
from graflo.hq.bulk.fold import (
    COUNTS,
    FOLDED,
    FoldCounts,
    RunCounts,
    fold,
    folded_cell,
    folded_path,
)
from graflo.hq.bulk.ledger import ChunkState, Phase, RunLedger
from graflo.hq.bulk.partition import rounds
from graflo.hq.bulk.stage import REJECTS, STAGE
from graflo.hq.write_prepare import WriteContext, project_edges, project_vertices

logger = logging.getLogger(__name__)

_OWN = (STAGE, REJECTS, FOLDED, COUNTS, "owners", "duckdb.tmp")


class BulkVerifyError(RuntimeError):
    """The target does not hold what the fold produced."""


def open_staging(staging_dir: Path, mode: str = "bulk") -> RunLedger:
    """The ledger of a new or resumable *mode* run (``bulk`` or ``sync``) in *staging_dir*.

    A run stopped while staging is staged again from the start; one stopped
    later resumes.

    Raises:
        RuntimeError: The ``bulk`` extra is not installed.
        ValueError: *staging_dir* holds files but no run, or a run of the other mode.
    """
    _deps.require()
    root = Path(staging_dir)
    root.mkdir(parents=True, exist_ok=True)
    ledger = RunLedger(root)
    if not ledger.exists:
        if any(root.iterdir()):
            raise ValueError(
                f"staging directory {root} is not empty and holds no bulk or sync "
                "run; give an empty or new directory"
            )
        ledger.set_mode(mode)
        ledger.mark(Phase.OPENED)
    elif (ledger.mode or "bulk") != mode:
        raise ValueError(
            f"staging directory {root} holds a {ledger.mode or 'bulk'} run; a "
            f"{mode} run needs its own directory"
        )
    elif not ledger.has(Phase.STAGED):
        for name in _OWN:
            path = root / name
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
        logger.info("Bulk run in %s was stopped while staging; staging again", root)
    return ledger


def dense_ids_for(conn_conf: DBConfig) -> bool:
    """Whether the target's bulk writer takes dense integer ids.

    Raises:
        ValueError: The target has no bulk writer.
    """
    conn_cls = ConnectionManager.require(conn_conf, ConnectionCapability.BULK_LOAD)
    return bool(conn_cls.bulk_dense_ids)


def edge_buckets_for(conn_conf: DBConfig, cfg: BulkConfig) -> int | None:
    """Buckets per endpoint to split edge runs into, when the writer asks for it.

    Raises:
        ValueError: The target has no bulk writer.
    """
    conn_cls = ConnectionManager.require(conn_conf, ConnectionCapability.BULK_LOAD)
    return cfg.parallelism if conn_cls.bulk_partition_edges else None


def fold_staged(
    staging_dir: Path,
    ledger: RunLedger,
    cfg: BulkConfig,
    *,
    dense_ids: bool,
    edge_buckets: int | None = None,
) -> FoldCounts:
    """Fold the staged runs once; a resumed run reuses the fold."""
    if ledger.has(Phase.FOLDED):
        return FoldCounts.load(staging_dir)
    counts = fold(
        staging_dir,
        dense_ids=dense_ids,
        edge_buckets=edge_buckets,
        memory_limit=cfg.memory_limit,
        threads=cfg.fold_threads,
    )
    ledger.mark(Phase.FOLDED)
    for run in counts.runs:
        logger.info(
            "Folded %s: %s staged, %s folded, %s rejected",
            run.type.label,
            run.staged,
            run.folded,
            run.rejected,
        )
    return counts


def write_folded(
    schema: Schema,
    conn_conf: DBConfig,
    staging_dir: Path,
    ledger: RunLedger,
    cfg: BulkConfig,
    *,
    recreate: bool = False,
) -> BulkCounts:
    """Write the folded runs to the target and check its counts against the fold.

    Args:
        schema: The schema, logical names, initialized for the target flavor.
        conn_conf: The target; it must declare ``supports_bulk_load``.
        staging_dir: A folded staging directory.
        ledger: Its ledger.
        cfg: Chunk size and parallelism.
        recreate: Drop the target's schema first instead of refusing an
            existing one.

    Raises:
        ValueError: The target has no bulk writer, or (from the writer's
            first-write guard) already holds a schema.
        BulkVerifyError: The target's counts differ from the fold's.
    """
    ConnectionManager.require(conn_conf, ConnectionCapability.BULK_LOAD)
    counts = FoldCounts.load(staging_dir)
    ctx = WriteContext.for_flavor(schema, conn_conf.connection_type)
    target_schema = physical_schema(schema, conn_conf.connection_type)
    if ledger.has(Phase.FINISHED):
        logger.info("Bulk run in %s is already finished", staging_dir)
    resume = ledger.has(Phase.BEGINNING)
    with ConnectionManager(connection_config=conn_conf) as db:
        ledger.mark(Phase.BEGINNING)
        plan = db.bulk_begin(target_schema, cfg, recreate=recreate, resume=resume)
        if resume and plan.restart:
            logger.warning(
                "The target lost the chunks written before; writing all again"
            )
            ledger.restart()
        ledger.mark(Phase.BEGUN)
    parallel = min(cfg.parallelism, plan.max_parallel or cfg.parallelism)

    vertex_runs = [r for r in counts.runs if r.type.kind == "vertex"]
    edge_runs = [r for r in counts.runs if r.type.kind == "edge" and not r.buckets]
    celled_runs = [r for r in counts.runs if r.type.kind == "edge" and r.buckets]
    with _Connections(conn_conf) as conns:
        _write_chunks(
            (
                c
                for run in vertex_runs
                for c in _vertex_chunks(ctx, staging_dir, run, cfg)
            ),
            lambda chunk: conns.get().bulk_write_vertices(target_schema, chunk),
            ledger,
            parallel,
        )
        if not ledger.has(Phase.EDGES_PREPARED):
            conns.get().bulk_prepare_edges(target_schema)
            ledger.mark(Phase.EDGES_PREPARED)
        _write_chunks(
            (
                c
                for run in edge_runs
                for c in _edge_chunks(
                    schema,
                    ctx,
                    run,
                    [folded_path(staging_dir, run.slot)],
                    run.slot,
                    cfg,
                )
            ),
            lambda chunk: conns.get().bulk_write_edges(target_schema, chunk),
            ledger,
            parallel,
        )
        # One run at a time: two runs may share an endpoint type.
        for run in celled_runs:
            _write_rounds(
                _edge_rounds(schema, ctx, staging_dir, run, cfg),
                lambda chunk: conns.get().bulk_write_edges(target_schema, chunk),
                ledger,
                parallel,
            )
        actual = conns.get().bulk_finish(target_schema)
    verify(_expected(schema, ctx, counts), actual)
    ledger.mark(Phase.FINISHED)
    return actual


def verify(expected: BulkCounts, actual: BulkCounts) -> None:
    """Refuse a load whose per-type counts differ from the fold's.

    Raises:
        BulkVerifyError: Listing every type that differs.
    """
    diffs = [
        f"vertex {name}: expected {expected.vertices.get(name, 0)}, "
        f"target holds {actual.vertices.get(name, 0)}"
        for name in sorted(set(expected.vertices) | set(actual.vertices))
        if expected.vertices.get(name, 0) != actual.vertices.get(name, 0)
    ] + [
        f"edge {edge}: expected {expected.edges.get(edge, 0)}, "
        f"target holds {actual.edges.get(edge, 0)}"
        for edge in sorted(set(expected.edges) | set(actual.edges), key=str)
        if expected.edges.get(edge, 0) != actual.edges.get(edge, 0)
    ]
    if diffs:
        raise BulkVerifyError("bulk load verification failed: " + "; ".join(diffs))


# -- chunks -------------------------------------------------------------------


_Chunk = tuple[str, Callable[[bool], Any]]


def _slices(paths: list[Path], size: int) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for path in paths:
        for row in read_rows(path):
            batch.append(row)
            if len(batch) == size:
                yield batch
                batch = []
    if batch:
        yield batch


def _vertex_chunks(
    ctx: WriteContext, staging_dir: Path, run: RunCounts, cfg: BulkConfig
) -> Iterator[_Chunk]:
    if not run.folded:
        return
    name = str(run.type.name)
    for index, rows in enumerate(
        _slices([folded_path(staging_dir, run.slot)], cfg.chunk_size)
    ):

        def build(replay: bool, rows: list[dict[str, Any]] = rows, index: int = index):
            stored = project_vertices(ctx, name, [row["doc"] for row in rows])
            return BulkVertexChunk(
                chunk_id=f"{run.slot}:{index}",
                class_name=stored.class_name,
                match_keys=stored.match_keys,
                rows=stored.docs,
                dense=[row["dense"] for row in rows] if "dense" in rows[0] else None,
                replay=replay,
            )

        yield f"{run.slot}:{index}", build


def _edge_chunks(
    schema: Schema,
    ctx: WriteContext,
    run: RunCounts,
    files: list[Path],
    prefix: str,
    cfg: BulkConfig,
) -> Iterator[_Chunk]:
    """Chunks of the edge rows in *files*, with ids ``<prefix>:<index>``."""
    if not run.folded:
        return
    edge_id = run.type.edge_id
    assert edge_id is not None
    edge = schema.core_schema.edge_config.declared(edge_id)
    assert edge is not None, edge_id
    relation = edge_id[2]
    for index, rows in enumerate(_slices(files, cfg.chunk_size)):

        def build(replay: bool, rows: list[dict[str, Any]] = rows, index: int = index):
            triples = [(row["source"], row["target"], row["doc"]) for row in rows]
            stored = project_edges(ctx, edge, relation, triples)
            return BulkEdgeChunk(
                chunk_id=f"{prefix}:{index}",
                source_class=stored.source_class,
                target_class=stored.target_class,
                relation_name=stored.relation_name,
                match_keys_source=list(stored.match_keys_source),
                match_keys_target=list(stored.match_keys_target),
                collection_name=stored.collection_name,
                merge_properties=list(stored.merge_properties or ()),
                rows=[tuple(t) for t in stored.docs],
                dense=(
                    [(row["sdense"], row["tdense"]) for row in rows]
                    if "sdense" in rows[0]
                    else None
                ),
                replay=replay,
            )

        yield f"{prefix}:{index}", build


def _edge_rounds(
    schema: Schema,
    ctx: WriteContext,
    staging_dir: Path,
    run: RunCounts,
    cfg: BulkConfig,
) -> Iterator[list[Iterator[_Chunk]]]:
    """An edge run split into cells, as rounds of lanes that share no endpoint."""
    assert run.buckets and run.type.edge_id is not None
    source, target, _ = run.type.edge_id
    for lanes in rounds(run.buckets, same_type=source == target):
        yield [
            (
                chunk
                for a, b in lane
                for chunk in _edge_chunks(
                    schema,
                    ctx,
                    run,
                    folded_cell(staging_dir, run.slot, (a, b)),
                    f"{run.slot}:{a}.{b}",
                    cfg,
                )
            )
            for lane in lanes
        ]


def _write_one(
    chunk_id: str,
    build: Callable[[bool], Any],
    write: Callable[[Any], None],
    ledger: RunLedger,
) -> None:
    """Write one chunk unless it is committed; replay it if its commit is unknown."""
    state = ledger.chunk(chunk_id)
    if state is ChunkState.COMMITTED:
        return
    chunk = build(state is ChunkState.UNCERTAIN)
    ledger.started(chunk_id)
    write(chunk)
    ledger.committed(chunk_id)


def _write_rounds(
    rounds_: Iterator[list[Iterator[_Chunk]]],
    write: Callable[[Any], None],
    ledger: RunLedger,
    parallel: int,
) -> None:
    """Write each round's lanes in parallel, a lane's chunks in order; round after round."""

    def lane_task(lane: Iterator[_Chunk]) -> None:
        for chunk_id, build in lane:
            _write_one(chunk_id, build, write, ledger)

    with ThreadPoolExecutor(
        max_workers=parallel, thread_name_prefix="graflo-bulk"
    ) as pool:
        for lanes in rounds_:
            futures = [pool.submit(lane_task, lane) for lane in lanes]
            wait(futures)
            for future in futures:
                future.result()


def _write_chunks(
    chunks: Iterator[_Chunk],
    write: Callable[[Any], None],
    ledger: RunLedger,
    parallel: int,
) -> None:
    """Write every chunk not yet committed, *parallel* at a time, in order of submission."""

    pool = ThreadPoolExecutor(max_workers=parallel, thread_name_prefix="graflo-bulk")
    pending: set[Future[None]] = set()
    try:
        for chunk_id, build in chunks:
            if ledger.chunk(chunk_id) is ChunkState.COMMITTED:
                continue
            while len(pending) >= 2 * parallel:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
            pending.add(pool.submit(_write_one, chunk_id, build, write, ledger))
        for future in wait(pending).done:
            future.result()
    except BaseException:
        pool.shutdown(wait=True, cancel_futures=True)
        raise
    pool.shutdown(wait=True)


def _expected(schema: Schema, ctx: WriteContext, counts: FoldCounts) -> BulkCounts:
    vc = ctx.db_aware.vertex_config
    vertices: dict[str, int] = {}
    edges: dict[StoredEdgeId, int] = {}
    for run in counts.runs:
        if run.type.kind == "vertex":
            name = vc.vertex_dbname(str(run.type.name))
            vertices[name] = vertices.get(name, 0) + run.folded
            continue
        edge_id = run.type.edge_id
        assert edge_id is not None
        edge = schema.core_schema.edge_config.declared(edge_id)
        assert edge is not None
        stored = project_edges(ctx, edge, edge_id[2], [])
        key = (stored.source_class, stored.relation_name, stored.target_class)
        edges[key] = edges.get(key, 0) + run.folded
    return BulkCounts(vertices=vertices, edges=edges)


class _Connections:
    """One connection per worker thread, all closed together."""

    def __init__(self, conn_conf: DBConfig) -> None:
        self._conf = conn_conf
        self._local = threading.local()
        self._managers: list[Any] = []
        self._lock = threading.Lock()

    def get(self) -> Any:
        db = getattr(self._local, "db", None)
        if db is None:
            manager = ConnectionManager(connection_config=self._conf)
            db = manager.__enter__()
            self._local.db = db
            with self._lock:
                self._managers.append(manager)
        return db

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        for manager in self._managers:
            try:
                manager.__exit__(None, None, None)
            except Exception:
                logger.warning("Could not close a database connection", exc_info=True)
