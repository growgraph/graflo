"""Write a grouped sync run: ensure every vertex, then rewrite the changed owners.

The input is staged, folded and grouped exactly as a bulk load's (see
:mod:`graflo.hq.bulk.run` and :mod:`graflo.hq.sync.group`), so a sync of a
release writes what a bulk load of it would. Vertices come first, key-only for
the owned types, so an owned edge finds both endpoints whichever chunk writes
it. Every chunk is recorded in the run ledger; a stopped run resumes.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from graflo.architecture.contract.ingestion.sync import OwnedEdge
from graflo.architecture.evolution.sanitize import physical_schema
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema import Schema
from graflo.connections.bulk import BulkConfig
from graflo.connections.onto import DBConfig
from graflo.db.bulk import BulkVertexChunk
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager
from graflo.db.sync import SyncEdgeType, SyncOwner, SyncOwnerChunk
from graflo.hq.bulk.encode import read_rows
from graflo.hq.bulk.fold import FoldCounts, RunCounts, folded_path
from graflo.hq.bulk.ledger import Phase, RunLedger
from graflo.hq.bulk.run import _Chunk, _Connections, _slices, _write_chunks
from graflo.hq.sync.group import OwnerRun, owners_path
from graflo.hq.write_prepare import WriteContext, project_edges, project_vertices

logger = logging.getLogger(__name__)


class SyncReport(BaseModel):
    """Owners rewritten and owners skipped, per owned vertex type."""

    written: dict[str, int] = {}
    skipped: dict[str, int] = {}


class SyncKeyOnlyError(ValueError):
    """A vertex type the run does not own carries more than its key."""


def check_key_only(
    schema: Schema, staging_dir: Path, counts: FoldCounts, owned: set[str]
) -> None:
    """Refuse a folded vertex of a type the run does not own that has properties.

    Such a type is only ever created by a sync run, never updated, so a
    property on it would go stale. Identity and reserved fields are its key and
    its stamp, and may be set.

    Raises:
        SyncKeyOnlyError: Naming each type and the fields found.
    """
    vc = schema.core_schema.vertex_config
    found: dict[str, set[str]] = {}
    for run in counts.runs:
        name = str(run.type.name)
        if run.type.kind != "vertex" or name in owned or not run.folded:
            continue
        allowed = set(vc.identity_fields(name)) | {
            f.name for f in vc.properties(name) if f.reserved
        }
        for row in read_rows(folded_path(staging_dir, run.slot)):
            extra = {k for k, v in row["doc"].items() if v is not None} - allowed
            if extra:
                found.setdefault(name, set()).update(extra)
    if found:
        listed = "; ".join(f"{k}: {sorted(v)}" for k, v in sorted(found.items()))
        raise SyncKeyOnlyError(
            "a sync run writes only the key of a type it does not own, but these "
            f"carry properties: {listed}"
        )


def write_sync(
    schema: Schema,
    conn_conf: DBConfig,
    staging_dir: Path,
    ledger: RunLedger,
    cfg: BulkConfig,
    owner_runs: list[OwnerRun],
    owned: Mapping[EdgeId, OwnedEdge],
) -> SyncReport:
    """Ensure the folded vertices, then rewrite every owner whose hash changed.

    Args:
        schema: The schema, logical names, initialized for the target flavor.
        conn_conf: The target; it must declare ``supports_sync``.
        staging_dir: A folded and grouped staging directory.
        ledger: Its ledger.
        cfg: Chunk size and parallelism.
        owner_runs: The grouped owner types.
        owned: How each owned edge is owned, by declared edge id.

    Raises:
        ValueError: The target has no sync writer.
    """
    ConnectionManager.require(conn_conf, ConnectionCapability.SYNC)
    counts = FoldCounts.load(staging_dir)
    ctx = WriteContext.for_flavor(schema, conn_conf.connection_type)
    target_schema = physical_schema(schema, conn_conf.connection_type)
    owner_types = {run.owner for run in owner_runs}
    edge_types = {
        run.owner: _edge_types(schema, ctx, run.owner, owned) for run in owner_runs
    }
    with ConnectionManager(connection_config=conn_conf) as db:
        db.sync_begin(
            target_schema,
            [t for types in edge_types.values() for _, t in types],
        )
    ledger.mark(Phase.BEGUN)
    report = SyncReport()
    lock = threading.Lock()

    def owners(chunk: SyncOwnerChunk, owner: str) -> None:
        done = conns.get().sync_write_owners(target_schema, chunk)
        with lock:
            report.written[owner] = report.written.get(owner, 0) + done.written
            report.skipped[owner] = report.skipped.get(owner, 0) + done.skipped

    with _Connections(conn_conf) as conns:
        _write_chunks(
            (
                c
                for run in counts.runs
                if run.type.kind == "vertex"
                for c in _ensure_chunks(ctx, staging_dir, run, owner_types, cfg)
            ),
            lambda chunk: conns.get().sync_ensure_vertices(target_schema, chunk),
            ledger,
            cfg.parallelism,
        )
        _write_chunks(
            (
                c
                for run in owner_runs
                for c in _owner_chunks(
                    schema, ctx, staging_dir, run, edge_types[run.owner], cfg
                )
            ),
            lambda chunk: owners(chunk, chunk.chunk_id.split(":")[1]),
            ledger,
            cfg.parallelism,
        )
    ledger.mark(Phase.FINISHED)
    for owner in sorted(report.written):
        logger.info(
            "Synced %s: %s owners rewritten, %s unchanged",
            owner,
            report.written[owner],
            report.skipped.get(owner, 0),
        )
    return report


def _edge_types(
    schema: Schema, ctx: WriteContext, owner: str, owned: Mapping[EdgeId, OwnedEdge]
) -> list[tuple[EdgeId, SyncEdgeType]]:
    """Every edge *owner* owns, as stored, with its declared id."""
    out: list[tuple[EdgeId, SyncEdgeType]] = []
    for edge_id, how in sorted(owned.items(), key=lambda item: str(item[0])):
        if how.owner != owner:
            continue
        edge = schema.core_schema.edge_config.declared(edge_id)
        assert edge is not None, edge_id
        stored = project_edges(ctx, edge, edge.relation, [])
        assert stored.relation_name is not None, edge_id
        prop = (
            ctx.keys.edge_fields(edge.edge_id, [how.owner_field])[0]
            if how.owner_field is not None
            else None
        )
        out.append(
            (
                edge.edge_id,
                SyncEdgeType(
                    source_class=stored.source_class,
                    target_class=stored.target_class,
                    relation_name=stored.relation_name,
                    match_keys_source=list(stored.match_keys_source),
                    match_keys_target=list(stored.match_keys_target),
                    side=how.side,
                    owner_property=prop,
                ),
            )
        )
    return out


def _ensure_chunks(
    ctx: WriteContext,
    staging_dir: Path,
    run: RunCounts,
    owned: set[str],
    cfg: BulkConfig,
) -> Iterator[_Chunk]:
    if not run.folded:
        return
    name = str(run.type.name)
    identity = ctx.db_aware.vertex_config.identity_fields(name)
    for index, rows in enumerate(
        _slices([folded_path(staging_dir, run.slot)], cfg.chunk_size)
    ):

        def build(replay: bool, rows: list[dict[str, Any]] = rows, index: int = index):
            docs = [row["doc"] for row in rows]
            if name in owned:
                # The owner chunk sets the properties; here the vertex only exists.
                docs = [{k: doc.get(k) for k in identity} for doc in docs]
            stored = project_vertices(ctx, name, docs)
            return BulkVertexChunk(
                chunk_id=f"ensure:{run.slot}:{index}",
                class_name=stored.class_name,
                match_keys=stored.match_keys,
                rows=stored.docs,
                replay=replay,
            )

        yield f"ensure:{run.slot}:{index}", build


def _owner_chunks(
    schema: Schema,
    ctx: WriteContext,
    staging_dir: Path,
    run: OwnerRun,
    edge_types: list[tuple[EdgeId, SyncEdgeType]],
    cfg: BulkConfig,
) -> Iterator[_Chunk]:
    """Chunks of *run*'s owners, ids ``owners:<owner>:<slot>:<index>``."""
    ec = schema.core_schema.edge_config
    position = {edge_id: i for i, (edge_id, _) in enumerate(edge_types)}
    edges = [ec.declared(edge_id) for edge_id, _ in edge_types]
    hash_field = ctx.keys.vertex_fields(run.owner, [run.hash_field])[0]
    for index, rows in enumerate(
        _slices([owners_path(staging_dir, run.slot)], cfg.chunk_size)
    ):
        chunk_id = f"owners:{run.owner}:{run.slot}:{index}"

        def build(
            _replay: bool, rows: list[dict[str, Any]] = rows, chunk_id: str = chunk_id
        ):
            stored = project_vertices(ctx, run.owner, [row["doc"] for row in rows])
            per_type: list[list[tuple[int, Any]]] = [[] for _ in edge_types]
            for i, row in enumerate(rows):
                for entry in row["edges"]:
                    declared = ec.declared(tuple(entry["edge"]))
                    assert declared is not None, entry["edge"]
                    triple = (entry["source"], entry["target"], entry["doc"])
                    per_type[position[declared.edge_id]].append((i, triple))
            owned: list[list[list[Any]]] = [[[] for _ in edge_types] for _ in rows]
            for t, items in enumerate(per_type):
                edge = edges[t]
                assert edge is not None
                projected = project_edges(
                    ctx, edge, edge.relation, [triple for _, triple in items]
                ).docs
                for (i, _), triple in zip(items, projected, strict=True):
                    owned[i][t].append(tuple(triple))
            return SyncOwnerChunk(
                chunk_id=chunk_id,
                class_name=stored.class_name,
                match_keys=stored.match_keys,
                hash_field=hash_field,
                edge_types=[t for _, t in edge_types],
                owners=[
                    SyncOwner(doc=doc, edges=owned[i])
                    for i, doc in enumerate(stored.docs)
                ],
            )

        yield chunk_id, build
