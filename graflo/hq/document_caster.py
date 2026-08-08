"""Stateless document-to-graph casting (no I/O)."""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
import threading
import traceback
from collections import OrderedDict
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Literal

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import (
    GraphContainer,
    ResourceCastResult,
    TransformCastFailure,
)
from graflo.architecture.pipeline.runtime.resource import (
    ResourceRuntime,
    resolve_effective_vertex_names,
)
from graflo.architecture.schema.vertex import VertexConfig
from graflo.hq.cast_pool import (
    CastSpec,
    WorkerCastError,
    cast_chunk,
    default_worker_count,
    init_worker,
)
from graflo.hq.ingestion_parameters import (
    CastBatchResult,
    DocCastFailure,
    IngestionParams,
)

logger = logging.getLogger(__name__)

_DOC_CAST_ERROR_TRACEBACK_MAX_CHARS = 16_384

# Cast pools are built lazily, mid-run, on the event-loop thread -- while
# `asyncio.to_thread` workers are concurrently casting and writing. Under the
# platform default on Linux (`fork`) a child inherits a snapshot of every lock
# held at that instant, permanently locked: `_VERTEX_ACTOR_CREATE_LOCK` in the
# vertex-router actor, or any `logging` handler lock. The child then deadlocks on
# its first log call inside `init_worker`, and the parent blocks forever in the
# `asyncio.gather` over the executor futures -- and again in `close()`.
#
# `spawn` starts each worker from a clean interpreter, so no lock crosses the
# boundary. Nothing else is needed to support it: `init_worker`/`cast_chunk` are
# module-level in `cast_pool`, `CastSpec` is a frozen model of plain dicts (and
# `initargs` was already pickled under fork), and `_build_runtime` re-imports
# what it needs child-side. The extra cost is one graflo import per worker, which
# is exactly what the `_MAX_LIVE_POOLS` cache below exists to amortize.
_MP_CONTEXT = multiprocessing.get_context("spawn")


def filter_graph_container_by_vertices_inplace(
    gc: GraphContainer, *, allowed_vertex_names: set[str] | None
) -> None:
    """Restrict persistence to a subset of vertex types (in-place)."""
    if allowed_vertex_names is None:
        return
    gc.vertices = {
        vcol: items
        for vcol, items in gc.vertices.items()
        if vcol in allowed_vertex_names
    }
    gc.edges = {
        (vfrom, vto, rel): items
        for (vfrom, vto, rel), items in gc.edges.items()
        if vfrom in allowed_vertex_names and vto in allowed_vertex_names
    }


def _identity_value_is_empty(value: Any) -> bool:
    return value is None or value == ""


def _vertex_doc_has_empty_identity(
    doc: dict[str, Any], identity_fields: list[str]
) -> bool:
    if not identity_fields:
        return False
    return all(_identity_value_is_empty(doc.get(field)) for field in identity_fields)


def filter_graph_container_drop_empty_identity_inplace(
    gc: GraphContainer,
    *,
    vertex_config: VertexConfig,
    edge_derivation: Any | None = None,
) -> None:
    """Remove vertex docs and edge tuples with no usable schema identity.

    An endpoint declared by a secondary identity is judged on *that* field-set:
    it carries no primary key by construction, so checking the primary identity
    would discard exactly the edges this resolution path exists to write.
    """
    blank = set(vertex_config.blank_vertices)
    assigned = set(vertex_config.assigned_vertices)
    skip_minted = blank | assigned
    vertex_set = vertex_config.vertex_set

    for vcol, docs in list(gc.vertices.items()):
        if vcol in skip_minted or vcol not in vertex_set:
            continue
        id_fields = vertex_config.identity_fields(vcol)
        gc.vertices[vcol] = [
            d for d in docs if not _vertex_doc_has_empty_identity(d, id_fields)
        ]

    for edge_id, docs in list(gc.edges.items()):
        vfrom, vto, _rel = edge_id
        if vfrom not in vertex_set or vto not in vertex_set:
            continue
        if vfrom in skip_minted or vto in skip_minted:
            continue
        match = (
            edge_derivation.endpoint_match_for(edge_id)
            if edge_derivation is not None
            else None
        )
        if match is not None:
            src_ids = vertex_config.match_fields(vfrom, match.source)
            tgt_ids = vertex_config.match_fields(vto, match.target)
        else:
            src_ids = vertex_config.identity_fields(vfrom)
            tgt_ids = vertex_config.identity_fields(vto)
        kept = [
            t
            for t in docs
            if not _vertex_doc_has_empty_identity(t[0], src_ids)
            and not _vertex_doc_has_empty_identity(t[1], tgt_ids)
        ]
        if kept:
            gc.edges[edge_id] = kept
        else:
            del gc.edges[edge_id]


def _format_traceback(exc: BaseException) -> str:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if len(tb) > _DOC_CAST_ERROR_TRACEBACK_MAX_CHARS:
        return tb[:_DOC_CAST_ERROR_TRACEBACK_MAX_CHARS] + "\n...(traceback truncated)"
    return tb


def _build_doc_preview(
    doc: dict[str, Any],
    keys: tuple[str, ...] | None,
    max_bytes: int,
) -> Any:
    if keys is not None:
        preview_obj: Any = {k: doc[k] for k in keys if k in doc}
    else:
        preview_obj = doc
    raw = json.dumps(preview_obj, default=str, sort_keys=True)
    encoded = raw.encode("utf-8")
    if len(encoded) <= max_bytes:
        return json.loads(raw)
    cut = raw.encode("utf-8")[:max_bytes].decode("utf-8", errors="replace")
    return f"{cut}...(doc preview truncated)"


def _doc_failure_from_exception(
    *,
    resource_name: str,
    doc_index: int,
    doc: dict[str, Any],
    exc: BaseException,
    doc_keys: tuple[str, ...] | None,
    doc_preview_max_bytes: int,
) -> DocCastFailure:
    # A failure from a worker process carries the original type name and traceback as
    # data; reporting it as WorkerCastError would hide what actually went wrong.
    if isinstance(exc, WorkerCastError):
        exception_type = exc.exception_type
        formatted = exc.worker_traceback[:_DOC_CAST_ERROR_TRACEBACK_MAX_CHARS]
    else:
        exception_type = type(exc).__name__
        formatted = _format_traceback(exc)

    return DocCastFailure(
        resource_name=resource_name,
        doc_index=doc_index,
        exception_type=exception_type,
        message=str(exc),
        traceback=formatted,
        doc_preview=_build_doc_preview(doc, doc_keys, doc_preview_max_bytes),
    )


def _doc_failure_from_transform(
    *,
    resource_name: str,
    doc_index: int,
    doc: dict[str, Any],
    fail: TransformCastFailure,
    doc_keys: tuple[str, ...] | None,
    doc_preview_max_bytes: int,
) -> DocCastFailure:
    tb = fail.traceback
    if len(tb) > _DOC_CAST_ERROR_TRACEBACK_MAX_CHARS:
        tb = tb[:_DOC_CAST_ERROR_TRACEBACK_MAX_CHARS] + "\n...(traceback truncated)"
    return DocCastFailure(
        resource_name=resource_name,
        doc_index=doc_index,
        failure_kind="transform",
        exception_type=fail.exception_type,
        message=fail.message,
        traceback=tb,
        doc_preview=_build_doc_preview(doc, doc_keys, doc_preview_max_bytes),
        location_path=fail.location.path,
        transform_label=fail.transform_label,
        nulled_fields=fail.nulled_fields,
    )


def _transform_failures_to_doc_cast_failures(
    *,
    resource_name: str,
    doc_index: int,
    doc: dict[str, Any],
    transform_failures: list[TransformCastFailure],
    doc_keys: tuple[str, ...] | None,
    doc_preview_max_bytes: int,
) -> list[DocCastFailure]:
    return [
        _doc_failure_from_transform(
            resource_name=resource_name,
            doc_index=doc_index,
            doc=doc,
            fail=fail,
            doc_keys=doc_keys,
            doc_preview_max_bytes=doc_preview_max_bytes,
        )
        for fail in transform_failures
    ]


def _flavor_value(flavor: Any) -> str | None:
    """Target flavor as a plain string.

    Reached with either a ``DBType`` or the bare string it was built from, depending
    on how the schema was assembled.
    """
    if flavor is None:
        return None
    return flavor.value if hasattr(flavor, "value") else str(flavor)


def _coerce_doc(doc_raw: Any) -> dict[str, Any]:
    if isinstance(doc_raw, dict):
        return doc_raw
    return {"_source_repr": repr(doc_raw)}


# Casting is pure Python and never releases the GIL, so a thread hop per document
# costs more than the cast itself. Work is handed to a worker one contiguous slice
# at a time; the slice floor keeps a small batch from being split into hops that
# cost more than they save.
_MIN_DOCS_PER_CAST_CHUNK = 64


def _cast_chunk(
    runtime: ResourceRuntime, docs: list[dict[str, Any]]
) -> list[ResourceCastResult | Exception]:
    """Cast a contiguous slice of documents, capturing per-document failures.

    Only :class:`Exception` is captured. ``CancelledError`` / ``KeyboardInterrupt`` /
    ``SystemExit`` propagate so they still abort the batch.
    """
    out: list[ResourceCastResult | Exception] = []
    for doc in docs:
        try:
            out.append(runtime.cast_document(doc))
        except Exception as exc:
            out.append(exc)
    return out


class DocumentCaster:
    """Cast source documents to :class:`GraphContainer` via ingestion resources."""

    # Concurrent sources may interleave batches of different resources; keeping a
    # small set of pools alive avoids tearing one down (and re-importing graflo in
    # every worker) each time the resource changes.
    _MAX_LIVE_POOLS = 4

    def __init__(self, ingestion_model: IngestionModel) -> None:
        self.ingestion_model = ingestion_model
        # Worker startup is dominated by importing graflo and rebuilding the runtime,
        # which costs far more than casting a batch. Pools are therefore kept alive
        # across batches, one per (resource, workers), LRU-bounded.
        self._pools: OrderedDict[tuple[str, int], ProcessPoolExecutor] = OrderedDict()
        # Serializing a resource + its schema is not free, and it is identical for
        # every batch of that resource.
        self._spec_cache: dict[str, CastSpec | None] = {}
        self._cache_lock = threading.Lock()

    def close(self) -> None:
        """Shut down all cast worker pools, if any were started."""
        with self._cache_lock:
            pools = list(self._pools.values())
            self._pools.clear()
        for pool in pools:
            pool.shutdown(wait=True)

    def _get_pool(
        self, spec: CastSpec, workers: int, *, resource_name: str
    ) -> ProcessPoolExecutor:
        key = (resource_name, workers)
        evicted: ProcessPoolExecutor | None = None
        with self._cache_lock:
            pool = self._pools.get(key)
            if pool is not None:
                self._pools.move_to_end(key)
                return pool
            pool = ProcessPoolExecutor(
                max_workers=workers,
                mp_context=_MP_CONTEXT,
                initializer=init_worker,
                initargs=(spec,),
            )
            self._pools[key] = pool
            if len(self._pools) > self._MAX_LIVE_POOLS:
                _, evicted = self._pools.popitem(last=False)
        if evicted is not None:
            evicted.shutdown(wait=True)
        return pool

    async def cast_batch(
        self,
        data: Iterable[Any],
        resource_name: str | None,
        *,
        params: IngestionParams,
        allowed_vertex_names: set[str] | None = None,
    ) -> CastBatchResult:
        runtime = self.ingestion_model.fetch_resource(resource_name)
        resolved_name = runtime.name
        vertex_filter = resolve_effective_vertex_names(
            runtime.collect_vertex_names(),
            allowed_vertex_names=allowed_vertex_names,
        )

        docs = [_coerce_doc(doc) for doc in data]
        mode = self._resolve_cast_mode(params, n_docs=len(docs))
        workers = max(1, params.n_cores)

        if mode == "process":
            spec = self._cast_spec(runtime, params=params)
            if spec is None:
                mode = "thread"
            else:
                try:
                    return await self._cast_batch_in_processes(
                        spec,
                        docs,
                        workers=workers,
                        resolved_name=resolved_name,
                        vertex_filter=vertex_filter,
                        params=params,
                    )
                except WorkerCastError:
                    # on_doc_error="fail": a document failed, not the pool.
                    raise
                except Exception as exc:
                    logger.warning(
                        "Process-pool casting unavailable (%s: %s); falling back to "
                        "in-process casting for resource %r.",
                        type(exc).__name__,
                        exc,
                        runtime.name,
                    )
                    mode = "thread"

        # Dynamic edge feedback mutates the shared edge_config *during* casting;
        # spreading documents over threads would race those registrations, so the
        # fallback for that configuration is single-threaded, not "thread".
        if mode == "thread" and params.dynamic_edges:
            mode = "inline"

        raw = await self._cast_documents(
            runtime, docs, n_cores=workers if mode == "thread" else 1
        )
        cast_results, failures = self._collect_cast_results(
            raw,
            docs,
            on_doc_error=params.on_doc_error,
            resolved_name=resolved_name,
            params=params,
        )

        graph = GraphContainer.from_docs_list(
            [r.entities for r in cast_results if isinstance(r, ResourceCastResult)]
        )
        filter_graph_container_by_vertices_inplace(
            graph, allowed_vertex_names=vertex_filter
        )
        if params.drop_empty_identity_docs:
            filter_graph_container_drop_empty_identity_inplace(
                graph,
                vertex_config=runtime.vertex_config,
                edge_derivation=runtime.edge_derivation,
            )
        return CastBatchResult(graph=graph, failures=failures)

    def _resolve_cast_mode(
        self, params: IngestionParams, *, n_docs: int
    ) -> Literal["inline", "thread", "process"]:
        """Resolve ``auto`` to a concrete executor for this batch.

        ``auto`` reaches for processes only when there is real CPU work to win
        back: more than one core requested, no dynamic-edge feedback (which
        cannot cross a process boundary), and enough documents that each worker
        gets at least a full chunk to amortize pickling documents out and the
        folded entities back. Workers fold and filter their own chunks, so the
        parent-side serial cost that used to cancel out the process-pool gain
        (~3x on casting over 8 workers) no longer applies.
        """
        mode = params.cast_executor
        if mode != "auto":
            return mode
        if (
            params.n_cores > 1
            and not params.dynamic_edges
            and n_docs >= params.n_cores * _MIN_DOCS_PER_CAST_CHUNK
        ):
            return "process"
        return "inline"

    def _collect_cast_results(
        self,
        raw: list[ResourceCastResult | Exception],
        docs: list[dict[str, Any]],
        *,
        on_doc_error: Literal["fail", "skip"],
        resolved_name: str,
        params: IngestionParams,
    ) -> tuple[list[ResourceCastResult | BaseException], list[DocCastFailure]]:
        if on_doc_error == "fail":
            # Preserve "the first failing document aborts the batch", by document
            # order rather than by whichever worker happened to fail first.
            for item in raw:
                if isinstance(item, BaseException):
                    raise item

        cast_results: list[ResourceCastResult | BaseException] = []
        failures: list[DocCastFailure] = []
        for i, item in enumerate(raw):
            doc = docs[i]
            if isinstance(item, BaseException):
                failures.append(
                    _doc_failure_from_exception(
                        resource_name=resolved_name,
                        doc_index=i,
                        doc=doc,
                        exc=item,
                        doc_keys=params.doc_error_preview_keys,
                        doc_preview_max_bytes=params.doc_error_preview_max_bytes,
                    )
                )
                continue
            failures.extend(
                _transform_failures_to_doc_cast_failures(
                    resource_name=resolved_name,
                    doc_index=i,
                    doc=doc,
                    transform_failures=item.transform_failures,
                    doc_keys=params.doc_error_preview_keys,
                    doc_preview_max_bytes=params.doc_error_preview_max_bytes,
                )
            )
            cast_results.append(item)
        return cast_results, failures

    def _cast_spec(
        self, runtime: ResourceRuntime, *, params: IngestionParams
    ) -> CastSpec | None:
        """Serializable description of *runtime*, or ``None`` if it cannot cross.

        Dynamic edge feedback registers new edges on the shared ``edge_config``
        *during* casting. In a worker process those registrations would be invisible
        to the parent's db-aware projection, so that configuration stays in-process.
        """
        if params.dynamic_edges:
            return None
        with self._cache_lock:
            if runtime.name in self._spec_cache:
                return self._spec_cache[runtime.name]
        spec = self._build_cast_spec(runtime, params=params)
        with self._cache_lock:
            return self._spec_cache.setdefault(runtime.name, spec)

    def _build_cast_spec(
        self, runtime: ResourceRuntime, *, params: IngestionParams
    ) -> CastSpec | None:
        try:
            return CastSpec(
                resource=runtime.config.to_dict(skip_defaults=False),
                vertex_config=runtime.vertex_config.to_dict(skip_defaults=False),
                edge_config=runtime.edge_config.to_dict(skip_defaults=False),
                transforms=[
                    proto.to_dict(skip_defaults=False)
                    for proto in self.ingestion_model.transforms
                ],
                strict_references=params.strict_references,
                target_db_flavor=_flavor_value(runtime.target_db_flavor),
            )
        except Exception as exc:
            logger.warning(
                "Resource %r cannot be described for worker processes (%s: %s); "
                "casting it in process instead.",
                runtime.name,
                type(exc).__name__,
                exc,
            )
            return None

    async def _cast_batch_in_processes(
        self,
        spec: CastSpec,
        docs: list[dict[str, Any]],
        *,
        workers: int,
        resolved_name: str,
        vertex_filter: set[str] | None,
        params: IngestionParams,
    ) -> CastBatchResult:
        """Cast *docs* across worker processes, preserving document order.

        Workers fold and filter their own contiguous chunks; the parent only
        concatenates the partial containers in chunk order, which yields the
        identical graph to a serial fold over the whole batch.
        """
        n_workers = default_worker_count(workers)
        size = -(-len(docs) // n_workers)
        chunks = [docs[i : i + size] for i in range(0, len(docs), size)]
        offsets = list(range(0, len(docs), size))
        filter_names = sorted(vertex_filter) if vertex_filter is not None else None

        loop = asyncio.get_running_loop()
        pool = self._get_pool(spec, n_workers, resource_name=resolved_name)
        # Only documents cross here: the workers were primed with the spec.
        parts = await asyncio.gather(
            *[
                loop.run_in_executor(
                    pool,
                    cast_chunk,
                    chunk,
                    filter_names,
                    params.drop_empty_identity_docs,
                )
                for chunk in chunks
            ]
        )

        if params.on_doc_error == "fail":
            # Chunks are contiguous and in order, so the first chunk reporting an
            # error holds the failing document with the smallest global index.
            for part in parts:
                errors = part[4]
                if errors:
                    _, error = min(errors, key=lambda e: e[0])
                    raise WorkerCastError(error)

        vertices: dict[str, list] = {}
        edges: dict[Any, list] = {}
        linear: list[Any] = []
        failures: list[DocCastFailure] = []
        for offset, part in zip(offsets, parts):
            part_vertices, part_edges, part_linear, transform_failures, errors = part
            for k, v in part_vertices.items():
                vertices.setdefault(k, []).extend(v)
            for k, v in part_edges.items():
                edges.setdefault(k, []).extend(v)
            linear.extend(part_linear)
            # A document either failed outright or carries transform failures,
            # never both; merging by local index restores document order.
            per_doc: list[tuple[int, tuple[str, str, str] | None, list]] = [
                (i, error, []) for i, error in errors
            ] + [(i, None, tfails) for i, tfails in transform_failures]
            for local_idx, error, tfails in sorted(per_doc, key=lambda t: t[0]):
                doc = docs[offset + local_idx]
                if error is not None:
                    failures.append(
                        _doc_failure_from_exception(
                            resource_name=resolved_name,
                            doc_index=offset + local_idx,
                            doc=doc,
                            exc=WorkerCastError(error),
                            doc_keys=params.doc_error_preview_keys,
                            doc_preview_max_bytes=params.doc_error_preview_max_bytes,
                        )
                    )
                    continue
                failures.extend(
                    _transform_failures_to_doc_cast_failures(
                        resource_name=resolved_name,
                        doc_index=offset + local_idx,
                        doc=doc,
                        transform_failures=tfails,
                        doc_keys=params.doc_error_preview_keys,
                        doc_preview_max_bytes=params.doc_error_preview_max_bytes,
                    )
                )

        graph = GraphContainer(vertices=vertices, edges=edges, linear=linear)
        return CastBatchResult(graph=graph, failures=failures)

    async def _cast_documents(
        self,
        runtime: ResourceRuntime,
        docs: list[dict[str, Any]],
        *,
        n_cores: int,
    ) -> list[ResourceCastResult | Exception]:
        """Cast *docs* in document order, spreading contiguous slices over workers.

        A slice per worker rather than a task per document: ``cast_document`` is
        GIL-bound, so the dispatch was costing more than the work. Anything beyond
        a trivially small batch still crosses into a worker thread once, to keep
        the event loop free for batch prefetch.
        """
        if not docs:
            return []

        workers = max(1, n_cores)
        if workers == 1 and len(docs) <= _MIN_DOCS_PER_CAST_CHUNK:
            return _cast_chunk(runtime, docs)

        max_useful = -(-len(docs) // _MIN_DOCS_PER_CAST_CHUNK)
        n_chunks = max(1, min(workers, max_useful))
        size = -(-len(docs) // n_chunks)
        chunks = [docs[i : i + size] for i in range(0, len(docs), size)]

        parts = await asyncio.gather(
            *[asyncio.to_thread(_cast_chunk, runtime, chunk) for chunk in chunks],
            return_exceptions=True,
        )
        out: list[ResourceCastResult | Exception] = []
        for part in parts:
            if isinstance(part, BaseException):
                # A chunk raised outside per-document capture (cancellation, exit):
                # abort the batch rather than reporting it against one document.
                raise part
            out.extend(part)
        return out
