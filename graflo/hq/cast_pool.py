"""Worker-process casting for the ingestion hot path.

Casting a document is pure Python and never releases the GIL, so threads cannot
speed it up — they only add dispatch cost. This module moves the work into worker
*processes*, which is where ``IngestionParams.n_cores`` finally means cores.

Only declarative data crosses the boundary. :class:`~graflo.architecture.pipeline.runtime.resource.ResourceRuntime`
is schema-bound and explicitly not serializable, so each worker rebuilds its own
from the serialized ``ResourceConfig`` / ``VertexConfig`` / ``EdgeConfig`` once and
caches it; afterwards only plain documents travel in and plain entity payloads
travel back.
"""

from __future__ import annotations

import logging
import os
import traceback
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class CastSpec(BaseModel):
    """Everything a worker needs to rebuild a resource runtime, as plain data."""

    model_config = ConfigDict(frozen=True)

    resource: dict[str, Any]
    vertex_config: dict[str, Any]
    edge_config: dict[str, Any]
    transforms: list[dict[str, Any]] = Field(default_factory=list)
    strict_references: bool = False
    allowed_vertex_names: list[str] | None = None
    target_db_flavor: str | None = None


# One chunk's outcome as a plain tuple:
# (vertices, edges, linear, transform_failures, errors) where
#   vertices/edges/linear are the chunk's *pre-folded, pre-filtered*
#   GraphContainer fields (the parent only concatenates chunks, in order);
#   transform_failures is [(local_doc_index, [TransformCastFailure, ...]), ...];
#   errors is [(local_doc_index, (exception_type, message, traceback)), ...].
#
# Deliberately not a pydantic model, and folded worker-side: building one model
# per document in the worker and another in the parent — plus re-folding the
# whole batch serially in the parent — cost more than the parallelism was
# saving. Pickling the tuple keeps shared references (an edge endpoint is the
# same object as its vertex doc), so identity survives the process boundary.
WorkerChunkResult = tuple[
    dict[str, list],
    dict[Any, list],
    list[Any],
    list[tuple[int, list]],
    list[tuple[int, tuple[str, str, str]]],
]


# Set once per worker by `init_worker`. Building it costs schema resolution and actor
# tree construction, so it must happen at worker start, not once per chunk.
_RUNTIME: Any = None


def _build_runtime(spec: CastSpec) -> Any:
    from graflo.architecture.contract.ingestion.resource import ResourceConfig
    from graflo.architecture.contract.ingestion.transform import ProtoTransform
    from graflo.architecture.pipeline.runtime.resource import build_resource_runtime
    from graflo.architecture.schema.edge import EdgeConfig
    from graflo.architecture.schema.vertex import VertexConfig
    from graflo.onto import DBType

    transforms = {}
    for payload in spec.transforms:
        proto = ProtoTransform.model_validate(payload)
        if proto.name:
            transforms[proto.name] = proto

    return build_resource_runtime(
        ResourceConfig.model_validate(spec.resource),
        VertexConfig.model_validate(spec.vertex_config),
        EdgeConfig.model_validate(spec.edge_config),
        transforms,
        strict_references=spec.strict_references,
        allowed_vertex_names=(
            set(spec.allowed_vertex_names)
            if spec.allowed_vertex_names is not None
            else None
        ),
        target_db_flavor=(
            DBType(spec.target_db_flavor) if spec.target_db_flavor else None
        ),
    )


def init_worker(spec: CastSpec) -> None:
    """Pool initializer: build this worker's runtime before any chunk arrives.

    The runtime is held per process so it is built once, not once per chunk — and so
    the spec never has to travel again. Only documents cross after this.
    """
    global _RUNTIME
    _RUNTIME = _build_runtime(spec)


def cast_chunk(
    docs: list[dict[str, Any]],
    post_filter_vertex_names: list[str] | None,
    drop_empty_identity: bool,
) -> WorkerChunkResult:
    """Cast a contiguous slice of documents in this worker, in order, and fold.

    Per-document exceptions are captured as data rather than raised: an exception
    raised here would fail the whole chunk, losing the documents around it, and not
    every exception survives pickling anyway.

    The fold (``GraphContainer.from_docs_list``) and the post-cast filters run
    here rather than in the parent: the fold is an associative, order-preserving
    concat, so chunk-wise folding followed by in-order concatenation in the
    parent yields the identical graph — while the serial parent-side cost that
    used to cancel out the process-pool speedup disappears.
    """
    runtime = _RUNTIME
    if runtime is None:  # pragma: no cover - pool always runs the initializer
        raise RuntimeError("cast worker was not initialized with a CastSpec")
    # Deferred: keep worker start cheap and avoid an import cycle with
    # document_caster (which imports this module at module level).
    from graflo.architecture.graph_types import GraphContainer
    from graflo.hq.document_caster import (
        filter_graph_container_by_vertices_inplace,
        filter_graph_container_drop_empty_identity_inplace,
    )

    entities_list: list[Any] = []
    transform_failures: list[tuple[int, list]] = []
    errors: list[tuple[int, tuple[str, str, str]]] = []
    for i, doc in enumerate(docs):
        try:
            result = runtime.cast_document(doc)
        except Exception as exc:
            errors.append((i, (type(exc).__name__, str(exc), traceback.format_exc())))
            continue
        entities_list.append(result.entities)
        if result.transform_failures:
            transform_failures.append((i, list(result.transform_failures)))

    gc = GraphContainer.from_docs_list(entities_list)
    filter_graph_container_by_vertices_inplace(
        gc,
        allowed_vertex_names=(
            set(post_filter_vertex_names)
            if post_filter_vertex_names is not None
            else None
        ),
    )
    if drop_empty_identity:
        filter_graph_container_drop_empty_identity_inplace(
            gc,
            vertex_config=runtime.vertex_config,
            edge_derivation=runtime.edge_derivation,
        )
    return gc.vertices, gc.edges, gc.linear, transform_failures, errors


class WorkerCastError(RuntimeError):
    """A per-document failure carried back from a worker process.

    Rebuilt from the worker's report rather than unpickled, so the original type
    name and traceback survive even when the exception itself would not.
    """

    def __init__(self, error: tuple[str, str, str]) -> None:
        exception_type, message, worker_traceback = error
        super().__init__(message)
        self.exception_type = exception_type
        self.worker_traceback = worker_traceback


def default_worker_count(n_cores: int) -> int:
    """Worker processes to run, bounded by the machine's actual CPUs."""
    available = os.cpu_count() or 1
    return max(1, min(n_cores, available))
