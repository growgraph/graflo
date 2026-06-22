"""Stateless document-to-graph casting (no I/O)."""

from __future__ import annotations

import asyncio
import json
import traceback
from collections.abc import Iterable
from typing import Any, Literal

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.runtime import ResourceRuntime
from graflo.architecture.contract.runtime.resource import resolve_effective_vertex_names
from graflo.architecture.graph_types import (
    GraphContainer,
    ResourceCastResult,
    TransformCastFailure,
)
from graflo.architecture.schema.vertex import VertexConfig
from graflo.hq.ingestion_parameters import (
    CastBatchResult,
    DocCastFailure,
    IngestionParams,
)

_DOC_CAST_ERROR_TRACEBACK_MAX_CHARS = 16_384


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
    gc: GraphContainer, *, vertex_config: VertexConfig
) -> None:
    """Remove vertex docs and edge tuples with no usable schema identity."""
    blank = set(vertex_config.blank_vertices)
    vertex_set = vertex_config.vertex_set

    for vcol, docs in list(gc.vertices.items()):
        if vcol in blank or vcol not in vertex_set:
            continue
        id_fields = vertex_config.identity_fields(vcol)
        gc.vertices[vcol] = [
            d for d in docs if not _vertex_doc_has_empty_identity(d, id_fields)
        ]

    for edge_id, docs in list(gc.edges.items()):
        vfrom, vto, _rel = edge_id
        if vfrom not in vertex_set or vto not in vertex_set:
            continue
        if vfrom in blank or vto in blank:
            continue
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
    return DocCastFailure(
        resource_name=resource_name,
        doc_index=doc_index,
        exception_type=type(exc).__name__,
        message=str(exc),
        traceback=_format_traceback(exc),
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


def _coerce_doc(doc_raw: Any) -> dict[str, Any]:
    if isinstance(doc_raw, dict):
        return doc_raw
    return {"_source_repr": repr(doc_raw)}


class DocumentCaster:
    """Cast source documents to :class:`GraphContainer` via ingestion resources."""

    def __init__(self, ingestion_model: IngestionModel) -> None:
        self.ingestion_model = ingestion_model

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

        doc_list = list(data)
        cast_results, failures = await self._gather_cast_results(
            runtime,
            doc_list,
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
            )
        return CastBatchResult(graph=graph, failures=failures)

    async def _gather_cast_results(
        self,
        runtime: ResourceRuntime,
        doc_list: list[Any],
        *,
        on_doc_error: Literal["fail", "skip"],
        resolved_name: str,
        params: IngestionParams,
    ) -> tuple[list[ResourceCastResult | BaseException], list[DocCastFailure]]:
        semaphore = asyncio.Semaphore(params.n_cores)

        async def process_doc(doc: dict[str, Any]) -> ResourceCastResult:
            async with semaphore:
                return await asyncio.to_thread(runtime.cast_document, doc)

        if on_doc_error == "fail":
            raw = await asyncio.gather(
                *[process_doc(_coerce_doc(doc)) for doc in doc_list]
            )
        else:
            raw = await asyncio.gather(
                *[process_doc(_coerce_doc(doc)) for doc in doc_list],
                return_exceptions=True,
            )

        cast_results: list[ResourceCastResult | BaseException] = []
        failures: list[DocCastFailure] = []
        for i, item in enumerate(raw):
            doc = _coerce_doc(doc_list[i])
            if isinstance(item, asyncio.CancelledError):
                raise item
            if isinstance(item, (KeyboardInterrupt, SystemExit)):
                raise item
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
