"""Assembly phase for turning extracted observations into graph edges."""

from __future__ import annotations

from typing import Any

from graflo.architecture.actor.edge_render import render_edge, render_weights
from graflo.architecture.edge import EdgeConfig
from graflo.architecture.onto import AssemblyContext, EdgeId, LocationIndex
from graflo.architecture.vertex import VertexConfig
from graflo.util.merge import merge_doc_basis


def _merge_vertices_for_edge(
    ctx: AssemblyContext, vertex_config: VertexConfig, source: str, target: str
) -> None:
    for vname in (source, target):
        for lindex, vlist in ctx.acc_vertex[vname].items():
            ctx.acc_vertex[vname][lindex] = merge_doc_basis(
                vlist, tuple(vertex_config.identity_fields(vname))
            )


def _emit_edge_documents(
    *,
    ctx: AssemblyContext,
    vertex_config: VertexConfig,
    edge: Any,
    lindex: LocationIndex | None,
) -> bool:
    _merge_vertices_for_edge(ctx, vertex_config, edge.source, edge.target)
    edges = render_edge(edge=edge, vertex_config=vertex_config, ctx=ctx, lindex=lindex)
    edges = render_weights(edge, vertex_config, ctx.acc_vertex, edges)
    emitted = False
    for relation, edocs in edges.items():
        if edocs:
            emitted = True
        ctx.acc_global[edge.source, edge.target, relation] += edocs
    return emitted


def _matches_selector(selector: EdgeId, edge_id: EdgeId) -> bool:
    ss, st, sr = selector
    es, et, er = edge_id
    return ss == es and st == et and (sr is None or sr == er)


def _is_inference_allowed(
    edge_id: EdgeId,
    *,
    infer_edge_only: set[EdgeId],
    infer_edge_except: set[EdgeId],
) -> bool:
    if infer_edge_only and not any(
        _matches_selector(selector, edge_id) for selector in infer_edge_only
    ):
        return False
    if infer_edge_except and any(
        _matches_selector(selector, edge_id) for selector in infer_edge_except
    ):
        return False
    return True


def assemble_edges(
    *,
    ctx: AssemblyContext,
    vertex_config: VertexConfig,
    edge_config: EdgeConfig,
    infer_edges: bool,
    infer_edge_only: set[EdgeId] | None = None,
    infer_edge_except: set[EdgeId] | None = None,
) -> None:
    """Assemble all edge documents after extraction finishes."""
    if infer_edge_only is None:
        infer_edge_only = set()
    if infer_edge_except is None:
        infer_edge_except = set()

    emitted_pairs: set[tuple[str, str]] = set()

    explicit_requests: list[tuple[Any, LocationIndex | None]] = [
        (intent.edge, intent.location) for intent in ctx.edge_intents
    ]
    if not explicit_requests:
        explicit_requests = list(ctx.edge_requests)

    for edge, lindex in explicit_requests:
        if _emit_edge_documents(
            ctx=ctx,
            vertex_config=vertex_config,
            edge=edge,
            lindex=lindex,
        ):
            emitted_pairs.add((edge.source, edge.target))
    ctx.edge_requests = []
    ctx.extraction.edge_intents = []

    if not infer_edges:
        return

    populated = {v for v, dd in ctx.acc_vertex.items() if any(dd.values())}
    for edge_id, edge in edge_config.edges_items():
        s, t, _ = edge_id
        if (s, t) in emitted_pairs or s not in populated or t not in populated:
            continue
        if not _is_inference_allowed(
            edge_id,
            infer_edge_only=infer_edge_only,
            infer_edge_except=infer_edge_except,
        ):
            continue
        if _emit_edge_documents(
            ctx=ctx,
            vertex_config=vertex_config,
            edge=edge,
            lindex=None,
        ):
            emitted_pairs.add((s, t))
