"""Assembly phase for turning extracted observations into graph edges."""

from __future__ import annotations

from typing import Any

from graflo.architecture.actor_util import render_edge, render_weights
from graflo.architecture.edge import EdgeConfig
from graflo.architecture.onto import ActionContext, LocationIndex
from graflo.architecture.vertex import VertexConfig
from graflo.util.merge import merge_doc_basis


def _merge_vertices_for_edge(
    ctx: ActionContext, vertex_config: VertexConfig, source: str, target: str
) -> None:
    for vname in (source, target):
        for lindex, vlist in ctx.acc_vertex[vname].items():
            ctx.acc_vertex[vname][lindex] = merge_doc_basis(
                vlist, tuple(vertex_config.index(vname).fields)
            )


def _emit_edge_documents(
    *,
    ctx: ActionContext,
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


def assemble_edges(
    *,
    ctx: ActionContext,
    vertex_config: VertexConfig,
    edge_config: EdgeConfig,
    edge_greedy: bool,
) -> None:
    """Assemble all edge documents after extraction finishes."""
    emitted_pairs: set[tuple[str, str]] = set()

    for edge, lindex in ctx.edge_requests:
        if _emit_edge_documents(
            ctx=ctx,
            vertex_config=vertex_config,
            edge=edge,
            lindex=lindex,
        ):
            emitted_pairs.add((edge.source, edge.target))
    ctx.edge_requests = []

    if not edge_greedy:
        return

    populated = {v for v, dd in ctx.acc_vertex.items() if any(dd.values())}
    for edge_id, edge in edge_config.edges_items():
        s, t, _ = edge_id
        if (s, t) in emitted_pairs or s not in populated or t not in populated:
            continue
        if _emit_edge_documents(
            ctx=ctx,
            vertex_config=vertex_config,
            edge=edge,
            lindex=None,
        ):
            emitted_pairs.add((s, t))
