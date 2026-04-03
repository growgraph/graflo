"""Assembly phase for turning extracted observations into graph edges."""

from __future__ import annotations

from typing import Any

from .actor.edge_render import render_edge, render_weights
from graflo.architecture.contract.declarations.edge_derivation_registry import (
    EdgeDerivationRegistry,
)
from graflo.architecture.schema.edge import (
    DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME,
    Edge,
    EdgeConfig,
)
from graflo.architecture.edge_derivation import EdgeDerivation
from graflo.architecture.graph_types import AssemblyContext, EdgeId, LocationIndex
from graflo.architecture.schema.vertex import VertexConfig
from graflo.onto import DBType
from graflo.util.merge import merge_doc_basis


def _resolved_relation_input_field(
    edge: Edge,
    *,
    derivation: EdgeDerivation | None,
    target_db_flavor: DBType | None,
) -> str | None:
    """Document/ctx field used to read per-row relation when schema relation is unset."""
    if edge.relation is not None:
        return None
    if derivation is not None and derivation.relation_field is not None:
        return derivation.relation_field
    if target_db_flavor == DBType.TIGERGRAPH:
        return DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME
    return None


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
    relation_input_field: str | None = None,
    derivation: EdgeDerivation | None = None,
    edge_derivation: EdgeDerivationRegistry | None = None,
) -> bool:
    _merge_vertices_for_edge(ctx, vertex_config, edge.source, edge.target)
    edges = render_edge(
        edge=edge,
        vertex_config=vertex_config,
        ctx=ctx,
        lindex=lindex,
        relation_input_field=relation_input_field,
        derivation=derivation,
    )
    vertex_rules: list = []
    if edge_derivation is not None:
        vertex_rules = edge_derivation.vertex_weights_for(edge.edge_id)
    edges = render_weights(
        edge, vertex_config, ctx.acc_vertex, edges, vertex_weights=vertex_rules
    )
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
    target_db_flavor: DBType | None = None,
    edge_derivation: EdgeDerivationRegistry | None = None,
) -> None:
    """Assemble all edge documents after extraction finishes."""
    if infer_edge_only is None:
        infer_edge_only = set()
    if infer_edge_except is None:
        infer_edge_except = set()

    emitted_pairs: set[tuple[str, str]] = set()

    if ctx.edge_intents:
        for intent in ctx.edge_intents:
            edge = intent.edge
            relation_input = _resolved_relation_input_field(
                edge,
                derivation=intent.derivation,
                target_db_flavor=target_db_flavor,
            )
            if _emit_edge_documents(
                ctx=ctx,
                vertex_config=vertex_config,
                edge=edge,
                lindex=intent.location,
                relation_input_field=relation_input,
                derivation=intent.derivation,
                edge_derivation=edge_derivation,
            ):
                emitted_pairs.add((edge.source, edge.target))
    else:
        for item in ctx.edge_requests:
            edge = item[0]
            lindex = item[1]
            relation_input = _resolved_relation_input_field(
                edge,
                derivation=None,
                target_db_flavor=target_db_flavor,
            )
            if _emit_edge_documents(
                ctx=ctx,
                vertex_config=vertex_config,
                edge=edge,
                lindex=lindex,
                relation_input_field=relation_input,
                derivation=None,
                edge_derivation=edge_derivation,
            ):
                emitted_pairs.add((edge.source, edge.target))
    ctx.edge_requests = []
    ctx.extraction.edge_intents = []

    if not infer_edges:
        return

    populated = {v for v, dd in ctx.acc_vertex.items() if any(dd.values())}
    for edge_id, edge in edge_config.items():
        s, t, _ = edge_id
        if (s, t) in emitted_pairs or s not in populated or t not in populated:
            continue
        if not _is_inference_allowed(
            edge_id,
            infer_edge_only=infer_edge_only,
            infer_edge_except=infer_edge_except,
        ):
            continue
        relation_input = _resolved_relation_input_field(
            edge,
            derivation=None,
            target_db_flavor=target_db_flavor,
        )
        if _emit_edge_documents(
            ctx=ctx,
            vertex_config=vertex_config,
            edge=edge,
            lindex=None,
            relation_input_field=relation_input,
            derivation=None,
            edge_derivation=edge_derivation,
        ):
            emitted_pairs.add((s, t))
