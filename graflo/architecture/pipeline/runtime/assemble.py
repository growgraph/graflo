"""Assembly phase for turning extracted observations into graph edges."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from graflo.architecture.graph_types import AssemblyContext, EdgeId, LocationIndex
from graflo.architecture.graph_types.edge_derivation import (
    EdgeDerivation,
    EdgeDerivationRegistry,
)
from graflo.architecture.graph_types.merge import fuse_doc_basis
from graflo.architecture.schema.edge import (
    DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME,
    Edge,
    EdgeConfig,
    inverse_map,
)
from graflo.architecture.schema.vertex import VertexConfig
from graflo.onto import DBType

from .actor.edge_render import render_edge, render_weights

logger = logging.getLogger(__name__)

#: ``(source, target, relation)`` triples already reported as unmirrorable, so a
#: step that cannot mirror says so once rather than once per document.
_reported_unmirrored: set[EdgeId] = set()


def mirrored_edge_id(
    edge_id: EdgeId,
    *,
    inverse_pairs: Mapping[str, str],
    edge_config: EdgeConfig,
) -> EdgeId | None:
    """The declared inverse edge of ``edge_id``, or None when there is nothing to write.

    ``(s, t, a)`` mirrors to ``(t, s, b)`` when ``a`` has a declared inverse ``b``
    **and** that inverse edge (or a relation-less template between the same
    types) is declared. The second condition is what makes a pair
    *materialized*: a pair that is only declared, or that the database
    maintains, has no edge to write into, and an undeclared edge would be
    dropped at the writer without a word.
    """
    source, target, relation = edge_id
    if relation is None:
        return None
    inverse = inverse_pairs.get(relation)
    if inverse is None or inverse == relation:
        return None
    if (target, source, inverse) in edge_config or (
        target,
        source,
        None,
    ) in edge_config:
        return target, source, inverse
    return None


def _resolved_relation_input_field(
    edge: Edge,
    *,
    derivation: EdgeDerivation | None,
    target_db_flavor: DBType | None,
) -> str | None:
    """Document/ctx field used to read per-document relation when schema relation is unset."""
    if edge.relation is not None:
        return None
    if derivation is not None and derivation.relation_field is not None:
        return derivation.relation_field
    if target_db_flavor == DBType.TIGERGRAPH:
        return DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME
    return None


def _fuse_vertices_for_edge(
    ctx: AssemblyContext,
    vertex_config: VertexConfig,
    source: str,
    target: str,
    *,
    source_fields: list[str],
    target_fields: list[str],
) -> None:
    """Merge endpoint observations on the fields they will be matched on.

    Merging on the primary identity would be wrong for endpoints matched by a
    secondary identity: those documents carry no primary key, so every one of
    them would look keyless and collapse into a single document.

    When both endpoints are the same vertex type the bucket is shared, so it is
    merged once on the union of both field-sets — conservative, and it never
    produces the keyless collapse.
    """
    if source == target:
        basis = list(dict.fromkeys([*source_fields, *target_fields]))
        bases: list[tuple[str, list[str]]] = [(source, basis)]
    else:
        bases = [(source, source_fields), (target, target_fields)]

    for vname, fields in bases:
        for lindex, vlist in ctx.acc_vertex[vname].items():
            ctx.acc_vertex[vname][lindex] = fuse_doc_basis(vlist, tuple(fields))


def _emit_edge_documents(
    *,
    ctx: AssemblyContext,
    vertex_config: VertexConfig,
    edge: Any,
    lindex: LocationIndex | None,
    relation_input_field: str | None = None,
    derivation: EdgeDerivation | None = None,
    edge_derivation: EdgeDerivationRegistry | None = None,
    edge_config: EdgeConfig | None = None,
    inverse_pairs: Mapping[str, str] | None = None,
) -> set[tuple[str, str]]:
    """Render one edge intent into ``ctx.acc_global``; the ``(source, target)`` pairs written.

    A step that sets ``emit_inverse`` also writes the declared inverse of each
    relation it resolved (see :func:`mirrored_edge_id`). The mirror is taken
    after weights are rendered, so it carries exactly what the forward edge
    carries, and after the relation is resolved, so it does not matter whether
    the relation was fixed, read from a field, mapped, or taken from a key.
    """
    source_fields = vertex_config.match_fields(
        edge.source, derivation.source_match if derivation is not None else None
    )
    target_fields = vertex_config.match_fields(
        edge.target, derivation.target_match if derivation is not None else None
    )
    _fuse_vertices_for_edge(
        ctx,
        vertex_config,
        edge.source,
        edge.target,
        source_fields=source_fields,
        target_fields=target_fields,
    )
    edges = render_edge(
        edge=edge,
        vertex_config=vertex_config,
        ctx=ctx,
        lindex=lindex,
        relation_input_field=relation_input_field,
        derivation=derivation,
        source_match_fields=source_fields,
        target_match_fields=target_fields,
    )
    vertex_rules: list = []
    if edge_derivation is not None:
        vertex_rules = edge_derivation.vertex_weights_for(edge.edge_id)
    edges = render_weights(
        edge, vertex_config, ctx.acc_vertex, edges, vertex_weights=vertex_rules
    )
    mirror = (
        derivation is not None
        and derivation.emit_inverse
        and edge_config is not None
        and inverse_pairs is not None
    )
    emitted: set[tuple[str, str]] = set()
    for relation, edocs in edges.items():
        ctx.acc_global[edge.source, edge.target, relation] += edocs
        if not edocs:
            continue
        emitted.add((edge.source, edge.target))
        if derivation is not None and derivation.emit_inverse:
            # A step that mirrors is the authority on the reverse reading between
            # these endpoints, for every row it writes -- including a row whose
            # relation has no inverse to mirror. Left to inference, that row
            # would be written under whatever inverse edge happens to be declared.
            emitted.add((edge.target, edge.source))
        if not mirror:
            continue
        assert edge_config is not None and inverse_pairs is not None
        forward_id = (edge.source, edge.target, relation)
        inverse_id = mirrored_edge_id(
            forward_id, inverse_pairs=inverse_pairs, edge_config=edge_config
        )
        if inverse_id is None:
            if forward_id not in _reported_unmirrored:
                _reported_unmirrored.add(forward_id)
                logger.info(
                    "emit_inverse: %s has no materialized inverse (no declared "
                    "pair, or its inverse edge is not declared); nothing is mirrored",
                    forward_id,
                )
            continue
        ctx.acc_global[inverse_id] += [
            (target_doc, source_doc, dict(weight))
            for source_doc, target_doc, weight in edocs
        ]
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
    return not (
        infer_edge_except
        and any(_matches_selector(selector, edge_id) for selector in infer_edge_except)
    )


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
    inverse_pairs: Mapping[str, str] | None = None,
) -> None:
    """Assemble all edge documents after extraction finishes.

    Args:
        inverse_pairs: ``{relation: declared inverse}`` for paired relations,
            symmetric ones excluded. Computed from ``edge_config`` when omitted;
            a caller assembling many documents passes it once.
    """
    if infer_edge_only is None:
        infer_edge_only = set()
    if infer_edge_except is None:
        infer_edge_except = set()
    if inverse_pairs is None and any(
        intent.derivation is not None and intent.derivation.emit_inverse
        for intent in ctx.edge_intents
    ):
        inverse_pairs = inverse_map(edge_config.inverses)

    # Pairs an explicit edge actor already produced -- mirrored ones included.
    # Inference skips them so it does not duplicate authored edges — the same
    # intent as the documented per-resource auto-exclusion, applied at
    # (source, target) granularity.
    explicit_pairs: set[tuple[str, str]] = set()

    for intent in ctx.edge_intents:
        edge = intent.edge
        relation_input = _resolved_relation_input_field(
            edge,
            derivation=intent.derivation,
            target_db_flavor=target_db_flavor,
        )
        explicit_pairs |= _emit_edge_documents(
            ctx=ctx,
            vertex_config=vertex_config,
            edge=edge,
            lindex=intent.location,
            relation_input_field=relation_input,
            derivation=intent.derivation,
            edge_derivation=edge_derivation,
            edge_config=edge_config,
            inverse_pairs=inverse_pairs,
        )
    ctx.extraction.edge_intents = []

    if not infer_edges:
        return

    populated = {v for v, dd in ctx.acc_vertex.items() if any(dd.values())}
    for edge_id, edge in edge_config.items():
        s, t, _ = edge_id
        if (s, t) in explicit_pairs or s not in populated or t not in populated:
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
        # Deliberately does not record (s, t): `edge_config.items()` yields each
        # edge_id once, so recording the pair could only ever suppress a *different*
        # relation between the same two vertex types. Two declared relations on one
        # pair are both inferable, and dropping one of them by iteration order was
        # silent edge loss.
        _emit_edge_documents(
            ctx=ctx,
            vertex_config=vertex_config,
            edge=edge,
            lindex=None,
            relation_input_field=relation_input,
            derivation=None,
            edge_derivation=edge_derivation,
        )
