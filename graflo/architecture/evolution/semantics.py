"""Applying grounding changes to a manifest.

Grounding was authorable only at the moment a type was written: the schema
models carry ``semantics`` blocks, but no operation could attach one to an
element that already existed. A manifest that arrived ungrounded -- an inferred
one, or anything authored before the block existed -- could therefore never be
grounded through the op system at all, only rewritten by hand, which is not
replayable and not reviewable.

These three handlers close that. They are deliberately *not* in ``physical.py``:
grounding is never consulted at execution time and has no physical projection,
so it changes what a reader can learn from the schema and nothing else.
"""

from __future__ import annotations

import logging

from graflo.architecture.contract.manifest import GraphManifest

from .ops import (
    SetEdgeSemanticsOp,
    SetFieldSemanticsOp,
    SetVertexSemanticsOp,
)

logger = logging.getLogger(__name__)


def apply_set_vertex_semantics(
    manifest: GraphManifest, op: SetVertexSemanticsOp
) -> None:
    """Attach, replace or clear the grounding on selected vertex types."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("set_vertex_semantics requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.semantics) - vertex_config.vertex_set)
    if unknown:
        raise ValueError(f"set_vertex_semantics: unknown vertices: {unknown}")

    for vertex in vertex_config.vertices:
        if vertex.name not in op.semantics:
            continue
        value = op.semantics[vertex.name]
        vertex.semantics = value.model_copy(deep=True) if value is not None else None

    schema.finish_init()


def apply_set_edge_semantics(manifest: GraphManifest, op: SetEdgeSemanticsOp) -> None:
    """Attach, replace or clear the grounding on selected edge relations."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("set_edge_semantics requires graph_schema")

    by_edge_id = {edge.edge_id: edge for edge in schema.core_schema.edge_config.edges}
    unknown = sorted(
        str(selector.edge_id())
        for selector in op.edges
        if selector.edge_id() not in by_edge_id
    )
    if unknown:
        raise ValueError(f"set_edge_semantics: unknown edges: {unknown}")

    for selector in op.edges:
        edge = by_edge_id[selector.edge_id()]
        edge.semantics = (
            op.semantics.model_copy(deep=True) if op.semantics is not None else None
        )

    schema.finish_init()


def apply_set_field_semantics(manifest: GraphManifest, op: SetFieldSemanticsOp) -> None:
    """Attach, replace or clear the grounding on selected vertex properties."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("set_field_semantics requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    unknown_vertices = sorted(
        {target.vertex for target in op.targets} - vertex_config.vertex_set
    )
    if unknown_vertices:
        raise ValueError(f"set_field_semantics: unknown vertices: {unknown_vertices}")

    by_name = {vertex.name: vertex for vertex in vertex_config.vertices}
    missing: list[str] = []
    for target in op.targets:
        vertex = by_name[target.vertex]
        if target.field not in {field.name for field in vertex.properties}:
            missing.append(f"{target.vertex}.{target.field}")
    if missing:
        # Naming the property rather than the vertex: the likely mistake is a
        # stale field name after a rename, and "unknown vertex" would send the
        # reader looking in the wrong place.
        raise ValueError(f"set_field_semantics: unknown properties: {sorted(missing)}")

    for target in op.targets:
        vertex = by_name[target.vertex]
        for field in vertex.properties:
            if field.name != target.field:
                continue
            field.semantics = (
                target.semantics.model_copy(deep=True)
                if target.semantics is not None
                else None
            )

    schema.finish_init()


__all__ = [
    "apply_set_edge_semantics",
    "apply_set_field_semantics",
    "apply_set_vertex_semantics",
]
