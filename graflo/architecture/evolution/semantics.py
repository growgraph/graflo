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
from graflo.architecture.refusal import OpRefusal

from .ops import (
    EdgeFieldSemanticsTarget,
    FieldSemanticsTarget,
    SetEdgeDescriptionsOp,
    SetEdgeSemanticsOp,
    SetEdgeSubpropertyOfOp,
    SetFieldSemanticsOp,
    SetVertexDescriptionsOp,
    SetVertexSemanticsOp,
    SetVertexSubclassOfOp,
)
from .refusal_subjects import edge_subject, edges, names, prop_subject

logger = logging.getLogger(__name__)


def apply_set_vertex_semantics(
    manifest: GraphManifest, op: SetVertexSemanticsOp
) -> None:
    """Attach, replace or clear the grounding on selected vertex types."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_vertex_semantics requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.semantics) - vertex_config.vertex_set)
    if unknown:
        raise OpRefusal(
            f"set_vertex_semantics: unknown vertices: {unknown}",
            check="unknown-vertex",
            subjects=names(unknown),
        )

    for vertex in vertex_config.vertices:
        if vertex.name not in op.semantics:
            continue
        value = op.semantics[vertex.name]
        vertex.semantics = value.model_copy(deep=True) if value is not None else None

    schema.finish_init()


def apply_set_vertex_subclass_of(
    manifest: GraphManifest, op: SetVertexSubclassOfOp
) -> None:
    """Declare, replace or clear the superclasses of selected vertex types."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_vertex_subclass_of requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.subclass_of) - vertex_config.vertex_set)
    if unknown:
        raise OpRefusal(
            f"set_vertex_subclass_of: unknown vertices: {unknown}",
            check="unknown-vertex",
            subjects=names(unknown),
        )

    for vertex in vertex_config.vertices:
        if vertex.name in op.subclass_of:
            vertex.subclass_of = [
                p.model_copy(deep=True) for p in op.subclass_of[vertex.name]
            ]

    try:
        schema.finish_init()
    except ValueError as exc:
        # A self parent or a cycle is only visible on the whole hierarchy.
        raise OpRefusal(
            str(exc), check="invalid-result", subjects=names(op.subclass_of)
        ) from exc


def apply_set_vertex_descriptions(
    manifest: GraphManifest, op: SetVertexDescriptionsOp
) -> None:
    """Set, replace or clear the description on selected vertex types."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_vertex_descriptions requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.descriptions) - vertex_config.vertex_set)
    if unknown:
        raise OpRefusal(
            f"set_vertex_descriptions: unknown vertices: {unknown}",
            check="unknown-vertex",
            subjects=names(unknown),
        )

    for vertex in vertex_config.vertices:
        if vertex.name in op.descriptions:
            vertex.description = op.descriptions[vertex.name]

    schema.finish_init()


def apply_set_edge_descriptions(
    manifest: GraphManifest, op: SetEdgeDescriptionsOp
) -> None:
    """Set, replace or clear the description on selected edges."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_edge_descriptions requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    by_edge_id = {edge.edge_id: edge for edge in schema.core_schema.edge_config.edges}
    unknown_ids = [
        target.edge_id() for target in op.edges if target.edge_id() not in by_edge_id
    ]
    unknown = sorted(str(edge_id) for edge_id in unknown_ids)
    if unknown:
        raise OpRefusal(
            f"set_edge_descriptions: unknown edges: {unknown}",
            check="unknown-edge",
            subjects=edges(unknown_ids),
        )

    for target in op.edges:
        by_edge_id[target.edge_id()].description = target.description

    schema.finish_init()


def apply_set_edge_subproperty_of(
    manifest: GraphManifest, op: SetEdgeSubpropertyOfOp
) -> None:
    """Declare, replace or clear the superproperties of selected edges."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_edge_subproperty_of requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    by_edge_id = {edge.edge_id: edge for edge in schema.core_schema.edge_config.edges}
    unknown_ids = [
        target.edge_id() for target in op.edges if target.edge_id() not in by_edge_id
    ]
    unknown = sorted(str(edge_id) for edge_id in unknown_ids)
    if unknown:
        raise OpRefusal(
            f"set_edge_subproperty_of: unknown edges: {unknown}",
            check="unknown-edge",
            subjects=edges(unknown_ids),
        )

    for target in op.edges:
        by_edge_id[target.edge_id()].subproperty_of = [
            p.model_copy(deep=True) for p in target.subproperty_of
        ]

    try:
        schema.finish_init()
    except ValueError as exc:
        # A self parent, a cycle or a parent whose endpoints do not subsume
        # the child's is only visible on the whole hierarchy.
        raise OpRefusal(
            str(exc),
            check="invalid-result",
            subjects=edges(target.edge_id() for target in op.edges),
        ) from exc


def apply_set_edge_semantics(manifest: GraphManifest, op: SetEdgeSemanticsOp) -> None:
    """Attach, replace or clear the grounding on selected edge relations."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_edge_semantics requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    by_edge_id = {edge.edge_id: edge for edge in schema.core_schema.edge_config.edges}
    unknown_ids = [
        selector.edge_id()
        for selector in op.edges
        if selector.edge_id() not in by_edge_id
    ]
    unknown = sorted(str(edge_id) for edge_id in unknown_ids)
    if unknown:
        raise OpRefusal(
            f"set_edge_semantics: unknown edges: {unknown}",
            check="unknown-edge",
            subjects=edges(unknown_ids),
        )

    for selector in op.edges:
        edge = by_edge_id[selector.edge_id()]
        edge.semantics = (
            op.semantics.model_copy(deep=True) if op.semantics is not None else None
        )

    schema.finish_init()


def apply_set_field_semantics(manifest: GraphManifest, op: SetFieldSemanticsOp) -> None:
    """Attach, replace or clear the grounding on selected vertex or edge properties."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_field_semantics requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    vertex_config = schema.core_schema.vertex_config
    vertex_targets = [t for t in op.targets if isinstance(t, FieldSemanticsTarget)]
    edge_targets = [t for t in op.targets if isinstance(t, EdgeFieldSemanticsTarget)]

    unknown_vertices = sorted(
        {target.vertex for target in vertex_targets} - vertex_config.vertex_set
    )
    if unknown_vertices:
        raise OpRefusal(
            f"set_field_semantics: unknown vertices: {unknown_vertices}",
            check="unknown-vertex",
            subjects=names(unknown_vertices),
        )
    by_edge_id = {edge.edge_id: edge for edge in schema.core_schema.edge_config.edges}
    unknown_edge_ids = [
        target.edge_id()
        for target in edge_targets
        if target.edge_id() not in by_edge_id
    ]
    unknown_edges = sorted(str(edge_id) for edge_id in unknown_edge_ids)
    if unknown_edges:
        raise OpRefusal(
            f"set_field_semantics: unknown edges: {unknown_edges}",
            check="unknown-edge",
            subjects=edges(unknown_edge_ids),
        )

    by_name = {vertex.name: vertex for vertex in vertex_config.vertices}
    holders = [(target, by_name[target.vertex]) for target in vertex_targets] + [
        (target, by_edge_id[target.edge_id()]) for target in edge_targets
    ]
    missing: list[str] = []
    missing_subjects: list[str] = []
    for target, holder in holders:
        if target.field not in {field.name for field in holder.properties}:
            if isinstance(target, FieldSemanticsTarget):
                label = target.vertex
                owner = target.vertex
            else:
                label = str(target.edge_id())
                owner = edge_subject(*target.edge_id())
            missing.append(f"{label}.{target.field}")
            missing_subjects.append(prop_subject(owner, target.field))
    if missing:
        # Naming the property rather than the vertex: the likely mistake is a
        # stale field name after a rename, and "unknown vertex" would send the
        # reader looking in the wrong place.
        raise OpRefusal(
            f"set_field_semantics: unknown properties: {sorted(missing)}",
            check="unknown-property",
            subjects=names(missing_subjects),
        )

    for target, holder in holders:
        for field in holder.properties:
            if field.name != target.field:
                continue
            field.semantics = (
                target.semantics.model_copy(deep=True)
                if target.semantics is not None
                else None
            )

    schema.finish_init()


__all__ = [
    "apply_set_edge_descriptions",
    "apply_set_edge_semantics",
    "apply_set_edge_subproperty_of",
    "apply_set_field_semantics",
    "apply_set_vertex_descriptions",
    "apply_set_vertex_semantics",
    "apply_set_vertex_subclass_of",
]
