"""Structure-plane evolution ops: introducing and retargeting graph entities.

The contract vocabulary could remove, merge, rename and project, but not *create*.
Everything here closes that half of the loop so a change set can describe a growing
graph without falling back to a binary compose.
"""

from __future__ import annotations

import logging

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId

from .ops import AddEdgesOp, AddVerticesOp, RetargetEdgesOp

logger = logging.getLogger(__name__)


def apply_add_vertices(manifest: GraphManifest, op: AddVerticesOp) -> None:
    """Add new logical vertex types to the schema."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("add_vertices requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    existing = vertex_config.vertex_set
    collisions = sorted(
        vertex.name for vertex in op.vertices if vertex.name in existing
    )
    if collisions:
        raise ValueError(
            f"add_vertices: vertices already exist: {collisions}; use "
            "RenameVerticesOp or MergeVerticesOp instead"
        )

    vertex_config.vertices = [
        *vertex_config.vertices,
        *(vertex.model_copy(deep=True) for vertex in op.vertices),
    ]
    schema.finish_init()


def apply_add_edges(manifest: GraphManifest, op: AddEdgesOp) -> None:
    """Add new logical edge relations between existing vertex types."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("add_edges requires graph_schema")

    edge_config = schema.core_schema.edge_config
    known_vertices = schema.core_schema.vertex_config.vertex_set
    existing_ids = {edge.edge_id for edge in edge_config.edges}

    collisions = sorted(
        str((edge.source, edge.target, edge.relation))
        for edge in op.edges
        if (edge.source, edge.target, edge.relation) in existing_ids
    )
    if collisions:
        raise ValueError(f"add_edges: edges already exist: {collisions}")

    unknown = sorted(
        {
            name
            for edge in op.edges
            for name in (edge.source, edge.target)
            if name not in known_vertices
        }
    )
    if unknown:
        raise ValueError(
            f"add_edges: unknown endpoint vertex types: {unknown}; add them with "
            "AddVerticesOp first"
        )

    edge_config.edges = [
        *edge_config.edges,
        *(edge.model_copy(deep=True) for edge in op.edges),
    ]
    schema.finish_init()


def apply_retarget_edges(manifest: GraphManifest, op: RetargetEdgesOp) -> None:
    """Repoint edges at different endpoint vertex types, keeping everything else.

    Mutates *manifest* in place: rewrites the edge's ``source`` / ``target``, moves the
    matching ``db_profile`` physical specs onto the new ``EdgeId``, and repoints
    pipeline edge steps that named the old triple.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("retarget_edges requires graph_schema")

    edge_config = schema.core_schema.edge_config
    by_edge_id = {edge.edge_id: edge for edge in edge_config.edges}
    known_vertices = schema.core_schema.vertex_config.vertex_set

    unknown_edges = sorted(
        str(entry.edge_id()) for entry in op.edges if entry.edge_id() not in by_edge_id
    )
    if unknown_edges:
        raise ValueError(f"retarget_edges: unknown edges: {unknown_edges}")

    unknown_vertices = sorted(
        {
            name
            for entry in op.edges
            for name in (entry.new_source, entry.new_target)
            if name is not None and name not in known_vertices
        }
    )
    if unknown_vertices:
        raise ValueError(
            f"retarget_edges: unknown endpoint vertex types: {unknown_vertices}"
        )

    collisions = sorted(
        str(entry.retargeted_edge_id())
        for entry in op.edges
        if entry.retargeted_edge_id() in by_edge_id
    )
    if collisions:
        raise ValueError(
            f"retarget_edges: retargeted edges collide with existing ones: "
            f"{collisions}; use MergeEdgesOp to combine them"
        )

    mapping: dict[EdgeId, tuple[str, str]] = {}
    for entry in op.edges:
        edge = by_edge_id[entry.edge_id()]
        new_source, new_target, _ = entry.retargeted_edge_id()
        mapping[entry.edge_id()] = (new_source, new_target)

        for spec in schema.db_profile.edge_specs:
            if (spec.source, spec.target, spec.relation) == entry.edge_id():
                spec.source = new_source
                spec.target = new_target

        edge.source = new_source
        edge.target = new_target
        logger.debug(
            "retarget_edges: %s -> %s", entry.edge_id(), entry.retargeted_edge_id()
        )

    # ``EdgeConfig._edges_map`` is a private index keyed on EdgeId and rebuilt by a
    # model validator. Mutating an edge's endpoints in place leaves it keyed on the
    # pre-retarget id, which the Schema-level spec check then reads as a dangling
    # reference. Reassigning the list re-runs the validator and rebuilds the index.
    edge_config.edges = list(edge_config.edges)

    schema.db_profile = _revalidated_profile(schema)
    schema.finish_init()

    if manifest.ingestion_model is not None:
        from .apply import _rebuild_ingestion_with_pipeline_rewrite
        from .rewrite import rewrite_edge_endpoints_in_pipeline

        _rebuild_ingestion_with_pipeline_rewrite(
            manifest,
            lambda pipeline: rewrite_edge_endpoints_in_pipeline(pipeline, mapping),
        )


def _revalidated_profile(schema):
    """Re-run ``DatabaseProfile`` validators after in-place edge_spec endpoint edits."""
    from .apply import _revalidate_db_profile

    return _revalidate_db_profile(schema.db_profile)
