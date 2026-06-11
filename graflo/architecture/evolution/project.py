"""Pure planning for :class:`~graflo.architecture.evolution.ops.ProjectManifestOp`."""

from __future__ import annotations

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId

from .ops import EdgeSelector, ProjectManifestOp


class ProjectionPlan(ConfigBaseModel):
    """Survivor and removal sets computed from a projection op."""

    surviving_vertices: set[str]
    surviving_edge_ids: set[EdgeId]
    removed_vertices: set[str]
    removed_edge_ids: set[EdgeId]


def _selector_edge_ids(selectors: list[EdgeSelector]) -> set[EdgeId]:
    return {selector.edge_id() for selector in selectors}


def compute_projection(
    manifest: GraphManifest, op: ProjectManifestOp
) -> ProjectionPlan:
    """Compute survivor/removal sets without mutating *manifest*."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("project_manifest requires graph_schema")

    all_vertices = schema.core_schema.vertex_config.vertex_set
    all_edges = schema.core_schema.edge_config.edges
    all_edge_ids = {edge.edge_id for edge in all_edges}

    if op.strict:
        if op.keep_vertices:
            missing_vertices = sorted(set(op.keep_vertices) - all_vertices)
            if missing_vertices:
                raise ValueError(
                    f"Unknown vertices in keep_vertices: {missing_vertices}"
                )
        if op.keep_edges:
            requested = _selector_edge_ids(op.keep_edges)
            missing_edges = sorted(requested - all_edge_ids)
            if missing_edges:
                raise ValueError(
                    "Unknown edges in keep_edges: "
                    + ", ".join(
                        f"({source!r}, {target!r}, {relation!r})"
                        for source, target, relation in missing_edges
                    )
                )

    if op.keep_edges is not None:
        keep_edge_ids = _selector_edge_ids(op.keep_edges)
        surviving_edge_ids = keep_edge_ids & all_edge_ids
    else:
        surviving_edge_ids = set(all_edge_ids)

    keep_vertex_set = set(op.keep_vertices) if op.keep_vertices is not None else None

    if keep_vertex_set is not None:
        surviving_edge_ids = {
            edge_id
            for edge_id in surviving_edge_ids
            if edge_id[0] in keep_vertex_set and edge_id[1] in keep_vertex_set
        }

    surviving_vertices: set[str] = set()
    for source, target, _relation in surviving_edge_ids:
        surviving_vertices.add(source)
        surviving_vertices.add(target)

    if keep_vertex_set is not None:
        surviving_vertices &= keep_vertex_set
        if op.connectivity == "induced_prune":
            connected_in_keep = {
                vertex
                for vertex in keep_vertex_set
                if any(
                    source == vertex or target == vertex
                    for source, target, _relation in surviving_edge_ids
                )
            }
            surviving_vertices = connected_in_keep
            surviving_edge_ids = {
                edge_id
                for edge_id in surviving_edge_ids
                if edge_id[0] in surviving_vertices and edge_id[1] in surviving_vertices
            }

    removed_vertices = all_vertices - surviving_vertices
    removed_edge_ids = all_edge_ids - surviving_edge_ids

    return ProjectionPlan(
        surviving_vertices=surviving_vertices,
        surviving_edge_ids=surviving_edge_ids,
        removed_vertices=removed_vertices,
        removed_edge_ids=removed_edge_ids,
    )
