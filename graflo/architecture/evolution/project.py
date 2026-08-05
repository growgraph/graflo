"""Pure planning for :class:`~graflo.architecture.evolution.ops.ProjectManifestOp`.

Selector validation and manifest unwrapping live here; the induced-connectivity
kernel itself lives at layer 2 in
:mod:`graflo.architecture.schema.projection` so manifest projection and schema
context projection cannot drift apart.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.projection import SubschemaSelection, select_induced

from .ops import EdgeSelector, ProjectManifestOp


def _selector_edge_ids(
    selectors: list[EdgeSelector],
) -> set[tuple[str, str, str | None]]:
    return {selector.edge_id() for selector in selectors}


def _validate_strict(manifest: GraphManifest, op: ProjectManifestOp) -> None:
    """Raise when strict mode is on and a selector names something undeclared."""
    schema = manifest.require_schema()
    all_vertices = schema.core_schema.vertex_config.vertex_set
    all_edge_ids = {edge.edge_id for edge in schema.core_schema.edge_config.edges}

    if op.keep_vertices:
        missing_vertices = sorted(set(op.keep_vertices) - all_vertices)
        if missing_vertices:
            raise ValueError(f"Unknown vertices in keep_vertices: {missing_vertices}")
    if op.keep_edges:
        missing_edges = sorted(_selector_edge_ids(op.keep_edges) - all_edge_ids)
        if missing_edges:
            raise ValueError(
                "Unknown edges in keep_edges: "
                + ", ".join(
                    f"({source!r}, {target!r}, {relation!r})"
                    for source, target, relation in missing_edges
                )
            )


def compute_projection(
    manifest: GraphManifest, op: ProjectManifestOp
) -> SubschemaSelection:
    """Compute survivor/removal sets without mutating *manifest*."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("project_manifest requires graph_schema")

    if op.strict:
        _validate_strict(manifest, op)

    return select_induced(
        schema.core_schema,
        keep_vertices=op.keep_vertices,
        keep_edge_ids=(
            _selector_edge_ids(op.keep_edges) if op.keep_edges is not None else None
        ),
        connectivity=op.connectivity,
    )
