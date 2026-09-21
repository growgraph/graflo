"""Pure planning for :class:`~graflo.architecture.evolution.ops.ProjectManifestOp`.

Selector validation and manifest unwrapping live here; the induced-connectivity
kernel itself lives at layer 2 in
:mod:`graflo.architecture.schema.projection` so manifest projection and schema
context projection cannot drift apart. The ``depth`` walk borrows the same way,
from :mod:`graflo.architecture.schema.context.graph` — the hop machinery is shared
with rank-then-budget schema context rather than reimplemented here.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.context.graph import (
    SchemaGraph,
    neighborhood_distances,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.inverse_realization import (
    materialized_inverse_id,
)
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


def _expand_seeds(
    schema: Schema,
    op: ProjectManifestOp,
    keep_edge_ids: set[EdgeId] | None,
) -> list[str]:
    """Vertex types within ``op.depth`` hops of ``op.keep_vertices``.

    ``keep_edge_ids`` bounds the walk as well as the result: an explicit edge
    whitelist and a neighbourhood expansion would otherwise give contradictory
    answers about which edges belong in the slice.

    Only the *vertex* set is returned. Handing the traversed edges to
    ``select_induced`` instead would produce a breadth-first tree rather than the
    induced subgraph on the hop ball — ``schema_neighbors`` never records an edge
    between two types both sitting at exactly ``depth``, because neither is
    expanded from.
    """
    graph = SchemaGraph.from_schema(schema)
    # Under `strict=False` an undeclared name is silently ignored (the kernel
    # intersects it away), so filter before walking — `schema_neighbors` raises.
    seeds = [name for name in op.keep_vertices or [] if name in graph.vertex_types]
    distances = neighborhood_distances(
        graph,
        seeds,
        hops=op.depth,
        direction=op.direction,
        edge_ids=keep_edge_ids,
    )
    return sorted(distances)


def compute_projection(
    manifest: GraphManifest, op: ProjectManifestOp
) -> SubschemaSelection:
    """Compute survivor/removal sets without mutating *manifest*."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("project_manifest requires graph_schema")

    if op.strict:
        _validate_strict(manifest, op)

    keep_edge_ids = (
        _selector_edge_ids(op.keep_edges) if op.keep_edges is not None else None
    )
    if keep_edge_ids is not None and op.keep_inverse_edges:
        # A materialized pair is two declared edges; keeping one reading of the
        # fact and dropping the other would leave a schema that states half of it.
        edge_config = schema.core_schema.edge_config
        keep_edge_ids = keep_edge_ids | {
            mirror
            for edge_id in keep_edge_ids
            if (mirror := materialized_inverse_id(edge_config, edge_id)) is not None
            and mirror in edge_config
        }
    keep_vertices = op.keep_vertices
    if op.depth > 0:
        keep_vertices = _expand_seeds(schema, op, keep_edge_ids)

    return select_induced(
        schema.core_schema,
        keep_vertices=keep_vertices,
        keep_edge_ids=keep_edge_ids,
        connectivity=op.connectivity,
    )
