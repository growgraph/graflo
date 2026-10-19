"""Planning the access aspect: a reserved ``access_zones`` field on named types.

Each vertex type is labelled with a merge mode for its zones: ``replace`` (an
upsert sets them) or ``union`` (an upsert keeps every zone stored and written).
Edges always replace: a union is not written on edges.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.vertex import Field, FieldMerge, FieldType

from ...ops import AddEdgePropertiesOp, AddVertexPropertiesOp, ManifestOp
from ..base import LiftError

#: The field the lift adds.
ACCESS_ZONES = "access_zones"


def access_field(merge: FieldMerge = "replace") -> Field:
    """``access_zones``: a reserved list of zone ids, set by the writer's stamp."""
    return Field(
        name=ACCESS_ZONES,
        type=FieldType.LIST,
        item_type=FieldType.INT,
        reserved=True,
        merge=merge,
        description=(
            "Ids of the access zones this element is visible in; set by the "
            "writer from its stamp."
        ),
    )


def plan_access(
    manifest: GraphManifest,
    *,
    vertices: Mapping[str, FieldMerge],
    edges: Collection[str],
    minted_vertices: Iterable[str] = (),
    minted_relations: Iterable[str] = (),
) -> list[ManifestOp]:
    """The ops that label *vertices* and *edges* with ``access_zones``.

    Args:
        manifest: the manifest to lift, already ``finish_init()``-ed.
        vertices: vertex type -> merge mode of its zones.
        edges: edge relations to label; their zones are replaced on upsert.
        minted_vertices: types the same lift adds before these ops apply.
        minted_relations: relations the same lift adds before these ops apply.

    Raises:
        LiftError: nothing to label, a type or relation the manifest does not
            have, or one that already declares the field.
    """
    if not vertices and not edges:
        raise LiftError(
            "the access aspect needs access_for or access_edges: it labels only "
            "the types a spec names"
        )
    schema = manifest.graph_schema
    if schema is None:
        raise LiftError("cannot lift a manifest with no schema block")
    core = schema.core_schema
    declared_vertices = {v.name: v for v in core.vertex_config.vertices}
    declared_relations: dict[str, list[str]] = {}
    for edge in core.edge_config.values():
        if edge.relation is not None:
            declared_relations.setdefault(edge.relation, []).extend(
                f.name for f in edge.properties
            )

    unknown = sorted(
        set(vertices) - set(declared_vertices) - set(minted_vertices)
    ) + sorted(set(edges) - set(declared_relations) - set(minted_relations))
    if unknown:
        raise LiftError(
            f"access names types or relations the manifest lacks: {unknown}"
        )
    labelled = sorted(
        name
        for name in vertices
        if name in declared_vertices
        and ACCESS_ZONES in {f.name for f in declared_vertices[name].properties}
    ) + sorted(
        relation
        for relation in edges
        if ACCESS_ZONES in declared_relations.get(relation, [])
    )
    if labelled:
        raise LiftError(f"already declare {ACCESS_ZONES}: {labelled}")

    ops: list[ManifestOp] = []
    if vertices:
        ops.append(
            AddVertexPropertiesOp(
                additions={
                    name: [access_field(merge)]
                    for name, merge in sorted(vertices.items())
                }
            )
        )
    if edges:
        ops.append(
            AddEdgePropertiesOp(
                additions={relation: [access_field()] for relation in sorted(edges)}
            )
        )
    return ops


__all__ = ["ACCESS_ZONES", "access_field", "plan_access"]
