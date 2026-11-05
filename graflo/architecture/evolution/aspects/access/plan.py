"""Planning the access aspect: a reserved ``access_zones`` field on named types.

Each vertex type and edge relation is labelled with a merge mode for its
zones: ``replace`` (an upsert sets them) or ``union`` (an upsert keeps every
zone stored and written).
"""

from __future__ import annotations

from collections.abc import Mapping

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.vertex import Field, FieldMerge, FieldType

from ...ops import ManifestOp
from ..base import LiftError
from ..join import join

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
    edges: Mapping[str, FieldMerge],
) -> list[ManifestOp]:
    """The ops that label *vertices* and *edges* with ``access_zones``.

    A type that already declares the field keeps it when it is the same field;
    one declared with another type or merge mode is a contradiction
    (:func:`~graflo.architecture.evolution.aspects.join.join`).

    Args:
        manifest: the manifest to lift, already ``finish_init()``-ed, with the
            types and relations it names.
        vertices: vertex type -> merge mode of its zones.
        edges: edge relation -> merge mode of its zones.

    Raises:
        LiftError: nothing to label, a type or relation the manifest does not
            have, or a field that contradicts the aspect's.
    """
    if not vertices and not edges:
        raise LiftError(
            "the access aspect needs access_for or access_edges: it labels only "
            "the types a spec names"
        )
    schema = manifest.graph_schema
    if schema is None:
        raise LiftError("cannot lift a manifest with no schema block")
    return join(
        schema.core_schema,
        vertex_fields={
            name: [access_field(merge)] for name, merge in sorted(vertices.items())
        },
        relation_fields={
            relation: [access_field(merge)] for relation, merge in sorted(edges.items())
        },
    )


__all__ = ["ACCESS_ZONES", "access_field", "plan_access"]
