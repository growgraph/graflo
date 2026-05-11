"""Typed manifest evolution operations."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import DBType


class RemoveVerticesOp(ConfigBaseModel):
    """Remove logical vertices and cascade: edges, ingestion resources, bindings."""

    op: Literal["remove_vertices"] = "remove_vertices"
    names: list[str] = PydanticField(
        ...,
        description="Vertex type names to remove from the schema.",
        min_length=1,
    )


class MergeVerticesOp(ConfigBaseModel):
    """Merge source vertices into a single logical name (schema, edges, ingestion)."""

    op: Literal["merge_vertices"] = "merge_vertices"
    sources: list[str] = PydanticField(
        ...,
        description=(
            "Vertex type names to merge away. Must not include ``into``. "
            "Each name must exist in the schema before the merge."
        ),
        min_length=1,
    )
    into: str = PydanticField(
        ...,
        description=(
            "Resulting vertex type name. If it already exists, source vertices are "
            "merged into it. If it does not exist, a new vertex is built from all sources."
        ),
    )


class RenameVertexPropertiesOp(ConfigBaseModel):
    """Rename vertex properties (and identity references) and propagate to ingestion.

    ``renames`` maps each vertex name to a per-vertex ``{old_field: new_field}`` map.
    Schema-side: rewrites ``Field.name``, ``vertex.identity``, and any DB profile
    structures that reference field names (``vertex_indexes``, ``edge_specs.indexes``).
    Ingestion-side: rewrites ``VertexActor.from`` so the doc still uses the OLD field
    name (injecting ``{new_field: old_field}`` when missing), rewrites
    ``TransformActor.rename`` values that target a renamed vertex field, and updates
    ``Resource.extra_weights`` / ``edge.vertex_weights`` (:class:`~graflo.architecture.graph_types.Weight`
    ``fields``, ``map``, and ``filter`` keys that address vertex observation columns).
    """

    op: Literal["rename_vertex_properties"] = "rename_vertex_properties"
    renames: dict[str, dict[str, str]] = PydanticField(
        ...,
        description=(
            "Per-vertex field rename map: ``{vertex_name: {old_field: new_field}}``."
        ),
    )


class RemoveVertexPropertiesOp(ConfigBaseModel):
    """Remove vertex properties and propagate pruning to ingestion/db profile references."""

    op: Literal["remove_vertex_properties"] = "remove_vertex_properties"
    removals: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-vertex field removal map: ``{vertex_name: [field_name, ...]}``."
        ),
    )


class RenameEntitiesOp(ConfigBaseModel):
    """Rename logical vertex names, edge relations, and ingestion resource names."""

    op: Literal["rename_entities"] = "rename_entities"
    vertices: dict[str, str] | None = PydanticField(
        default=None,
        description="Vertex rename map: ``{old_vertex: new_vertex}``.",
    )
    edges: dict[str, str] | None = PydanticField(
        default=None,
        description="Edge relation rename map: ``{old_relation: new_relation}``.",
    )
    resources: dict[str, str] | None = PydanticField(
        default=None,
        description="Ingestion resource rename map: ``{old_resource: new_resource}``.",
    )


class SanitizeOp(ConfigBaseModel):
    """Apply DB-flavor-specific name/field sanitization to a manifest.

    Composes (in order):

    1. Storage-name sanitization on ``DatabaseProfile`` (vertex storage names + edge
       relation names) against the flavor's reserved-words set.
    2. Vertex field rename for fields whose names are reserved words.
    3. For TigerGraph, normalize identity fields across edges that share a relation
       (TigerGraph requires consistent source/target indexes per relation).
    """

    op: Literal["sanitize"] = "sanitize"
    db_flavor: DBType = PydanticField(
        ...,
        description="Target database flavor whose reserved words/constraints drive the sanitization.",
    )
    reserved_words: list[str] | None = PydanticField(
        default=None,
        description=(
            "Optional override for the flavor's reserved words. "
            "When unset, ``graflo.db.util.load_reserved_words(db_flavor)`` is used."
        ),
    )


ManifestOp = Annotated[
    RemoveVerticesOp
    | MergeVerticesOp
    | RenameVertexPropertiesOp
    | RemoveVertexPropertiesOp
    | RenameEntitiesOp
    | SanitizeOp,
    PydanticField(discriminator="op"),
]
