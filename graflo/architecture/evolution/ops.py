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


class AddVertexPropertiesOp(ConfigBaseModel):
    """Add vertex properties to existing logical vertex types."""

    op: Literal["add_vertex_properties"] = "add_vertex_properties"
    additions: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-vertex property additions: ``{vertex_name: [field_name, ...]}``."
        ),
    )


class RenameVerticesOp(ConfigBaseModel):
    """Rename logical vertex names across schema, ingestion, and bindings."""

    op: Literal["rename_vertices"] = "rename_vertices"
    vertices: dict[str, str] = PydanticField(
        ...,
        description="Vertex rename map: ``{old_vertex: new_vertex}``.",
    )


class RenameRelationsOp(ConfigBaseModel):
    """Rename logical edge relation names across schema and ingestion."""

    op: Literal["rename_relations"] = "rename_relations"
    relations: dict[str, str] = PydanticField(
        ...,
        description="Relation rename map: ``{old_relation: new_relation}``.",
    )


class RenameResourcesOp(ConfigBaseModel):
    """Rename ingestion resource names and bindings references."""

    op: Literal["rename_resources"] = "rename_resources"
    resources: dict[str, str] = PydanticField(
        ...,
        description="Ingestion resource rename map: ``{old_resource: new_resource}``.",
    )


class RemoveEdgesOp(ConfigBaseModel):
    """Remove logical edge relations from schema, profile, and ingestion selectors."""

    op: Literal["remove_edges"] = "remove_edges"
    relations: list[str] = PydanticField(
        ...,
        description="Relation names to remove from edge definitions and references.",
        min_length=1,
    )


class MergeEdgesOp(ConfigBaseModel):
    """Merge source relation names into a canonical relation name."""

    op: Literal["merge_edges"] = "merge_edges"
    sources: list[str] = PydanticField(
        ...,
        description="Relation names to merge away. Must not include ``into``.",
        min_length=1,
    )
    into: str = PydanticField(
        ...,
        description="Canonical relation name that receives all source relations.",
    )


class RenameEdgePropertiesOp(ConfigBaseModel):
    """Rename edge properties for each relation across schema/profile/ingestion."""

    op: Literal["rename_edge_properties"] = "rename_edge_properties"
    renames: dict[str, dict[str, str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field rename map: "
            "``{relation_name: {old_field: new_field}}``."
        ),
    )


class RemoveEdgePropertiesOp(ConfigBaseModel):
    """Remove edge properties for each relation across schema/profile/ingestion."""

    op: Literal["remove_edge_properties"] = "remove_edge_properties"
    removals: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field removals: ``{relation_name: [field_name, ...]}``."
        ),
    )


class AddEdgePropertiesOp(ConfigBaseModel):
    """Add edge properties for each relation in schema/profile defaults."""

    op: Literal["add_edge_properties"] = "add_edge_properties"
    additions: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field additions: ``{relation_name: [field_name, ...]}``."
        ),
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
    | AddVertexPropertiesOp
    | RenameVerticesOp
    | RenameRelationsOp
    | RenameResourcesOp
    | RemoveEdgesOp
    | MergeEdgesOp
    | RenameEdgePropertiesOp
    | RemoveEdgePropertiesOp
    | AddEdgePropertiesOp
    | SanitizeOp,
    PydanticField(discriminator="op"),
]
