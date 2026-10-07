"""Ops that add, remove, merge, or rename vertices and their properties."""

from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.vertex import (
    Field,
    Vertex,
)

from .validation import validate_merge_sources, validate_rename_map_is_injective


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
    allow_self_relations: bool = PydanticField(
        default=False,
        description=(
            "Accept edges whose endpoints both land on ``into``. A self-relation makes "
            "both endpoints share one accumulator slot, so assembly merges observations "
            "that were previously separate nodes. Rejected unless set."
        ),
    )
    allow_observation_fusion: bool = PydanticField(
        default=False,
        validation_alias=AliasChoices("allow_observation_fusion", "allow_row_fusion"),
        description=(
            "Accept resource pipelines whose sources land in one accumulator slot — "
            "the same level and the same ``role``, or both bare. Those steps then "
            "write to one slot, fusing into one node what a single source document "
            "emitted as two vertex observations. Same-level steps with distinct "
            "``role``s never share a slot and need no acknowledgement. Rejected "
            "unless set. ``allow_row_fusion`` is accepted as a legacy alias."
        ),
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> MergeVerticesOp:
        validate_merge_sources(self.sources, self.into, kind="merge_vertices")
        return self


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
        min_length=1,
    )


class RemoveVertexPropertiesOp(ConfigBaseModel):
    """Remove vertex properties and propagate pruning to ingestion/db profile references."""

    op: Literal["remove_vertex_properties"] = "remove_vertex_properties"
    removals: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-vertex field removal map: ``{vertex_name: [field_name, ...]}``."
        ),
        min_length=1,
    )


class AddVertexPropertiesOp(ConfigBaseModel):
    """Add vertex properties to existing logical vertex types.

    An entry may be a bare name or a full :class:`~graflo.architecture.schema.vertex.Field`.
    Bare names were the original shape and still mean what they meant -- an
    untyped property -- but they cannot express a property that arrives with a
    type and a grounding, which is what any op stream that adds a *measured* or
    *temporal* property has to say in one replayable step.
    """

    op: Literal["add_vertex_properties"] = "add_vertex_properties"
    additions: dict[str, list[str | Field]] = PydanticField(
        ...,
        description=(
            "Per-vertex property additions: ``{vertex_name: [field_name | Field, ...]}``."
        ),
        min_length=1,
    )

    def field_names(self, vertex: str) -> list[str]:
        """The names added to *vertex*, whichever shape they were written in."""
        return [
            entry if isinstance(entry, str) else entry.name
            for entry in self.additions.get(vertex, [])
        ]


class RenameVerticesOp(ConfigBaseModel):
    """Rename logical vertex names across schema, ingestion, and bindings."""

    op: Literal["rename_vertices"] = "rename_vertices"
    renames: dict[str, str] = PydanticField(
        ...,
        validation_alias=AliasChoices("renames", "vertices"),
        description=(
            "Vertex rename map: ``{old_vertex: new_vertex}``. Must be injective. "
            "``vertices`` is accepted as a legacy alias."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameVerticesOp:
        validate_rename_map_is_injective(
            self.renames,
            kind="rename_vertices",
            merge_hint="MergeVerticesOp(sources=[...], into=...)",
        )
        return self


class AddVerticesOp(ConfigBaseModel):
    """Introduce new logical vertex types.

    The unary counterpart to what :class:`MergeManifestsOp` can only do binarily.
    A replayable change set that cannot introduce a type could only ever describe a
    shrinking graph, which is why this exists alongside ``remove_vertices``.
    """

    op: Literal["add_vertices"] = "add_vertices"
    vertices: list[Vertex] = PydanticField(
        ...,
        description="Full vertex definitions, in the shape the schema block accepts.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_names(self) -> AddVerticesOp:
        names = [vertex.name for vertex in self.vertices]
        if len(names) != len(set(names)):
            raise ValueError("add_vertices entries must be unique by name")
        return self
