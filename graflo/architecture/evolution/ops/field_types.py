"""Ops that change the declared type of vertex and edge fields."""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.vertex import (
    FieldType,
)


class FieldTypeSpec(ConfigBaseModel):
    """Target logical type for one property."""

    type: FieldType | None = PydanticField(
        ...,
        description="New logical field type; ``None`` clears the declared type.",
    )
    item_type: FieldType | None = PydanticField(
        default=None,
        description="Element type, required when ``type`` is ``LIST``.",
    )

    @model_validator(mode="after")
    def _validate_item_type(self) -> FieldTypeSpec:
        if self.type == FieldType.LIST and self.item_type is None:
            raise ValueError("a LIST field type requires item_type")
        if self.type != FieldType.LIST and self.item_type is not None:
            raise ValueError("item_type is only meaningful for a LIST field type")
        return self


class ChangeFieldTypesOp(ConfigBaseModel):
    """Set the logical type of existing vertex or edge properties.

    Makes the differ's ``CHANGE_VERTEX_FIELD_TYPE`` / ``CHANGE_EDGE_FIELD_TYPE``
    authorable. Targets are validated against the profile's ``db_flavor`` so an
    unsupported type fails here rather than at define time.
    """

    op: Literal["change_field_types"] = "change_field_types"
    vertices: dict[str, dict[str, FieldTypeSpec]] = PydanticField(
        default_factory=dict,
        description="``{vertex_name: {field_name: {type, item_type}}}``.",
    )
    edges: dict[str, dict[str, FieldTypeSpec]] = PydanticField(
        default_factory=dict,
        description="``{relation_name: {field_name: {type, item_type}}}``.",
    )

    @model_validator(mode="after")
    def _require_a_target(self) -> ChangeFieldTypesOp:
        if not self.vertices and not self.edges:
            raise ValueError(
                "change_field_types requires at least one of vertices or edges"
            )
        return self


class MergeFieldTypes(ConfigBaseModel):
    """The type of a merged property, declared on the merge by its merged names.

    Keyed by the class or relation name and the property name the **merged**
    manifest carries. The merge retypes every member, on either side, that
    carries the property (under whatever spelling it is renamed from) before
    folding it, so members that disagree on a type merge into the declared one.
    Members that do not carry the property are left alone.
    """

    vertices: dict[str, dict[str, FieldTypeSpec]] = PydanticField(
        default_factory=dict,
        description="``{merged_vertex: {merged_property: {type, item_type}}}``.",
    )
    edges: dict[str, dict[str, FieldTypeSpec]] = PydanticField(
        default_factory=dict,
        description="``{merged_relation: {property: {type, item_type}}}``.",
    )

    @model_validator(mode="after")
    def _require_a_target(self) -> MergeFieldTypes:
        if not self.vertices and not self.edges:
            raise ValueError("field_types requires at least one of vertices or edges")
        return self
