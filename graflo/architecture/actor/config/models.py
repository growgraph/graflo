"""Pydantic models for actor configuration."""

from __future__ import annotations

from typing import Annotated, Any, Literal, cast

from pydantic import Field as PydanticField, TypeAdapter, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.edge import EdgeBase
from graflo.architecture.transform import DressConfig, KeyRuleConfig

from .normalize import normalize_actor_step


class VertexActorConfig(ConfigBaseModel):
    """Configuration for a VertexActor."""

    type: Literal["vertex"] = PydanticField(
        default="vertex", description="Actor type discriminator"
    )
    vertex: str = PydanticField(..., description="Name of the vertex type to create")
    from_doc: dict[str, str] | None = PydanticField(
        default=None,
        alias="from",
        description="Projection: {vertex_field: doc_field}.",
    )
    keep_fields: list[str] | None = PydanticField(
        default=None, description="Optional list of fields to keep"
    )

    @model_validator(mode="before")
    @classmethod
    def set_type(cls, data: Any) -> Any:
        if isinstance(data, dict) and "vertex" in data and "type" not in data:
            data = dict(data)
            data["type"] = "vertex"
        return data


class TransformActorConfig(ConfigBaseModel):
    """Configuration for a TransformActor."""

    type: Literal["transform"] = PydanticField(
        default="transform", description="Actor type discriminator"
    )
    map: dict[str, str] | None = PydanticField(
        default=None, description="Field mapping: output_key -> input_key"
    )
    name: str | None = PydanticField(
        default=None, description="Named transform function"
    )
    params: dict[str, Any] = PydanticField(
        default_factory=dict, description="Transform function parameters"
    )
    module: str | None = PydanticField(
        default=None, description="Module containing transform function"
    )
    foo: str | None = PydanticField(
        default=None, description="Transform function name in module"
    )
    input: list[str] | None = PydanticField(
        default=None, description="Input field names for functional transform"
    )
    output: list[str] | None = PydanticField(
        default=None, description="Output field names for functional transform"
    )
    dress: DressConfig | None = PydanticField(
        default=None,
        description="Dressing spec for pivoted output.",
    )
    rule: KeyRuleConfig | None = PydanticField(
        default=None,
        description="Generic key renaming rule for transform.",
    )

    @model_validator(mode="before")
    @classmethod
    def set_type_and_flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = normalize_actor_step(cast(dict[str, Any], data))
        return normalized if normalized.get("type") == "transform" else data


class EdgeActorConfig(EdgeBase):
    """Configuration for an EdgeActor."""

    type: Literal["edge"] = PydanticField(
        default="edge", description="Actor type discriminator"
    )
    source: str = PydanticField(
        ..., alias="from", description="Source vertex type name"
    )
    target: str = PydanticField(..., alias="to", description="Target vertex type name")
    weights: dict[str, list[str]] | None = PydanticField(
        default=None, description="Weight configuration"
    )

    @model_validator(mode="before")
    @classmethod
    def set_type_and_flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = normalize_actor_step(cast(dict[str, Any], data))
        return normalized if normalized.get("type") == "edge" else data


class DescendActorConfig(ConfigBaseModel):
    """Configuration for a DescendActor."""

    type: Literal["descend"] = PydanticField(
        default="descend", description="Actor type discriminator"
    )
    key: str | None = PydanticField(default=None, description="Key to descend into")
    any_key: bool = PydanticField(default=False, description="Process all keys")
    pipeline: list["ActorConfig"] = PydanticField(
        default_factory=list,
        alias="apply",
        description="Pipeline of actors to apply to nested data",
    )

    @model_validator(mode="before")
    @classmethod
    def set_type_and_normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = normalize_actor_step(cast(dict[str, Any], data))
        return normalized if normalized.get("type") == "descend" else data

    @model_validator(mode="after")
    def validate_explicit_vertex_requirements(self) -> DescendActorConfig:
        return self


class VertexRouterActorConfig(ConfigBaseModel):
    """Configuration for a VertexRouterActor."""

    type: Literal["vertex_router"] = PydanticField(
        default="vertex_router", description="Actor type discriminator"
    )
    type_field: str = PydanticField(
        ...,
        description="Document field whose value determines the target vertex type name.",
    )
    prefix: str | None = PydanticField(
        default=None,
        description="Optional prefix to strip from document field keys.",
    )
    field_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Optional explicit rename map.",
    )
    type_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Map raw document values to vertex type names.",
    )
    vertex_from_map: dict[str, dict[str, str]] | None = PydanticField(
        default=None,
        description="Per-vertex-type field projection.",
    )

    @model_validator(mode="before")
    @classmethod
    def set_type(cls, data: Any) -> Any:
        if isinstance(data, dict) and "type_field" in data and "type" not in data:
            data = dict(data)
            data["type"] = "vertex_router"
        return data


class EdgeRouterActorConfig(ConfigBaseModel):
    """Configuration for an EdgeRouterActor."""

    type: Literal["edge_router"] = PydanticField(
        default="edge_router", description="Actor type discriminator"
    )
    source_type_field: str = PydanticField(
        ...,
        description="Document field whose value determines the source vertex type.",
    )
    target_type_field: str = PydanticField(
        ...,
        description="Document field whose value determines the target vertex type.",
    )
    source_fields: dict[str, str] | None = PydanticField(
        default=None,
        description="Projection for source vertex identity.",
    )
    target_fields: dict[str, str] | None = PydanticField(
        default=None,
        description="Projection for target vertex identity.",
    )
    relation_field: str | None = PydanticField(
        default=None,
        description="Document field whose value determines the relation type per row.",
    )
    relation: str | None = PydanticField(
        default=None,
        description="Static relation type when relation_field is not used.",
    )
    type_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Shared map: raw type value -> vertex type name.",
    )
    source_type_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Override type_map for source side only.",
    )
    target_type_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Override type_map for target side only.",
    )
    relation_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Map raw relation values to canonical names.",
    )

    @model_validator(mode="before")
    @classmethod
    def set_type(cls, data: Any) -> Any:
        if (
            isinstance(data, dict)
            and "source_type_field" in data
            and "type" not in data
        ):
            data = dict(data)
            data["type"] = "edge_router"
        return data


ActorConfig = Annotated[
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
    | EdgeRouterActorConfig,
    PydanticField(discriminator="type"),
]

DescendActorConfig.model_rebuild()

_actor_config_adapter: TypeAdapter[
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
    | EdgeRouterActorConfig
] = TypeAdapter(
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
    | EdgeRouterActorConfig
)
