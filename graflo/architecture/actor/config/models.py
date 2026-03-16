"""Pydantic models for actor configuration."""

from __future__ import annotations

from typing import Annotated, Any, Literal, cast

from pydantic import Field as PydanticField, TypeAdapter, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.edge import EdgeBase
from graflo.architecture.transform import DressConfig

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
    rename: dict[str, str] | None = PydanticField(
        default=None,
        description="Rename mapping in explicit DSL form: transform.rename.",
    )
    call: TransformCallConfig | None = PydanticField(
        default=None,
        description="Function-call configuration in explicit DSL form: transform.call.",
    )

    @model_validator(mode="before")
    @classmethod
    def set_type_and_flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = normalize_actor_step(cast(dict[str, Any], data))
        if normalized.get("type") != "transform":
            return data
        normalized = dict(normalized)
        call = normalized.get("call")
        if isinstance(call, dict):
            call = dict(call)
            for key in ("input", "output"):
                value = call.get(key)
                if isinstance(value, str):
                    call[key] = [value]
                elif isinstance(value, tuple):
                    call[key] = list(value)
            normalized["call"] = call
        return normalized

    @model_validator(mode="after")
    def validate_mode(self) -> "TransformActorConfig":
        enabled = sum([self.rename is not None, self.call is not None])
        if enabled != 1:
            raise ValueError(
                "Transform step must define exactly one of rename or call."
            )
        return self


class TransformCallConfig(ConfigBaseModel):
    """Explicit function call transform DSL payload."""

    use: str | None = PydanticField(
        default=None,
        description=(
            "Named transform reference from ingestion_model.transforms. "
            "When provided, module/foo must be omitted."
        ),
    )
    module: str | None = PydanticField(
        default=None, description="Module containing transform function."
    )
    foo: str | None = PydanticField(
        default=None, description="Transform function name in module."
    )
    params: dict[str, Any] = PydanticField(
        default_factory=dict, description="Function call keyword arguments."
    )
    input: list[str] | None = PydanticField(
        default=None, description="Input field names for function execution."
    )
    output: list[str] | None = PydanticField(
        default=None, description="Optional output field names."
    )
    strategy: Literal["single", "each", "all"] | None = PydanticField(
        default=None, description="Execution strategy for function calls."
    )
    dress: DressConfig | None = PydanticField(
        default=None, description="Pivot dressing output for scalar call results."
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_io(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        for key in ("input", "output"):
            value = data.get(key)
            if isinstance(value, str):
                data[key] = [value]
            elif isinstance(value, tuple):
                data[key] = list(value)
        return data

    @model_validator(mode="after")
    def validate_target(self) -> "TransformCallConfig":
        if self.use is not None and (self.module is not None or self.foo is not None):
            raise ValueError("call.use cannot be combined with call.module/call.foo.")
        if self.use is None and (self.module is None or self.foo is None):
            raise ValueError(
                "call must provide either use, or both module and foo for inline function."
            )
        if self.strategy == "all" and self.input:
            raise ValueError("call.strategy='all' does not accept call.input.")
        return self


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
TransformActorConfig.model_rebuild()

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
