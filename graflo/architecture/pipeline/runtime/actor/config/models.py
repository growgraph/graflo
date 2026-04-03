"""Pydantic models for actor configuration."""

from __future__ import annotations

from typing import Annotated, Any, Literal, cast

from pydantic import Field as PydanticField, TypeAdapter, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.declarations.transform import DressConfig
from graflo.architecture.edge_derivation import EdgeDerivation

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
            keys = call.get("keys")
            if isinstance(keys, str):
                call["keys"] = {"mode": "include", "names": [keys]}
            elif isinstance(keys, tuple):
                call["keys"] = {"mode": "include", "names": list(keys)}
            elif isinstance(keys, list):
                call["keys"] = {"mode": "include", "names": keys}
            elif isinstance(keys, dict):
                keys = dict(keys)
                names = keys.get("names")
                if isinstance(names, str):
                    keys["names"] = [names]
                elif isinstance(names, tuple):
                    keys["names"] = list(names)
                call["keys"] = keys
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

    class KeySelectionConfig(ConfigBaseModel):
        """Selection of document keys for key-target transforms."""

        mode: Literal["all", "include", "exclude"] = PydanticField(
            default="all",
            description=(
                "How keys are selected when target='keys': "
                "all=all keys, include=only provided keys, "
                "exclude=all except provided keys."
            ),
        )
        names: list[str] = PydanticField(
            default_factory=list,
            description="Key names used by include/exclude selection modes.",
        )

        @model_validator(mode="after")
        def validate_mode_names(self) -> "TransformCallConfig.KeySelectionConfig":
            if self.mode == "all" and self.names:
                raise ValueError(
                    "call.keys.names must be empty when call.keys.mode='all'."
                )
            if self.mode in {"include", "exclude"} and not self.names:
                raise ValueError(
                    "call.keys.names must be provided when call.keys.mode is include/exclude."
                )
            return self

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
    input_groups: list[list[str]] | None = PydanticField(
        default=None,
        description=(
            "Explicit groups of input fields for repeated tuple-style function calls."
        ),
    )
    output_groups: list[list[str]] | None = PydanticField(
        default=None,
        description="Optional output field groups aligned with input_groups.",
    )
    target: Literal["values", "keys"] | None = PydanticField(
        default=None,
        description=(
            "Transform target. Omit with call.use to inherit from ingestion_model.transforms "
            "entry. values=transform input values, keys=transform selected document keys. "
            "Inline calls (no use) default to values when omitted."
        ),
    )
    keys: KeySelectionConfig | None = PydanticField(
        default=None,
        description="Optional key selection for target='keys'.",
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
        for key in ("input_groups", "output_groups"):
            value = data.get(key)
            if value is None:
                continue
            if isinstance(value, tuple):
                value = list(value)
            if isinstance(value, list):
                normalized_groups: list[Any] = []
                for group in value:
                    if isinstance(group, str):
                        normalized_groups.append([group])
                    elif isinstance(group, tuple):
                        normalized_groups.append(list(group))
                    else:
                        normalized_groups.append(group)
                data[key] = normalized_groups
        keys = data.get("keys")
        if isinstance(keys, str):
            data["keys"] = {"mode": "include", "names": [keys]}
        elif isinstance(keys, tuple):
            data["keys"] = {"mode": "include", "names": list(keys)}
        elif isinstance(keys, list):
            data["keys"] = {"mode": "include", "names": keys}
        elif isinstance(keys, dict):
            keys = dict(keys)
            names = keys.get("names")
            if isinstance(names, str):
                keys["names"] = [names]
            elif isinstance(names, tuple):
                keys["names"] = list(names)
            data["keys"] = keys
        return data

    @model_validator(mode="after")
    def validate_target(self) -> "TransformCallConfig":
        if self.use is not None and (self.module is not None or self.foo is not None):
            raise ValueError("call.use cannot be combined with call.module/call.foo.")
        if self.use is None and (self.module is None or self.foo is None):
            raise ValueError(
                "call must provide either use, or both module and foo for inline function."
            )
        if self.use is None:
            effective_target: Literal["values", "keys"] | None = (
                self.target if self.target is not None else "values"
            )
        else:
            effective_target = self.target

        if effective_target == "keys":
            if self.strategy is not None:
                raise ValueError(
                    "call.strategy is not allowed when call.target='keys'; key mode uses implicit per-key execution."
                )
            if self.input:
                raise ValueError(
                    "call.input is not allowed when call.target='keys'; use call.keys selection instead."
                )
            if self.output:
                raise ValueError("call.output is not allowed when call.target='keys'.")
            if self.input_groups:
                raise ValueError(
                    "call.input_groups is not allowed when call.target='keys'."
                )
            if self.output_groups:
                raise ValueError(
                    "call.output_groups is not allowed when call.target='keys'."
                )
            if self.dress is not None:
                raise ValueError("call.dress is not supported when call.target='keys'.")
        elif effective_target == "values" and self.keys is not None:
            raise ValueError("call.keys can only be used when call.target='keys'.")
        if self.input is not None and self.input_groups is not None:
            raise ValueError(
                "Provide either call.input or call.input_groups, not both."
            )
        if self.output_groups is not None and self.input_groups is None:
            raise ValueError("call.output_groups requires call.input_groups.")
        if self.input_groups is not None:
            if self.strategy not in {None, "single"}:
                raise ValueError(
                    "call.input_groups does not support call.strategy. "
                    "Grouped mode is an explicit repeated execution."
                )
            if self.dress is not None:
                raise ValueError("call.input_groups is not compatible with call.dress.")
            if self.output is not None and self.output_groups is not None:
                raise ValueError(
                    "Provide either call.output or call.output_groups for grouped calls, not both."
                )
            if self.output_groups is not None and len(self.output_groups) != len(
                self.input_groups
            ):
                raise ValueError(
                    "call.output_groups must have the same number of groups as call.input_groups."
                )
            if self.output is not None and len(self.output) != len(self.input_groups):
                raise ValueError(
                    "For grouped scalar outputs, call.output length must equal number of call.input_groups."
                )
        if self.strategy == "all" and self.input:
            raise ValueError("call.strategy='all' does not accept call.input.")
        if self.strategy == "all" and self.input_groups:
            raise ValueError("call.strategy='all' does not accept call.input_groups.")
        return self


class EdgeActorConfig(ConfigBaseModel):
    """Configuration for an EdgeActor (logical edge + ingestion derivation; flat YAML)."""

    type: Literal["edge"] = PydanticField(
        default="edge", description="Actor type discriminator"
    )
    source: str = PydanticField(
        ..., alias="from", description="Source vertex type name"
    )
    target: str = PydanticField(..., alias="to", description="Target vertex type name")
    relation: str | None = PydanticField(
        default=None,
        description="Optional fixed logical relation / edge type name.",
    )
    relation_from_key: bool = PydanticField(
        default=False,
        description="Ingestion: derive per-row relation label from the location key during assembly.",
    )
    description: str | None = PydanticField(
        default=None,
        description="Optional semantic description (merged into schema Edge).",
    )
    relation_field: str | None = PydanticField(
        default=None,
        description="Ingestion: document field name for per-row relationship type.",
    )
    match_source: str | None = PydanticField(
        default=None,
        description="Ingestion: require this path segment in source locations.",
    )
    match_target: str | None = PydanticField(
        default=None,
        description="Ingestion: require this path segment in target locations.",
    )
    exclude_source: str | None = PydanticField(
        default=None,
        description="Ingestion: exclude source locations containing this segment.",
    )
    exclude_target: str | None = PydanticField(
        default=None,
        description="Ingestion: exclude target locations containing this segment.",
    )
    match: str | None = PydanticField(
        default=None,
        description="Ingestion: require this segment on both source and target locations.",
    )
    properties: list[Any] = PydanticField(
        default_factory=list,
        description="Edge properties merged into schema Edge (same forms as Edge.properties).",
    )
    vertex_weights: list[Any] = PydanticField(
        default_factory=list,
        description="Vertex-derived weight rules registered in EdgeDerivationRegistry.",
    )

    @property
    def derivation(self) -> EdgeDerivation:
        """Normalized ingestion-only fields for assembly/render."""
        return EdgeDerivation(
            match_source=self.match_source,
            match_target=self.match_target,
            exclude_source=self.exclude_source,
            exclude_target=self.exclude_target,
            match=self.match,
            relation_field=self.relation_field,
            relation_from_key=self.relation_from_key,
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
    source_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Document field whose value determines the source vertex type. "
            "Provide this or source."
        ),
    )
    target_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Document field whose value determines the target vertex type. "
            "Provide this or target."
        ),
    )
    source: str | None = PydanticField(
        default=None,
        description=(
            "Static source vertex type name. Provide this or source_type_field."
        ),
    )
    target: str | None = PydanticField(
        default=None,
        description=(
            "Static target vertex type name. Provide this or target_type_field."
        ),
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
            and (
                "source_type_field" in data
                or "target_type_field" in data
                or "source" in data
                or "target" in data
            )
            and "type" not in data
        ):
            data = dict(data)
            data["type"] = "edge_router"
        return data

    @model_validator(mode="after")
    def validate_side_type_sources(self) -> "EdgeRouterActorConfig":
        if self.source is None and self.source_type_field is None:
            raise ValueError(
                "edge_router requires source or source_type_field to be provided."
            )
        if self.target is None and self.target_type_field is None:
            raise ValueError(
                "edge_router requires target or target_type_field to be provided."
            )
        if self.source_type_field is None and self.target_type_field is None:
            raise ValueError(
                "edge_router requires at least one of "
                "source_type_field or target_type_field."
            )
        return self


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
