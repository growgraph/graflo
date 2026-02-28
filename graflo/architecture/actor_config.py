"""Pydantic models for actor configuration.

These models define the structure of YAML configuration files for the
actor-based graph transformation system. They provide validation,
type safety, and explicit format support (pipeline, transform/map/to_vertex,
create_edge/edge with from/to).

These replace the implicit type inference in ActorWrapper.__init__()
with explicit Pydantic discriminated unions.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, Literal, cast

from pydantic import Field, TypeAdapter, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.edge import EdgeBase
from graflo.architecture.transform import DressConfig

logger = logging.getLogger(__name__)


def _steps_list(val: Any) -> list[Any]:
    """Ensure value is a list of steps (single dict becomes [dict])."""
    return val if isinstance(val, list) else [val]


def normalize_actor_step(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw step dict so it has 'type' and flat structure for validation.

    Supports explicit format:
    - {"vertex": "user"} -> {"type": "vertex", "vertex": "user"}
    - {"transform": {"map": {...}, "to_vertex": "x"}} -> {"type": "transform", "map": {...}, "to_vertex": "x"}
    - {"edge": {"from": "a", "to": "b"}} or {"create_edge": {...}} -> {"type": "edge", "from": "a", "to": "b"}
    - {"descend": {"into": "k", "pipeline": [...]}} or {"apply": [...]} / {"pipeline": [...]}
    """
    if not isinstance(data, dict):
        return data
    data = dict(data)
    if "type" in data:
        return data

    if "vertex" in data:
        data["type"] = "vertex"
        return data

    if "transform" in data:
        inner = data.pop("transform")
        if isinstance(inner, dict):
            data.update(inner)
        # Legacy: convert switch -> input + dress
        if "switch" in data:
            switch = data.pop("switch")
            if isinstance(switch, dict) and switch:
                key = next(iter(switch))
                vals = switch[key]
                if isinstance(vals, (list, tuple)) and len(vals) >= 2:
                    data.setdefault("input", [key])
                    data.setdefault("dress", {"key": vals[0], "value": vals[1]})
        # Legacy: convert list-style dress -> dict-style
        if "dress" in data and isinstance(data["dress"], (list, tuple)):
            vals = data["dress"]
            if len(vals) >= 2:
                data["dress"] = {"key": vals[0], "value": vals[1]}
        data["type"] = "transform"
        return data

    if "edge" in data:
        inner = data.pop("edge")
        if isinstance(inner, dict):
            data.update(inner)
        data["type"] = "edge"
        return data
    if ("source" in data or "from" in data) and ("target" in data or "to" in data):
        data = dict(data)
        data["type"] = "edge"
        return data
    if "create_edge" in data:
        inner = data.pop("create_edge")
        if isinstance(inner, dict):
            data.update(inner)
        data["type"] = "edge"
        return data

    if "descend" in data:
        inner = data.pop("descend")
        if isinstance(inner, dict):
            if "pipeline" in inner:
                inner["pipeline"] = [
                    normalize_actor_step(s) for s in _steps_list(inner["pipeline"])
                ]
            elif "apply" in inner:
                inner["pipeline"] = [
                    normalize_actor_step(s) for s in _steps_list(inner["apply"])
                ]
                del inner["apply"]
            data.update(inner)
        data["type"] = "descend"
        if "pipeline" not in data and "apply" in data:
            data["pipeline"] = [
                normalize_actor_step(s) for s in _steps_list(data["apply"])
            ]
            del data["apply"]
        return data

    if "vertex_router" in data:
        inner = data.pop("vertex_router")
        if isinstance(inner, dict):
            data.update(inner)
        data["type"] = "vertex_router"
        return data

    if "apply" in data:
        data["type"] = "descend"
        data["pipeline"] = [normalize_actor_step(s) for s in _steps_list(data["apply"])]
        del data["apply"]
        return data
    if "pipeline" in data:
        data["type"] = "descend"
        data["pipeline"] = [
            normalize_actor_step(s) for s in _steps_list(data["pipeline"])
        ]
        return data

    # Minimal transform step
    if "type" not in data and (
        "name" in data or "map" in data or "switch" in data or "dress" in data
    ):
        data = dict(data)
        if "switch" in data:
            switch = data.pop("switch")
            if isinstance(switch, dict) and switch:
                key = next(iter(switch))
                vals = switch[key]
                if isinstance(vals, (list, tuple)) and len(vals) >= 2:
                    data.setdefault("input", [key])
                    data.setdefault("dress", {"key": vals[0], "value": vals[1]})
        if "dress" in data and isinstance(data["dress"], (list, tuple)):
            vals = data["dress"]
            if len(vals) >= 2:
                data["dress"] = {"key": vals[0], "value": vals[1]}
        data["type"] = "transform"
        return data

    return data


class VertexActorConfig(ConfigBaseModel):
    """Configuration for a VertexActor."""

    type: Literal["vertex"] = Field(
        default="vertex", description="Actor type discriminator"
    )
    vertex: str = Field(..., description="Name of the vertex type to create")
    keep_fields: list[str] | None = Field(
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
    """Configuration for a TransformActor.

    Explicit format: transform with map and to_vertex (target vertex for output).
    """

    type: Literal["transform"] = Field(
        default="transform", description="Actor type discriminator"
    )
    map: dict[str, str] | None = Field(
        default=None, description="Field mapping: output_key -> input_key"
    )
    to_vertex: str | None = Field(
        default=None,
        alias="target_vertex",
        description="Target vertex to send transformed output to",
    )
    name: str | None = Field(default=None, description="Named transform function")
    params: dict[str, Any] = Field(
        default_factory=dict, description="Transform function parameters"
    )
    module: str | None = Field(
        default=None, description="Module containing transform function"
    )
    foo: str | None = Field(
        default=None, description="Transform function name in module"
    )
    input: list[str] | None = Field(
        default=None, description="Input field names for functional transform"
    )
    output: list[str] | None = Field(
        default=None, description="Output field names for functional transform"
    )
    dress: DressConfig | None = Field(
        default=None,
        description=(
            "Dressing spec for pivoted output: {key: <key_field>, value: <value_field>}. "
            "key_field receives the input field name, value_field receives the "
            "function result."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def set_type_and_flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = normalize_actor_step(cast(dict[str, Any], data))
        return normalized if normalized.get("type") == "transform" else data


class EdgeActorConfig(EdgeBase):
    """Configuration for an EdgeActor. Extends EdgeBase; supports 'from'/'to' and 'source'/'target'."""

    type: Literal["edge"] = Field(
        default="edge", description="Actor type discriminator"
    )
    source: str = Field(..., alias="from", description="Source vertex type name")
    target: str = Field(..., alias="to", description="Target vertex type name")
    weights: dict[str, list[str]] | None = Field(
        default=None, description="Weight configuration"
    )
    indexes: list[dict[str, Any]] | None = Field(
        default=None, description="Index configuration"
    )

    @model_validator(mode="before")
    @classmethod
    def set_type_and_flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = normalize_actor_step(cast(dict[str, Any], data))
        return normalized if normalized.get("type") == "edge" else data


class DescendActorConfig(ConfigBaseModel):
    """Configuration for a DescendActor. Uses 'pipeline' (alias 'apply') and optional 'into' (alias 'key')."""

    type: Literal["descend"] = Field(
        default="descend", description="Actor type discriminator"
    )
    into: str | None = Field(
        default=None, alias="key", description="Key to descend into"
    )
    any_key: bool = Field(default=False, description="Process all keys")
    pipeline: list["ActorConfig"] = Field(
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


class VertexRouterActorConfig(ConfigBaseModel):
    """Configuration for a VertexRouterActor.

    Routes documents to the correct VertexActor based on a type field value.
    Optionally strips a prefix from field keys or applies an explicit field map.
    """

    type: Literal["vertex_router"] = Field(
        default="vertex_router", description="Actor type discriminator"
    )
    type_field: str = Field(
        ...,
        description="Document field whose value determines the target vertex type name.",
    )
    prefix: str | None = Field(
        default=None,
        description="Optional prefix to strip from document field keys when building the vertex sub-doc.",
    )
    field_map: dict[str, str] | None = Field(
        default=None,
        description="Optional explicit rename map (original_key -> vertex_field_key). "
        "Mutually exclusive with prefix.",
    )

    @model_validator(mode="before")
    @classmethod
    def set_type(cls, data: Any) -> Any:
        if isinstance(data, dict) and "type_field" in data and "type" not in data:
            data = dict(data)
            data["type"] = "vertex_router"
        return data


# Discriminated union for parsing a single pipeline step (used in ActorWrapper and Resource)
ActorConfig = Annotated[
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig,
    Field(discriminator="type"),
]

DescendActorConfig.model_rebuild()

# TypeAdapter for validating a single pipeline step (union type has no model_validate)
_actor_config_adapter: TypeAdapter[
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
] = TypeAdapter(
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
)


# Keys to strip from step dicts (runtime or resource-level, not part of ActorConfig)
_STEP_STRIP_KEYS = frozenset(
    {
        "vertex_config",
        "edge_config",
        "edge_greedy",
        "transforms",
        "resource_name",
    }
)


def validate_actor_step(
    data: dict[str, Any],
) -> (
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
):
    """Validate a normalized step dict as ActorConfig (discriminated union)."""
    return _actor_config_adapter.validate_python(data)


def parse_root_config(
    *args: Any,
    **kwargs: Any,
) -> (
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
):
    """Parse root input into a single ActorConfig (single step or descend pipeline).

    Accepts the same shapes as ActorWrapper:
    - Single step dict: e.g. {"vertex": "user"} or **kwargs
    - Pipeline: list of steps, or kwargs with "apply"/"pipeline"

    Returns:
        Validated ActorConfig. For pipeline input, returns a DescendActorConfig
        with into=None and pipeline=[...].
    """
    pipeline: list[dict[str, Any]] | None = None
    single: dict[str, Any] | None = None

    if kwargs and ("apply" in kwargs or "pipeline" in kwargs):
        raw = kwargs.get("pipeline") or kwargs.get("apply")
        if raw is not None:
            pipeline = cast(
                list[dict[str, Any]],
                list(raw) if isinstance(raw, list) else [raw],
            )
    elif args:
        if len(args) == 1 and isinstance(args[0], list):
            pipeline = list(args[0])
        elif len(args) == 1 and isinstance(args[0], dict):
            single = dict(args[0])
        elif args and all(isinstance(a, dict) for a in args):
            pipeline = [dict(a) for a in args]

    if pipeline is not None:
        configs = [
            _actor_config_adapter.validate_python(normalize_actor_step(s))
            for s in pipeline
        ]
        return DescendActorConfig.model_validate(
            {
                "type": "descend",
                "into": None,
                "any_key": False,
                "pipeline": configs,
            }
        )
    if single is not None:
        step_dict = {k: v for k, v in single.items() if k not in _STEP_STRIP_KEYS}
        return _actor_config_adapter.validate_python(normalize_actor_step(step_dict))
    step_kwargs = {k: v for k, v in kwargs.items() if k not in _STEP_STRIP_KEYS}
    return _actor_config_adapter.validate_python(normalize_actor_step(step_kwargs))
