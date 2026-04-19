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
    role: str | None = PydanticField(
        default=None,
        description=(
            "Named accumulator slot for this vertex. When set, the vertex is stored at "
            "lindex.extend((role, 0)) instead of bare lindex, making it addressable by "
            "a downstream edge step via source_role / target_role. Use when multiple "
            "vertices of the same type appear as distinct roles in one row (e.g. "
            "role='buyer' and role='seller' both vertex type 'company')."
        ),
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


class EdgeLinkConfig(ConfigBaseModel):
    """One intent in a multi-link edge step.

    Each item in an ``EdgeActorConfig.links`` list describes one source→target→relation
    binding to emit per row. Equivalent to a single-intent ``edge`` step without the
    ``links`` field itself.

    Slot resolution (``source_role`` / ``target_role``) works identically to
    ``source_type_field`` / ``target_type_field`` on a standalone ``edge`` step — the
    slot name is the accumulator segment populated by an upstream ``vertex`` step with a
    matching ``role``, or a ``vertex_router`` step (``role`` when set, else ``type_field``).
    """

    model_config = {"extra": "forbid", "populate_by_name": True}

    source: str | None = PydanticField(
        default=None,
        alias="from",
        description="Static source vertex type name. Exclusive with source_type_field / source_role.",
    )
    target: str | None = PydanticField(
        default=None,
        alias="to",
        description="Static target vertex type name. Exclusive with target_type_field / target_role.",
    )
    source_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Accumulator slot segment for the source vertex (VertexRouterActor role or "
            "type_field; or vertex step role). Exclusive with 'from' and source_role."
        ),
    )
    target_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Accumulator slot segment for the target vertex (VertexRouterActor role or "
            "type_field; or vertex step role). Exclusive with 'to' and target_role."
        ),
    )
    source_role: str | None = PydanticField(
        default=None,
        description=(
            "Sugar for source_type_field: same accumulator segment name. Exclusive with "
            "source_type_field."
        ),
    )
    target_role: str | None = PydanticField(
        default=None,
        description=(
            "Sugar for target_type_field: same accumulator segment name. Exclusive with "
            "target_type_field."
        ),
    )
    relation: str | None = PydanticField(
        default=None,
        description="Fixed relation / edge type name for this link.",
    )
    relation_field: str | None = PydanticField(
        default=None,
        description="Document field name for per-row relationship type.",
    )
    match_source: str | None = PydanticField(
        default=None,
        description="Require this path segment in source vertex locations.",
    )
    match_target: str | None = PydanticField(
        default=None,
        description="Require this path segment in target vertex locations.",
    )

    @model_validator(mode="after")
    def resolve_and_validate(self) -> "EdgeLinkConfig":
        # Resolve role aliases → type_field (they name the same accumulator slot).
        # Use object.__setattr__ to bypass validate_assignment re-triggering this validator.
        if self.source_role is not None:
            if self.source_type_field is not None:
                raise ValueError(
                    "source_role and source_type_field are mutually exclusive in an edge link."
                )
            object.__setattr__(self, "source_type_field", self.source_role)
        if self.target_role is not None:
            if self.target_type_field is not None:
                raise ValueError(
                    "target_role and target_type_field are mutually exclusive in an edge link."
                )
            object.__setattr__(self, "target_type_field", self.target_role)

        # Each side needs exactly one of: static type or slot reference.
        if self.source is None and self.source_type_field is None:
            raise ValueError(
                "edge link requires 'from' (source), source_type_field, or source_role."
            )
        if self.target is None and self.target_type_field is None:
            raise ValueError(
                "edge link requires 'to' (target), target_type_field, or target_role."
            )
        if self.source is not None and self.source_type_field is not None:
            raise ValueError(
                "'from' and source_type_field/source_role are mutually exclusive."
            )
        if self.target is not None and self.target_type_field is not None:
            raise ValueError(
                "'to' and target_type_field/target_role are mutually exclusive."
            )
        return self


class EdgeActorConfig(ConfigBaseModel):
    """Configuration for an EdgeActor (logical edge + ingestion derivation; flat YAML).

    **Single-intent mode** (default): declare source/target via ``from``/``to`` (static
    vertex type names) or ``source_type_field``/``target_type_field`` / ``source_role``/
    ``target_role`` (slot-based dynamic resolution).  One edge intent is emitted per row.

    **Multi-link mode** (``links`` list): declare a list of :class:`EdgeLinkConfig` items.
    Each item emits one edge intent per row, allowing a single pipeline step to produce
    multiple relationship types from one flat row.  Mutually exclusive with all top-level
    source/target fields.
    """

    type: Literal["edge"] = PydanticField(
        default="edge", description="Actor type discriminator"
    )
    source: str | None = PydanticField(
        default=None,
        alias="from",
        description="Source vertex type name (optional if source_type_field/source_role is set).",
    )
    target: str | None = PydanticField(
        default=None,
        alias="to",
        description="Target vertex type name (optional if target_type_field/target_role is set).",
    )
    source_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Accumulator slot segment for the source vertex (same name as the upstream "
            "VertexRouterActor's role, or type_field when role is unset). EdgeActor scans "
            "acc_vertex for data at lindex.extend((source_type_field, 0)) to resolve the "
            "source type dynamically. Exclusive with 'from' and source_role."
        ),
    )
    target_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Accumulator slot segment for the target vertex (VertexRouterActor role or "
            "type_field). Exclusive with 'to' and target_role."
        ),
    )
    source_role: str | None = PydanticField(
        default=None,
        description=(
            "Role slot name for the source vertex — sugar for source_type_field when the "
            "slot was populated by an upstream 'vertex' step with a matching role. "
            "Exclusive with source_type_field."
        ),
    )
    target_role: str | None = PydanticField(
        default=None,
        description=(
            "Role slot name for the target vertex — sugar for target_type_field when the "
            "slot was populated by an upstream 'vertex' step with a matching role. "
            "Exclusive with target_type_field."
        ),
    )
    links: list[EdgeLinkConfig] | None = PydanticField(
        default=None,
        description=(
            "Multi-intent list. When set, each item emits one edge intent per row. "
            "Mutually exclusive with all top-level source/target/role fields. "
            "Use when a single flat row encodes multiple relationships."
        ),
    )
    relation_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Map raw relation values to canonical relation names.",
    )
    strict_edge_types: bool = PydanticField(
        default=False,
        description=(
            "When True, skip rows whose resolved (source_type, target_type) pair "
            "is not pre-declared in the resource edge_config at init. "
            "When False (default), dynamic pairs are registered at runtime."
        ),
    )
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

    @model_validator(mode="after")
    def validate_type_sources(self) -> "EdgeActorConfig":
        if self.links is not None:
            # Multi-link mode: top-level source/target fields must all be absent.
            has_single = any(
                [
                    self.source,
                    self.target,
                    self.source_type_field,
                    self.target_type_field,
                    self.source_role,
                    self.target_role,
                ]
            )
            if has_single:
                raise ValueError(
                    "edge 'links' is mutually exclusive with top-level "
                    "from/to/source_type_field/target_type_field/source_role/target_role."
                )
            return self

        # Single-intent mode: resolve role aliases → type_field.
        # Use object.__setattr__ to bypass validate_assignment re-triggering this validator.
        if self.source_role is not None:
            if self.source_type_field is not None:
                raise ValueError(
                    "source_role and source_type_field are mutually exclusive."
                )
            object.__setattr__(self, "source_type_field", self.source_role)
        if self.target_role is not None:
            if self.target_type_field is not None:
                raise ValueError(
                    "target_role and target_type_field are mutually exclusive."
                )
            object.__setattr__(self, "target_type_field", self.target_role)

        # Each side needs exactly one of: static type or dynamic slot.
        if self.source is None and self.source_type_field is None:
            raise ValueError("edge step requires 'from' (source) or source_type_field.")
        if self.target is None and self.target_type_field is None:
            raise ValueError("edge step requires 'to' (target) or target_type_field.")
        if self.source is not None and self.source_type_field is not None:
            raise ValueError("'from' and source_type_field are mutually exclusive.")
        if self.target is not None and self.target_type_field is not None:
            raise ValueError("'to' and target_type_field are mutually exclusive.")
        # Mixed mode (one static + one dynamic) is valid; both-static is pure static mode.
        return self

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
    """Configuration for a VertexRouterActor.

    Field handling matches :class:`VertexActorConfig`: optional router-level ``from`` /
    ``from_doc`` (and per-type ``vertex_from_map``), optional ``keep_fields``, and the
    same merged observation dict is passed to the lazily created :class:`VertexActor`
    (no separate slice / rename layer).
    """

    type: Literal["vertex_router"] = PydanticField(
        default="vertex_router", description="Actor type discriminator"
    )
    type_field: str = PydanticField(
        ...,
        description=(
            "Key on the merged observation (document + same-location transform buffer) "
            "whose value determines the target vertex type (after type_map). Use the "
            "actual column name (e.g. ``s__class_name`` or ``p_kind``)."
        ),
    )
    type_map: dict[str, str] | None = PydanticField(
        default=None,
        description="Map raw document values to vertex type names.",
    )
    vertex_from_map: dict[str, dict[str, str]] | None = PydanticField(
        default=None,
        description="Per-vertex-type field projection.",
    )
    role: str | None = PydanticField(
        default=None,
        description=(
            "Named accumulator slot segment. When set, vertices are stored at "
            "lindex.extend((role, 0)). When omitted, the slot segment defaults to type_field. "
            "A downstream edge step references this slot via source_type_field / target_type_field "
            "(or source_role / target_role) using the same segment name."
        ),
    )
    from_doc: dict[str, str] | None = PydanticField(
        default=None,
        alias="from",
        description=(
            "Default projection {vertex_field: doc_field} for all routed vertex types. "
            "Per-type vertex_from_map overrides this for a given resolved type when that type "
            "is present as a key in vertex_from_map."
        ),
    )
    keep_fields: list[str] | None = PydanticField(
        default=None,
        description=(
            "Forwarded to each lazily created VertexActor: restrict passthrough to this "
            "subset of vertex property names (same semantics as vertex.keep_fields)."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def set_type(cls, data: Any) -> Any:
        if isinstance(data, dict) and "type_field" in data and "type" not in data:
            data = dict(data)
            data["type"] = "vertex_router"
        return data


ActorConfig = Annotated[
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig,
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
] = TypeAdapter(
    VertexActorConfig
    | TransformActorConfig
    | EdgeActorConfig
    | DescendActorConfig
    | VertexRouterActorConfig
)
