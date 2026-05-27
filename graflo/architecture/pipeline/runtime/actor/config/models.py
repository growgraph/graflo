"""Pydantic models for actor configuration."""

from __future__ import annotations

from typing import Annotated, Any, Literal, cast

from pydantic import Field as PydanticField, TypeAdapter, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.transform import DressConfig
from graflo.architecture.edge_derivation import EdgeDerivation
from graflo.architecture.schema.vertex import VertexName

from .normalize import normalize_actor_step


class VertexExtractionOptionsConfig(ConfigBaseModel):
    """Shared field-extraction options for vertex-like actors."""

    from_doc: dict[str, str] | None = PydanticField(
        default=None,
        alias="from",
        description="Projection: {vertex_field: doc_field}.",
    )
    keep_fields: list[str] | None = PydanticField(
        default=None, description="Optional list of fields to keep"
    )
    extraction_scope: Literal["full", "mapped_only"] = PydanticField(
        default="full",
        description=(
            "Field extraction policy. full (default) includes passthrough for remaining "
            "schema properties from the merged observation "
            "(doc + same-location transform buffer), while mapped_only limits extraction "
            "to explicit field mappings declared in from."
        ),
    )
    role: str | None = PydanticField(
        default=None,
        description=(
            "Optional accumulator slot segment used for storage/addressing. "
            "Vertex-like actors store observations at lindex.extend((role, 0)) when set. "
            "When omitted, actor-specific defaults may apply."
        ),
    )


class VertexActorConfig(VertexExtractionOptionsConfig):
    """Configuration for a VertexActor."""

    type: Literal["vertex"] = PydanticField(
        default="vertex", description="Actor type discriminator"
    )
    vertex: VertexName = PydanticField(
        ..., description="Name of the vertex type to create"
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
        if self.use is None:
            has_inline_callable = self.module is not None or self.foo is not None
            if has_inline_callable and (self.module is None or self.foo is None):
                raise ValueError(
                    "Inline call functions require both call.module and call.foo."
                )
            if not has_inline_callable and self.dress is None:
                raise ValueError(
                    "call must provide either use, both module+foo, or dress shorthand."
                )
            if self.dress is not None and not self.input:
                raise ValueError("dress shorthand requires call.input.")
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

    Slot resolution uses role-first semantics (``source_role`` / ``target_role``).
    Legacy aliases (``source_type_field`` / ``target_type_field``) are accepted and
    canonicalized to their role counterparts. The slot name is the accumulator segment
    populated by an upstream ``vertex`` step with a matching ``role``, or by
    ``vertex_router.role`` (which defaults to ``type_field`` when omitted).
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
            "Accumulator slot segment for the source vertex (same name as upstream "
            "vertex/vertex_router role). Exclusive with 'from' and source_role."
        ),
    )
    target_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Accumulator slot segment for the target vertex (same name as upstream "
            "vertex/vertex_router role). Exclusive with 'to' and target_role."
        ),
    )
    source_role: str | None = PydanticField(
        default=None,
        description=(
            "Role-first alias for source_type_field (same accumulator segment name). "
            "When both are set, values must match."
        ),
    )
    target_role: str | None = PydanticField(
        default=None,
        description=(
            "Role-first alias for target_type_field (same accumulator segment name). "
            "When both are set, values must match."
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

    @staticmethod
    def _canonicalize_slot_key(
        role: str | None,
        legacy_type_field: str | None,
        *,
        role_name: str,
        type_field_name: str,
        context: str,
    ) -> str | None:
        """Canonicalize legacy slot-name fields to role-first semantics."""
        if (
            role is not None
            and legacy_type_field is not None
            and role != legacy_type_field
        ):
            raise ValueError(
                f"{role_name} and {type_field_name} must match when both are set in {context}."
            )
        return role if role is not None else legacy_type_field

    @model_validator(mode="after")
    def resolve_and_validate(self) -> "EdgeLinkConfig":
        # Canonicalize to role-first slot names while preserving legacy key input.
        # Use object.__setattr__ to bypass validate_assignment re-triggering this validator.
        source_role = self._canonicalize_slot_key(
            self.source_role,
            self.source_type_field,
            role_name="source_role",
            type_field_name="source_type_field",
            context="an edge link",
        )
        target_role = self._canonicalize_slot_key(
            self.target_role,
            self.target_type_field,
            role_name="target_role",
            type_field_name="target_type_field",
            context="an edge link",
        )
        object.__setattr__(self, "source_role", source_role)
        object.__setattr__(self, "target_role", target_role)
        object.__setattr__(self, "source_type_field", None)
        object.__setattr__(self, "target_type_field", None)

        # Each side needs exactly one of: static type or slot reference.
        if self.source is None and self.source_role is None:
            raise ValueError(
                "edge link requires 'from' (source), source_role, or source_type_field."
            )
        if self.target is None and self.target_role is None:
            raise ValueError(
                "edge link requires 'to' (target), target_role, or target_type_field."
            )
        if self.source is not None and self.source_role is not None:
            raise ValueError(
                "'from' and source_type_field/source_role are mutually exclusive."
            )
        if self.target is not None and self.target_role is not None:
            raise ValueError(
                "'to' and target_type_field/target_role are mutually exclusive."
            )
        return self


class EdgeActorConfig(ConfigBaseModel):
    """Configuration for an EdgeActor (logical edge + ingestion derivation; flat YAML).

    **Single-intent mode** (default): declare source/target via ``from``/``to`` (static
    vertex type names) or ``source_role``/``target_role`` (slot-based dynamic
    resolution; ``source_type_field``/``target_type_field`` remain accepted aliases).
    One edge intent is emitted per row.

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
            "VertexRouterActor role, inferred from type_field when role is omitted). EdgeActor scans "
            "acc_vertex for data at lindex.extend((source_type_field, 0)) to resolve the "
            "source type dynamically. Legacy alias for source_role."
        ),
    )
    target_type_field: str | None = PydanticField(
        default=None,
        description=(
            "Accumulator slot segment for the target vertex (same name as upstream "
            "VertexRouterActor role, inferred from type_field when role is omitted). "
            "Legacy alias for target_role."
        ),
    )
    source_role: str | None = PydanticField(
        default=None,
        description=(
            "Role slot name for the source vertex — role-first alias for source_type_field. "
            "When both are set, values must match."
        ),
    )
    target_role: str | None = PydanticField(
        default=None,
        description=(
            "Role slot name for the target vertex — role-first alias for target_type_field. "
            "When both are set, values must match."
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

    @staticmethod
    def _canonicalize_slot_key(
        role: str | None,
        legacy_type_field: str | None,
        *,
        role_name: str,
        type_field_name: str,
        context: str,
    ) -> str | None:
        """Canonicalize legacy slot-name fields to role-first semantics."""
        if (
            role is not None
            and legacy_type_field is not None
            and role != legacy_type_field
        ):
            raise ValueError(
                f"{role_name} and {type_field_name} must match when both are set in {context}."
            )
        return role if role is not None else legacy_type_field

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

        # Single-intent mode: canonicalize to role-first slot names.
        # Use object.__setattr__ to bypass validate_assignment re-triggering this validator.
        source_role = self._canonicalize_slot_key(
            self.source_role,
            self.source_type_field,
            role_name="source_role",
            type_field_name="source_type_field",
            context="an edge step",
        )
        target_role = self._canonicalize_slot_key(
            self.target_role,
            self.target_type_field,
            role_name="target_role",
            type_field_name="target_type_field",
            context="an edge step",
        )
        object.__setattr__(self, "source_role", source_role)
        object.__setattr__(self, "target_role", target_role)
        object.__setattr__(self, "source_type_field", None)
        object.__setattr__(self, "target_type_field", None)

        # Each side needs exactly one of: static type or dynamic slot.
        if self.source is None and self.source_role is None:
            raise ValueError(
                "edge step requires 'from' (source), source_role, or source_type_field."
            )
        if self.target is None and self.target_role is None:
            raise ValueError(
                "edge step requires 'to' (target), target_role, or target_type_field."
            )
        if self.source is not None and self.source_role is not None:
            raise ValueError("'from' and source_type_field are mutually exclusive.")
        if self.target is not None and self.target_role is not None:
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


class VertexRouterActorConfig(VertexExtractionOptionsConfig):
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
            "whose value determines the target vertex type (after type_map). "
            "This is a discriminator field, not the internal slot key. Use the "
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

    @model_validator(mode="before")
    @classmethod
    def set_type(cls, data: Any) -> Any:
        if isinstance(data, dict) and "type_field" in data and "type" not in data:
            data = dict(data)
            data["type"] = "vertex_router"
        return data

    @model_validator(mode="after")
    def normalize_role(self) -> "VertexRouterActorConfig":
        if self.role is None:
            object.__setattr__(self, "role", self.type_field)
        return self


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
