"""Declarative resource configuration (YAML/manifest contract)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Sequence
from typing import Any

from pydantic import (
    AliasChoices,
    SerializationInfo,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.steps.models import (
    ActorConfig,
    DescendActorConfig,
    EdgeActorConfig,
    VertexActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.contract.ingestion.steps.parse import as_actor_step
from graflo.architecture.contract.ingestion.sync import ResourceSync
from graflo.architecture.graph_types.enums import EncodingType
from graflo.architecture.graph_types.identifiers import EdgeId
from graflo.architecture.graph_types.index_config import Weight
from graflo.architecture.schema.edge import Edge

logger = logging.getLogger(__name__)


def collect_vertex_names_from_pipeline(steps: Sequence[ActorConfig]) -> set[str]:
    """Vertex names the steps reference, at any level."""
    names: set[str] = set()
    for step in steps:
        if isinstance(step, VertexActorConfig):
            names.add(step.vertex)
        elif isinstance(step, VertexRouterActorConfig):
            names |= step.mapped_classes()
            names |= set(step.vertex_from_map or {})
            names |= set(step.vertex_types or ())
        elif isinstance(step, EdgeActorConfig):
            for intent in step.intents():
                names |= {v for v in (intent.source, intent.target) if v is not None}
            for weight in step.vertex_weights:
                name = weight.get("name") if isinstance(weight, dict) else None
                if isinstance(name, str):
                    names.add(name)
        elif isinstance(step, DescendActorConfig):
            names |= collect_vertex_names_from_pipeline(step.pipeline)
    return names


def step_produces_vertices(
    step: ActorConfig, *, known_vertices: Collection[str] | None = None
) -> set[str]:
    """Vertex names a single (non-recursive) actor step *produces*.

    Production, not reference: an ``edge`` step names endpoints it looks up, so
    it is not counted. A ``vertex_router`` produces its reach
    (:meth:`VertexRouterActorConfig.reach`). For an unbounded router without
    *known_vertices* that is unknown, and the answer is its *explicit* targets
    -- the classes its ``type_map`` selects. A ``vertex_from_map`` key is not
    one: it projects a class the router reaches, and naming it does not make
    the router produce it.
    """
    if isinstance(step, VertexActorConfig):
        return {step.vertex}
    if isinstance(step, VertexRouterActorConfig):
        reach = step.reach(known_vertices)
        return set(step.mapped_classes() if reach is None else reach)
    return set()


def step_looks_up(step: ActorConfig, vertex: str) -> bool:
    """Whether *step* only looks *vertex* up -- matches it, never writes it.

    A ``vertex`` step says so with ``lookup_only: true``; a ``vertex_router``
    with ``lookup_only: true`` (every class it routes to, within its
    ``vertex_types``) or a list naming *vertex*.
    """
    if isinstance(step, VertexActorConfig):
        return step.vertex == vertex and step.lookup_only
    if isinstance(step, VertexRouterActorConfig):
        if step.lookup_only is True:
            return step.admits(vertex)
        return isinstance(step.lookup_only, list) and vertex in step.lookup_only
    return False


def step_finds(
    step: ActorConfig,
    vertex: str,
    *,
    known_vertices: Collection[str] | None = None,
) -> str | None:
    """The secondary identity *step* finds *vertex* by; ``None`` when it does not.

    A ``vertex`` step names it in ``find``; a ``vertex_router`` in its per-class
    ``find`` map, for a class the router can reach over *known_vertices*.
    """
    if isinstance(step, VertexActorConfig):
        return step.find if step.vertex == vertex else None
    if isinstance(step, VertexRouterActorConfig):
        selector = step.find_for(vertex)
        reach = step.reach(known_vertices)
        if selector is not None and (reach is None or vertex in reach):
            return selector
    return None


def steps_producing(
    steps: Sequence[ActorConfig], vertex: str, *, known_vertices: Collection[str]
) -> list[ActorConfig]:
    """Every step at any level of *steps* that produces *vertex*."""
    out: list[ActorConfig] = []
    for step in steps:
        if vertex in step_produces_vertices(step, known_vertices=known_vertices):
            out.append(step)
        if isinstance(step, DescendActorConfig):
            out.extend(
                steps_producing(step.pipeline, vertex, known_vertices=known_vertices)
            )
    return out


def pipeline_find_selectors(
    pipeline: Sequence[ActorConfig], *, known_vertices: Collection[str] | None = None
) -> dict[str, str]:
    """The secondary identity each class is found by, over every level of *pipeline*.

    Raises:
        ValueError: A class gets two different selectors, or is produced both
            by a ``find`` step and by a step without ``find`` (a plain write or
            a ``lookup_only`` reference by primary identity): edges to it would
            have no single selector.
    """
    found: dict[str, set[str]] = {}
    plain: set[str] = set()
    for step in iter_steps(pipeline):
        for vertex in step_produces_vertices(step, known_vertices=known_vertices):
            selector = step_finds(step, vertex, known_vertices=known_vertices)
            if selector is None:
                plain.add(vertex)
            else:
                found.setdefault(vertex, set()).add(selector)
    for vertex, selectors in sorted(found.items()):
        if len(selectors) > 1:
            raise ValueError(
                f"vertex {vertex!r} is found by several identities "
                f"{sorted(selectors)}; a resource finds a class by one"
            )
        if vertex in plain:
            raise ValueError(
                f"vertex {vertex!r} is produced both by a find step "
                f"({min(selectors)!r}) and by a step without find; "
                "declare explicit edge selectors or split the resource"
            )
    return {vertex: next(iter(sel)) for vertex, sel in found.items()}


def iter_steps(pipeline: Sequence[ActorConfig]) -> list[ActorConfig]:
    """Every step of *pipeline*, depth first, each ``descend`` before its children."""
    out: list[ActorConfig] = []
    for step in pipeline:
        out.append(step)
        if isinstance(step, DescendActorConfig):
            out.extend(iter_steps(step.pipeline))
    return out


def role_reach(pipeline: Sequence[ActorConfig]) -> dict[str, frozenset[str] | None]:
    """What each accumulator role in *pipeline* can hold, at any level.

    A ``vertex`` step with a ``role`` holds its class; a router's role (its
    ``role``, else its ``type_field``) holds its reach, ``None`` when that is
    any class. Several producers of one role add up.
    """
    roles: dict[str, frozenset[str] | None] = {}

    def hold(role: str, classes: frozenset[str] | None) -> None:
        if role in roles and roles[role] is None:
            return
        roles[role] = (
            None if classes is None else roles.get(role, frozenset()) | classes
        )

    for step in iter_steps(pipeline):
        if isinstance(step, VertexActorConfig) and step.role is not None:
            hold(step.role, frozenset({step.vertex}))
        elif isinstance(step, VertexRouterActorConfig):
            hold(step.slot, step.reach())
    return roles


def _level_produces(level: Sequence[ActorConfig], vertex: str) -> bool:
    return any(vertex in step_produces_vertices(step) for step in level)


def _level_has_unbounded_router(level: Sequence[ActorConfig]) -> bool:
    return any(
        isinstance(step, VertexRouterActorConfig) and step.unbounded for step in level
    )


def _level_has_pass_through_router(level: Sequence[ActorConfig]) -> bool:
    return any(
        isinstance(step, VertexRouterActorConfig) and step.passes_through
        for step in level
    )


def find_vertex_producing_levels(
    steps: Sequence[ActorConfig],
    vertex: str,
    *,
    known_vertices: Collection[str] | None = None,
) -> list[list[int]]:
    """Index paths of every pipeline level with a step producing *vertex*.

    A path indexes one level's steps per element, descending through ``descend``
    steps: ``[]`` is the root level, ``[2]`` the level inside the root's third
    step, ``[2, 0]`` one further down. Paths are returned outermost-first.

    This is how a level-targeted op finds where to act. The level matters
    because an actor reads its transform buffer at its own ``LocationIndex``
    with no ancestor fallback, so a derivation appended at the root is invisible
    to a vertex produced under a ``descend``.

    Two tiers. Levels with an *explicit* producer — a ``vertex`` step, a
    router whose table names the class, or one whose ``vertex_types`` lists
    it — decide when any exist. Only when none does, and *known_vertices*
    declares the class, every level holding an unbounded ``vertex_router``
    counts: the router routes the raw discriminator value as the class name,
    which is the whole mechanism of a router without a ``type_map``. An
    explicit table outranks pass-through so that adding one dynamic router
    elsewhere never turns a resolved level ambiguous.
    """
    explicit = _walk_levels(steps, lambda level: _level_produces(level, vertex))
    if explicit or known_vertices is None or vertex not in known_vertices:
        return explicit
    return _walk_levels(steps, _level_has_unbounded_router)


def pipeline_has_unbounded_router(steps: Sequence[ActorConfig]) -> bool:
    """Whether any level of *steps* holds a router that can produce any class.

    An unbounded router routes an unmapped discriminator value as the class
    name, so a pipeline holding one can produce any class the schema declares
    — not only the names its steps state. Anything scoping a schema to a
    resource by the names its pipeline mentions must widen to every class when
    this is true, or the router silently drops each record whose class it did
    not name. A closed or bounded router's classes are its reach, which the
    pipeline names.
    """
    return bool(_walk_levels(steps, _level_has_unbounded_router))


def pipeline_has_pass_through_router(steps: Sequence[ActorConfig]) -> bool:
    """Whether any level of *steps* holds a router routing unmapped values as-is."""
    return bool(_walk_levels(steps, _level_has_pass_through_router))


def pipeline_level(
    steps: Sequence[ActorConfig], path: Sequence[int]
) -> list[ActorConfig]:
    """The steps of the level *path* addresses (see :func:`find_vertex_producing_levels`).

    Raises:
        ValueError: An index on the way is out of range or is not a ``descend``.
    """
    level = list(steps)
    for depth, index in enumerate(path):
        walked = list(path[:depth]) or "root"
        if not 0 <= index < len(level):
            raise ValueError(
                f"pipeline path {list(path)} does not resolve: index {index} is out "
                f"of range at level {walked}"
            )
        step = level[index]
        if not isinstance(step, DescendActorConfig):
            raise ValueError(
                f"pipeline path {list(path)} does not resolve: step {index} at "
                f"level {walked} is a {step.type!r} step, not a descend — only a "
                "descend owns a nested level"
            )
        level = list(step.pipeline)
    return level


def _walk_levels(
    steps: Sequence[ActorConfig], matches: Callable[[Sequence[ActorConfig]], bool]
) -> list[list[int]]:
    found: list[list[int]] = []

    def walk(level: Sequence[ActorConfig], path: list[int]) -> None:
        if matches(level):
            found.append(list(path))
        for index, step in enumerate(level):
            if isinstance(step, DescendActorConfig):
                walk(step.pipeline, [*path, index])

    walk(steps, [])
    return found


class EdgeInferSpec(ConfigBaseModel):
    """Selector for controlling inferred edge emission."""

    source: str = PydanticField(..., description="Edge source vertex name.")
    target: str = PydanticField(..., description="Edge target vertex name.")
    relation: str | None = PydanticField(
        default=None,
        description=(
            "Optional relation discriminator. If omitted, selector applies to all relations "
            "for (source, target)."
        ),
    )

    @property
    def edge_id(self) -> EdgeId:
        return self.source, self.target, self.relation

    def matches(self, edge_id: EdgeId) -> bool:
        source, target, relation = edge_id
        return (
            self.source == source
            and self.target == target
            and (self.relation is None or self.relation == relation)
        )


class ResourceExtraWeightEntry(ConfigBaseModel):
    """Schema edge plus optional vertex-derived weight rules for DB enrichment."""

    edge: Edge
    vertex_weights: list[Weight] = PydanticField(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _from_yaml(cls, data: Any) -> Any:
        if data is None:
            return data
        if isinstance(data, Edge):
            return {"edge": data, "vertex_weights": []}
        if not isinstance(data, dict):
            raise TypeError(
                f"extra_weights item must be dict or Edge, got {type(data)}"
            )
        d = dict(data)
        vw_raw = d.pop("vertex_weights", None) or []
        if not isinstance(vw_raw, list):
            vw_raw = [vw_raw]
        v_w = [Weight.model_validate(x) for x in vw_raw]
        if "edge" in d and isinstance(d["edge"], dict):
            edge = Edge.model_validate(dict(d.pop("edge")))
            if d:
                raise ValueError(
                    f"extra_weights entry has unexpected keys with 'edge': {sorted(d)}"
                )
            return {"edge": edge, "vertex_weights": v_w}
        edge = Edge.model_validate(d)
        return {"edge": edge, "vertex_weights": v_w}


class ResourceConfig(ConfigBaseModel):
    """Declarative resource definition (serializable contract)."""

    model_config = {"extra": "forbid"}

    name: str = PydanticField(
        ...,
        description="Name of the resource (e.g. table or file identifier).",
    )
    pipeline: list[ActorConfig] = PydanticField(
        ...,
        description="Pipeline of actor steps to apply in sequence (vertex, edge, transform, "
        "descend, vertex_router). Input accepts every step spelling, e.g. "
        '{"vertex": "user"} or {"edge": {"from": "a", "to": "b"}}; a step that is not '
        "valid on its own is refused. Dumped in that compact shorthand.",
        validation_alias=AliasChoices("pipeline", "apply"),
    )
    encoding: EncodingType = PydanticField(
        default=EncodingType.UTF_8,
        description="Character encoding for input/output (e.g. utf-8, ISO-8859-1).",
    )
    merge_collections: list[str] = PydanticField(
        default_factory=list,
        description=(
            "Not implemented: nothing reads it, so a non-empty value is refused. "
            "Documents of one vertex fuse by identity without it."
        ),
    )
    extra_weights: list[ResourceExtraWeightEntry] = PydanticField(
        default_factory=list,
        description="Additional edge attribute / vertex-weight enrichment for this resource.",
    )
    types: dict[str, str] = PydanticField(
        default_factory=dict,
        description='Field name to Python type expression for casting (e.g. {"amount": "float"}).',
    )
    infer_edges: bool = PydanticField(
        default=True,
        description=(
            "If True, infer edges from current vertex population. "
            "If False, emit only edges explicitly declared as edge actors in the pipeline."
        ),
    )
    infer_edge_only: list[EdgeInferSpec] = PydanticField(
        default_factory=list,
        description=(
            "Optional allow-list for inferred edges. Applies only to inferred (greedy) edges, "
            "not explicit edge actors."
        ),
    )
    infer_edge_except: list[EdgeInferSpec] = PydanticField(
        default_factory=list,
        description=(
            "Optional deny-list for inferred edges. Applies only to inferred (greedy) edges, "
            "not explicit edge actors."
        ),
    )
    drop_trivial_input_fields: bool = PydanticField(
        default=False,
        description=(
            "If True, remove top-level input keys whose value is None or the empty string before "
            "the actor pipeline runs."
        ),
    )
    fail_fast: bool = PydanticField(
        default=False,
        description=(
            "If True, a transform step fails when required input keys are missing in the "
            "current document (rename: all source keys must be present; call: all input keys). "
            "If False (default), rename applies only to keys present in the document and "
            "functional transforms skip the step when inputs are missing."
        ),
    )
    tolerate_transform_errors: bool = PydanticField(
        default=True,
        description=(
            "If True, a failing transform step sets its declared output fields to None, "
            "records the error, and continues the pipeline."
        ),
    )
    sync: ResourceSync | None = PydanticField(
        default=None,
        description=(
            "Record sync ownership: the vertex type and the edges this resource's "
            "records replace when re-ingested in sync mode."
        ),
    )

    @field_validator("pipeline", mode="before")
    @classmethod
    def _steps_as_models(cls, value: Any) -> Any:
        if not isinstance(value, list):
            return value
        return [as_actor_step(step) for step in value]

    @field_serializer("pipeline", mode="plain")
    def _dump_steps_as_shorthand(
        self, pipeline: list[ActorConfig], info: SerializationInfo
    ) -> list[dict[str, Any]]:
        return [step.to_shorthand(info.mode) for step in pipeline]

    @model_validator(mode="after")
    def _validate_policy(self) -> ResourceConfig:
        if self.infer_edge_only and self.infer_edge_except:
            raise ValueError(
                "Resource infer_edge_only and infer_edge_except are mutually exclusive."
            )
        if self.merge_collections:
            raise ValueError(
                "merge_collections is not implemented: nothing reads it. Documents "
                "of one vertex fuse by identity without it; remove the key."
            )
        return self

    def collect_vertex_names(self) -> set[str]:
        """Vertex types referenced by this resource (pipeline and related config)."""
        names = collect_vertex_names_from_pipeline(self.pipeline)
        for spec in self.infer_edge_only:
            names.add(spec.source)
            names.add(spec.target)
        for spec in self.infer_edge_except:
            names.add(spec.source)
            names.add(spec.target)
        for entry in self.extra_weights:
            names.add(entry.edge.source)
            names.add(entry.edge.target)
            for weight in entry.vertex_weights:
                if weight.name is not None:
                    names.add(weight.name)
        return names

    def canonical_field_payload(self, field_name: str) -> Any | None:
        """Canonical rendering of *field_name*, when it differs from its dump.

        Consulted by content hashing and by the manifest differ. ``pipeline``
        is stored as authored dicts, and a step has several equivalent
        spellings; this renders each step in one spelling (see
        :func:`~graflo.architecture.contract.ingestion.steps.parse.canonical_actor_step`).
        ``None`` means the field's ordinary dump is already canonical.
        """
        if field_name != "pipeline":
            return None
        return [
            {"type": step.type, **step.to_minimal_canonical_dict()}
            for step in self.pipeline
        ]

    def pipeline_actor_count(self) -> int:
        """Count actors in the pipeline without binding schema context."""
        from graflo.architecture.pipeline.runtime.actor import ActorWrapper

        return ActorWrapper(*self.pipeline).count()


# Internal-only alias; prefer ResourceConfig in new code.
Resource = ResourceConfig
