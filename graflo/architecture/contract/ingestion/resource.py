"""Declarative resource configuration (YAML/manifest contract)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection
from typing import Any

from pydantic import AliasChoices, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.graph_types.enums import EncodingType
from graflo.architecture.graph_types.identifiers import EdgeId
from graflo.architecture.graph_types.index_config import Weight
from graflo.architecture.schema.edge import Edge

logger = logging.getLogger(__name__)


def collect_vertex_names_from_pipeline(steps: list[Any]) -> set[str]:
    """Collect vertex names referenced by pipeline steps (including nested descend)."""
    names: set[str] = set()
    for step in steps:
        if not isinstance(step, dict):
            continue
        normalized = normalize_actor_step(dict(step))
        step_type = normalized.get("type")
        if step_type == "vertex" and isinstance(normalized.get("vertex"), str):
            names.add(normalized["vertex"])
        elif step_type == "vertex_router":
            names |= _router_table_targets(normalized)
            vertex_from_map = normalized.get("vertex_from_map")
            if isinstance(vertex_from_map, dict):
                names |= {key for key in vertex_from_map if isinstance(key, str)}
            names |= _router_bound(normalized) or frozenset()
        elif step_type == "edge":
            links = normalized.get("links")
            for payload in [
                normalized,
                *(link for link in links or [] if isinstance(link, dict)),
            ]:
                for key in ("source", "from", "target", "to"):
                    if isinstance(endpoint := payload.get(key), str):
                        names.add(endpoint)
            vertex_weights = normalized.get("vertex_weights")
            if isinstance(vertex_weights, list):
                for weight in vertex_weights:
                    if isinstance(weight, dict) and isinstance(weight.get("name"), str):
                        names.add(weight["name"])
        elif step_type == "descend":
            sub_pipeline = normalized.get("pipeline")
            if isinstance(sub_pipeline, list):
                names |= collect_vertex_names_from_pipeline(sub_pipeline)
    return names


def step_produces_vertices(
    step: dict[str, Any], *, known_vertices: Collection[str] | None = None
) -> set[str]:
    """Vertex names a single (non-recursive) actor step *produces*.

    Production, not reference: an ``edge`` step names endpoints it looks up, so
    it is not counted. A ``vertex_router`` produces its :func:`router_reach`.
    For an unbounded router without *known_vertices* that is unknown, and the
    answer is its *explicit* targets -- the classes its ``type_map`` selects.
    A ``vertex_from_map`` key is not one: it projects a class the router
    reaches, and naming it does not make the router produce it.
    """
    normalized = normalize_actor_step(dict(step))
    step_type = normalized.get("type")
    if step_type == "vertex" and isinstance(normalized.get("vertex"), str):
        return {normalized["vertex"]}
    if step_type == "vertex_router":
        reach = router_reach(normalized, known_vertices=known_vertices)
        if reach is None:
            return _router_table_targets(normalized)
        return set(reach)
    return set()


def step_looks_up(step: dict[str, Any], vertex: str) -> bool:
    """Whether *step* only looks *vertex* up -- matches it, never writes it.

    A ``vertex`` step says so with ``lookup_only: true``; a ``vertex_router``
    with ``lookup_only: true`` (every class it routes to) or a list naming
    *vertex*.
    """
    normalized = normalize_actor_step(dict(step))
    flag = normalized.get("lookup_only")
    if normalized.get("type") == "vertex":
        return normalized.get("vertex") == vertex and flag is True
    if normalized.get("type") == "vertex_router":
        if flag is True:
            bound = _router_bound(normalized)
            return bound is None or vertex in bound
        return isinstance(flag, list) and vertex in flag
    return False


def route_discriminator(
    step: dict[str, Any], raw: Any, declared: Collection[str]
) -> str | None:
    """The class a ``vertex_router`` sends a record with discriminator *raw* to.

    The runtime's rule: a closed router (``type_map_only``) skips a value its
    table lacks; ``type_map`` translates, and an unmapped value stands for the
    class of that name; a class *declared* lacks, or ``vertex_types``
    excludes, is skipped. ``None`` for a skipped record.
    """
    normalized = normalize_actor_step(dict(step))
    table = _router_table(normalized)
    if normalized.get("type_map_only") and raw not in table:
        return None
    vertex = table.get(raw, raw)
    if not isinstance(vertex, str) or vertex not in declared:
        return None
    bound = _router_bound(normalized)
    if bound is not None and vertex not in bound:
        return None
    return vertex


def router_reach(
    step: dict[str, Any], *, known_vertices: Collection[str] | None = None
) -> frozenset[str] | None:
    """The classes a ``vertex_router`` step can produce; ``None`` for any class.

    A closed router (``type_map_only``) reaches the classes its table names.
    An open one also routes an unmapped value as the class of that name, so it
    reaches every class *known_vertices* declares -- or, without them, any
    class. ``vertex_types`` bounds either: an open router with it reaches
    exactly the listed classes, known or not.
    """
    normalized = normalize_actor_step(dict(step))
    if normalized.get("type") != "vertex_router":
        return frozenset()
    bound = _router_bound(normalized)
    if normalized.get("type_map_only"):
        reach = frozenset(_router_table_targets(normalized))
        return reach if bound is None else reach & bound
    if bound is not None:
        return bound
    if known_vertices is None:
        return None
    return frozenset(_router_table_targets(normalized)) | frozenset(known_vertices)


def is_pass_through_router(step: dict[str, Any]) -> bool:
    """Whether *step* is a ``vertex_router`` routing an unmapped value as itself."""
    normalized = normalize_actor_step(dict(step))
    return normalized.get("type") == "vertex_router" and not normalized.get(
        "type_map_only"
    )


def is_unbounded_router(step: dict[str, Any]) -> bool:
    """Whether *step* is a ``vertex_router`` that can produce any declared class.

    It passes unmapped values through and has no ``vertex_types``.
    """
    normalized = normalize_actor_step(dict(step))
    return is_pass_through_router(normalized) and _router_bound(normalized) is None


def role_reach(pipeline: Any) -> dict[str, frozenset[str] | None]:
    """What each accumulator role in *pipeline* can hold, at any level.

    A ``vertex`` step with a ``role`` holds its class; a router's role (its
    ``role``, else its ``type_field``) holds its :func:`router_reach`, ``None``
    when that is any class. Several producers of one role add up.
    """
    roles: dict[str, frozenset[str] | None] = {}

    def hold(role: str, classes: frozenset[str] | None) -> None:
        if role in roles and roles[role] is None:
            return
        roles[role] = (
            None if classes is None else roles.get(role, frozenset()) | classes
        )

    def walk(item: Any) -> None:
        if isinstance(item, list):
            for sub in item:
                walk(sub)
            return
        if not isinstance(item, dict):
            return
        step = normalize_actor_step(dict(item))
        step_type = step.get("type")
        if step_type == "vertex" and isinstance(step.get("role"), str):
            vertex = step.get("vertex")
            hold(step["role"], frozenset({vertex} if isinstance(vertex, str) else ()))
        elif step_type == "vertex_router":
            role = step.get("role") or step.get("type_field")
            if isinstance(role, str):
                hold(role, router_reach(step))
        elif step_type == "descend":
            walk(step.get("pipeline"))

    walk(pipeline)
    return roles


def _router_table(normalized: dict[str, Any]) -> dict[Any, Any]:
    type_map = normalized.get("type_map")
    return type_map if isinstance(type_map, dict) else {}


def _router_bound(normalized: dict[str, Any]) -> frozenset[str] | None:
    bound = normalized.get("vertex_types")
    if not isinstance(bound, list):
        return None
    return frozenset(v for v in bound if isinstance(v, str))


def _router_table_targets(normalized: dict[str, Any]) -> set[str]:
    return {v for v in _router_table(normalized).values() if isinstance(v, str)}


def _level_produces(level: list[Any], vertex: str) -> bool:
    return any(
        isinstance(step, dict) and vertex in step_produces_vertices(step)
        for step in level
    )


def _level_has_unbounded_router(level: list[Any]) -> bool:
    return any(isinstance(step, dict) and is_unbounded_router(step) for step in level)


def _level_has_pass_through_router(level: list[Any]) -> bool:
    return any(
        isinstance(step, dict) and is_pass_through_router(step) for step in level
    )


def find_vertex_producing_levels(
    steps: list[Any], vertex: str, *, known_vertices: Collection[str] | None = None
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


def pipeline_has_unbounded_router(steps: list[Any]) -> bool:
    """Whether any level of *steps* holds a router that can produce any class.

    An unbounded router routes an unmapped discriminator value as the class
    name, so a pipeline holding one can produce any class the schema declares
    — not only the names its steps state. Anything scoping a schema to a
    resource by the names its pipeline mentions must widen to every class when
    this is true, or the router silently drops each record whose class it did
    not name. A closed or bounded router's classes are its
    :func:`router_reach`, which the pipeline names.
    """
    return bool(_walk_levels(steps, _level_has_unbounded_router))


def pipeline_has_pass_through_router(steps: list[Any]) -> bool:
    """Whether any level of *steps* holds a router routing unmapped values as-is."""
    return bool(_walk_levels(steps, _level_has_pass_through_router))


def _walk_levels(
    steps: list[Any], matches: Callable[[list[Any]], bool]
) -> list[list[int]]:
    found: list[list[int]] = []

    def walk(level: list[Any], path: list[int]) -> None:
        if matches(level):
            found.append(list(path))
        for index, step in enumerate(level):
            if not isinstance(step, dict):
                continue
            normalized = normalize_actor_step(dict(step))
            if normalized.get("type") != "descend":
                continue
            sub_pipeline = normalized.get("pipeline")
            if isinstance(sub_pipeline, list):
                walk(sub_pipeline, [*path, index])

    walk(steps, [])
    return found


def resolve_pipeline_level(steps: list[Any], path: list[int]) -> list[Any]:
    """Return the live step list *path* addresses inside *steps*.

    Mutating the returned list mutates *steps*. Getting that guarantee requires
    rewriting each walked ``descend`` step into its normalized form and storing
    it back: the shorthand spellings (``{descend: {apply: [...]}}``, a bare
    ``{key, apply}``) keep their sub-steps under a different key, so returning
    whatever ``normalize_actor_step`` built would hand back a list nothing
    holds — and an append into it would vanish without a word. Steps off the
    path, and the root level itself, are left exactly as authored.

    Raises when the path does not resolve. Every index on the way must address
    a ``descend`` step; those are the only steps that own a nested level.
    """
    level = steps
    for depth, index in enumerate(path):
        walked = path[:depth] or "root"
        if not 0 <= index < len(level):
            raise ValueError(
                f"pipeline path {path} does not resolve: index {index} is out of "
                f"range at level {walked}"
            )
        step = level[index]
        if not isinstance(step, dict):
            raise ValueError(
                f"pipeline path {path} does not resolve: step {index} at level "
                f"{walked} is not an actor step"
            )
        normalized = normalize_actor_step(dict(step))
        if normalized.get("type") != "descend":
            raise ValueError(
                f"pipeline path {path} does not resolve: step {index} at level "
                f"{walked} is a {normalized.get('type')!r} step, not a descend — "
                "only a descend owns a nested level"
            )
        sub_pipeline = normalized.get("pipeline")
        normalized["pipeline"] = sub_pipeline if isinstance(sub_pipeline, list) else []
        level[index] = normalized
        level = normalized["pipeline"]
    return level


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
    pipeline: list[dict[str, Any]] = PydanticField(
        ...,
        description="Pipeline of actor steps to apply in sequence (vertex, edge, transform, descend). "
        'Each step is a dict, e.g. {"vertex": "user"} or {"edge": {"from": "a", "to": "b"}}.',
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
        from graflo.architecture.contract.ingestion.steps.parse import (
            canonical_actor_step,
        )

        return [canonical_actor_step(step) for step in self.pipeline]

    def pipeline_actor_count(self) -> int:
        """Count actors in the pipeline without binding schema context."""
        from graflo.architecture.pipeline.runtime.actor import ActorWrapper

        return ActorWrapper(*self.pipeline).count()


# Internal-only alias; prefer ResourceConfig in new code.
Resource = ResourceConfig
