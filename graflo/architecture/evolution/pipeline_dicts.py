"""The one place pipeline steps cross between actor models and plain dicts.

A resource's pipeline is a list of actor configs. The rewrite walkers of the
evolution engine still edit steps as dicts: they take them from
:func:`pipeline_to_dicts` and hand the result back through
:func:`pipeline_from_dicts`, which validates every step again. Nothing else in
the engine sees a step as a dict.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import wraps
from typing import Any, Concatenate, ParamSpec, TypeVar

from graflo.architecture.contract.ingestion.steps import (
    ActorConfig,
    DescendActorConfig,
    VertexRouterActorConfig,
    as_actor_step,
    normalize_actor_step,
)

P = ParamSpec("P")
R = TypeVar("R")


def step_to_dict(step: ActorConfig) -> dict[str, Any]:
    """*step* as a flat dict with its ``type``; a ``descend``'s children under ``pipeline``."""
    body = step.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
        exclude_defaults=True,
        exclude={"type", "pipeline"},
    )
    out: dict[str, Any] = {"type": step.type, **body}
    if isinstance(step, DescendActorConfig):
        out["pipeline"] = pipeline_to_dicts(step.pipeline)
    return out


def pipeline_to_dicts(steps: Sequence[ActorConfig]) -> list[dict[str, Any]]:
    """*steps* as dicts for a rewrite walker; see :func:`step_to_dict`."""
    return [step_to_dict(step) for step in steps]


def pipeline_from_dicts(steps: Sequence[dict[str, Any]]) -> list[ActorConfig]:
    """Steps a walker rewrote, validated back into actor configs.

    Raises:
        ValueError: A rewritten step is not a valid step.
    """
    return [as_actor_step(step) for step in steps]


def rewrites_pipeline(
    walker: Callable[Concatenate[list[dict[str, Any]], P], list[dict[str, Any]]],
) -> Callable[Concatenate[Sequence[ActorConfig], P], list[ActorConfig]]:
    """A dict walker that rewrites a pipeline, typed at both ends.

    The walker receives :func:`pipeline_to_dicts` of the pipeline and its
    result goes back through :func:`pipeline_from_dicts`.
    """

    @wraps(walker)
    def typed(
        pipeline: Sequence[ActorConfig], *args: P.args, **kwargs: P.kwargs
    ) -> list[ActorConfig]:
        return pipeline_from_dicts(walker(pipeline_to_dicts(pipeline), *args, **kwargs))

    return typed


def reads_pipeline(
    walker: Callable[Concatenate[list[dict[str, Any]], P], R],
) -> Callable[Concatenate[Sequence[ActorConfig], P], R]:
    """A dict walker that reads a pipeline, taking the typed pipeline."""

    @wraps(walker)
    def typed(pipeline: Sequence[ActorConfig], *args: P.args, **kwargs: P.kwargs) -> R:
        return walker(pipeline_to_dicts(pipeline), *args, **kwargs)

    return typed


def router_of(step: dict[str, Any]) -> VertexRouterActorConfig:
    """The router a walker holds as a dict, as its model, for the router's own rules.

    Raises:
        ValueError: *step* is not a valid ``vertex_router`` step.
    """
    model = as_actor_step(step)
    if not isinstance(model, VertexRouterActorConfig):
        raise ValueError(f"expected a vertex_router step, got a {model.type!r} step")
    return model


def resolve_pipeline_level(steps: list[Any], path: list[int]) -> list[Any]:
    """Return the live step list *path* addresses inside dict *steps*.

    Mutating the returned list mutates *steps*. Getting that guarantee requires
    rewriting each walked ``descend`` step into its normalized form and storing
    it back: the shorthand spellings (``{descend: {apply: [...]}}``, a bare
    ``{key, apply}``) keep their sub-steps under a different key, so returning
    whatever ``normalize_actor_step`` built would hand back a list nothing
    holds — and an append into it would vanish without a word. Steps off the
    path, and the root level itself, are left exactly as they were.

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
