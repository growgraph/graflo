"""Parsing and validation of actor configuration."""

from __future__ import annotations

from typing import Any, cast

from .models import (
    DescendActorConfig,
    EdgeActorConfig,
    EdgeRouterActorConfig,
    TransformActorConfig,
    VertexActorConfig,
    VertexRouterActorConfig,
    _actor_config_adapter,
)
from .normalize import normalize_actor_step

_STEP_STRIP_KEYS = frozenset(
    {
        "vertex_config",
        "edge_config",
        "infer_edges",
        "infer_edge_only",
        "infer_edge_except",
        "transforms",
        "name",
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
    | EdgeRouterActorConfig
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
    | EdgeRouterActorConfig
):
    """Parse root input into a single ActorConfig (single step or descend pipeline)."""
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
            list_arg = args[0]
            if not all(isinstance(item, dict) for item in list_arg):
                raise ValueError("pipeline must be a list of dict actor steps")
            pipeline = [dict(item) for item in list_arg]
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
                "key": None,
                "any_key": False,
                "pipeline": configs,
            }
        )
    if single is not None:
        step_dict = {k: v for k, v in single.items() if k not in _STEP_STRIP_KEYS}
        return _actor_config_adapter.validate_python(normalize_actor_step(step_dict))
    step_kwargs = {k: v for k, v in kwargs.items() if k not in _STEP_STRIP_KEYS}
    return _actor_config_adapter.validate_python(normalize_actor_step(step_kwargs))
