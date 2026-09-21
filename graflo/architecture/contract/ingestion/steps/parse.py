"""Parsing and validation of actor configuration."""

from __future__ import annotations

from typing import Any, Never, cast

from pydantic import ValidationError

from .models import (
    ActorConfig,
    DescendActorConfig,
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


def _raise_step_validation_error(data: dict[str, Any], err: ValidationError) -> Never:
    """Raise a concise, user-facing validation error for malformed actor steps."""
    keys = ", ".join(sorted(data.keys()))
    is_legacy_map = "map" in data and "rename" not in data
    if (
        not is_legacy_map
        and data.get("type") == "transform"
        and isinstance(data.get("transform"), dict)
    ):
        inner = data["transform"]
        is_legacy_map = "map" in inner and "rename" not in inner

    if is_legacy_map:
        raise ValueError(
            "Invalid transform step: `map` is legacy syntax. "
            "Use `rename` (for field renaming) or `call` (for function transforms). "
            f"Step keys: [{keys}]."
        ) from err
    if data.get("type") == "transform":
        raise ValueError(
            "Invalid transform step. Expected exactly one of `rename` or `call`. "
            f"Step keys: [{keys}]."
        ) from err
    if data.get("type") == "edge":
        # An edge step is recognised; what it broke is one of the step's own
        # rules, and the rule's wording says what to change.
        rules = "; ".join(
            dict.fromkeys(
                str(error["msg"]).removeprefix("Value error, ")
                for error in err.errors()
            )
        )
        raise ValueError(f"Invalid edge step: {rules} Step keys: [{keys}].") from err
    raise ValueError(
        "Invalid actor step configuration. "
        "Supported step forms include `vertex`, `transform`, `edge`, `descend`, "
        "and `vertex_router`. "
        f"Step keys: [{keys}]."
    ) from err


def validate_actor_step(
    data: dict[str, Any],
) -> ActorConfig:
    """Validate a normalized step dict as ActorConfig (discriminated union)."""
    try:
        return _actor_config_adapter.validate_python(data)
    except ValidationError as err:
        _raise_step_validation_error(data, err)


def canonical_actor_step(step: dict[str, Any]) -> dict[str, Any]:
    """One spelling for a pipeline step, whatever spelling it was authored in.

    A step accepts several equivalent spellings -- ``{"vertex": v}`` with or
    without an explicit ``type``, ``{"edge": {...}}`` or its flat body,
    ``from``/``to`` or ``source``/``target``, a ``descend`` with ``apply`` or
    ``pipeline``. Stored as authored, two manifests meaning the same thing
    compare and hash differently. This validates the step into its actor model
    and renders the minimal canonical dict with the discriminator made explicit.

    Lists inside a step keep their order: whether any of them is a set has not
    been established, and preserving is the safe direction for a content hash.

    A step that does not validate on its own is returned unchanged, so a
    canonical form never fails where the raw one would have been accepted.
    """
    try:
        model = _actor_config_adapter.validate_python(normalize_actor_step(step))
    except (ValidationError, ValueError, TypeError):
        return step
    return {"type": model.type, **model.to_minimal_canonical_dict()}


def parse_root_config(
    *args: Any,
    **kwargs: Any,
) -> ActorConfig:
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
        configs = []
        for step in pipeline:
            normalized = normalize_actor_step(step)
            try:
                configs.append(_actor_config_adapter.validate_python(normalized))
            except ValidationError as err:
                _raise_step_validation_error(normalized, err)
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
        return validate_actor_step(normalize_actor_step(step_dict))
    step_kwargs = {k: v for k, v in kwargs.items() if k not in _STEP_STRIP_KEYS}
    return validate_actor_step(normalize_actor_step(step_kwargs))
