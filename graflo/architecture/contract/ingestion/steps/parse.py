"""Parsing and validation of actor configuration."""

from __future__ import annotations

from typing import Any, Never, cast

from pydantic import ValidationError

from .models import (
    ACTOR_CONFIG_TYPES,
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


#: The model a recognised step is validated as, by its ``type``.
_STEP_MODEL_NAMES = {
    "transform": "TransformActorConfig",
    "edge": "EdgeActorConfig",
}


def _broken_rules(err: ValidationError, step_type: str, model_name: str) -> str:
    """What the step's own model objected to, in the validator's own words.

    The discriminated union reports a step under its ``type`` tag; a step
    validated against every kind reports each under the model's name. Only the
    step's own kind says what to change.
    """
    rules: dict[str, None] = {}
    for error in err.errors():
        loc = error["loc"]
        if not loc or (loc[0] != step_type and model_name not in str(loc[0])):
            continue
        message = str(error["msg"]).rstrip(".")
        if error["type"] == "value_error":
            rules[message.removeprefix("Value error, ")] = None
            continue
        path = ".".join(str(part) for part in loc[1:])
        rules[f"`{path}`: {message}" if path else message] = None
    return "; ".join(rules)


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
    step_type = data.get("type")
    model_name = (
        _STEP_MODEL_NAMES.get(step_type) if isinstance(step_type, str) else None
    )
    if model_name is not None:
        # The step is recognised; what it broke is one of its own rules, and
        # the rule's wording says what to change.
        rules = _broken_rules(err, cast(str, step_type), model_name)
        if rules:
            raise ValueError(
                f"Invalid {step_type} step: {rules}. Step keys: [{keys}]."
            ) from err
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


def as_actor_step(step: ActorConfig | dict[str, Any]) -> ActorConfig:
    """*step* as its actor model: a model is returned as is, a dict in any spelling is validated.

    Raises:
        ValueError: A dict that is not a valid step, with the step's own rule
            that it broke.
        TypeError: Anything else.
    """
    if isinstance(step, ACTOR_CONFIG_TYPES):
        return step
    if isinstance(step, dict):
        return validate_actor_step(normalize_actor_step(step))
    raise TypeError(
        f"a pipeline step is an actor config or a dict, not {type(step).__name__}"
    )


def canonical_actor_step(step: ActorConfig | dict[str, Any]) -> dict[str, Any]:
    """One spelling for a pipeline step, whatever spelling it was authored in.

    A step accepts several equivalent spellings -- ``{"vertex": v}`` with or
    without an explicit ``type``, ``{"edge": {...}}`` or its flat body,
    ``from``/``to`` or ``source``/``target``, a ``descend`` with ``apply`` or
    ``pipeline``. This renders the minimal canonical dict of its actor model
    with the discriminator made explicit; content hashes and the differ read
    it.

    Lists inside a step keep their order: whether any of them is a set has not
    been established, and preserving is the safe direction for a content hash.
    """
    model = as_actor_step(step)
    return {"type": model.type, **model.to_minimal_canonical_dict()}


def parse_root_config(
    *args: Any,
    **kwargs: Any,
) -> ActorConfig:
    """Parse root input into a single ActorConfig (single step or descend pipeline).

    Steps may be actor configs or dicts in any spelling. No input at all is an
    empty pipeline.
    """
    if not args and not kwargs:
        return DescendActorConfig(pipeline=[])
    steps: list[Any] | None = None
    if kwargs and ("apply" in kwargs or "pipeline" in kwargs):
        raw = kwargs.get("pipeline") or kwargs.get("apply")
        steps = [] if raw is None else list(raw) if isinstance(raw, list) else [raw]
    elif len(args) == 1 and isinstance(args[0], list):
        steps = list(args[0])
    elif len(args) > 1:
        steps = list(args)
    elif len(args) == 1:
        single = args[0]
        if isinstance(single, dict):
            single = {k: v for k, v in single.items() if k not in _STEP_STRIP_KEYS}
        return as_actor_step(single)

    if steps is not None:
        return DescendActorConfig(pipeline=[as_actor_step(step) for step in steps])
    step_kwargs = {k: v for k, v in kwargs.items() if k not in _STEP_STRIP_KEYS}
    return validate_actor_step(normalize_actor_step(step_kwargs))
