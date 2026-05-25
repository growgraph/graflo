"""Safe document field type casting for ingestion resources."""

from __future__ import annotations

import builtins
from typing import Any, Callable

SAFE_TYPE_CASTERS: dict[str, Callable[..., Any]] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "bytes": bytes,
    "list": list,
    "dict": dict,
    "tuple": tuple,
    "set": set,
}


def resolve_type_caster(name: str) -> Callable[..., Any] | None:
    """Resolve a type caster by name from a strict allowlist."""
    if not isinstance(name, str):
        return None
    candidate = SAFE_TYPE_CASTERS.get(name)
    if candidate is not None:
        return candidate
    if "." in name:
        module_name, attr_name = name.split(".", 1)
        if module_name == "builtins":
            builtin_attr = getattr(builtins, attr_name, None)
            if callable(builtin_attr) and attr_name in SAFE_TYPE_CASTERS:
                return SAFE_TYPE_CASTERS[attr_name]
    return None


def resolve_type_casters(
    types: dict[str, str],
) -> dict[str, Callable[..., Any]]:
    """Resolve declared field types to callables, skipping unknown names."""
    resolved: dict[str, Callable[..., Any]] = {}
    for field_name, type_name in types.items():
        caster = resolve_type_caster(type_name)
        if caster is not None:
            resolved[field_name] = caster
    return resolved


def apply_type_casters(
    doc: dict[str, Any], casters: dict[str, Callable[..., Any]]
) -> dict[str, Any]:
    """Apply configured type casters to top-level document fields in place."""
    if not casters:
        return doc
    for field_name, caster in casters.items():
        if field_name in doc:
            doc[field_name] = caster(doc[field_name])
    return doc
