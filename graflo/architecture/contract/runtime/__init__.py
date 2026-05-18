"""Schema-bound runtime executors (non-serializable)."""

from __future__ import annotations

from typing import Any

from .edge_derivation import EdgeDerivationRegistry

__all__ = [
    "EdgeDerivationRegistry",
    "ResourceRuntime",
    "build_resource_runtime",
    "filter_vertex_config_for_resource",
    "strip_trivial_top_level_fields",
]


def __getattr__(name: str) -> Any:
    if name in {
        "ResourceRuntime",
        "build_resource_runtime",
        "filter_vertex_config_for_resource",
        "strip_trivial_top_level_fields",
    }:
        from . import resource as _resource

        return getattr(_resource, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
