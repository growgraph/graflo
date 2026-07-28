"""Declarative ingestion contract: resources, transforms, and ingestion model."""

from __future__ import annotations

from typing import Any

from graflo.util.casting import resolve_type_caster

from .model import IngestionModel
from .resource import (
    EdgeInferSpec,
    Resource,
    ResourceConfig,
    ResourceExtraWeightEntry,
    collect_vertex_names_from_pipeline,
)
from .transform import (
    DressConfig,
    KeySelectionConfig,
    ProtoTransform,
    Transform,
    TransformException,
)

__all__ = [
    "DressConfig",
    "EdgeInferSpec",
    "IngestionModel",
    "KeySelectionConfig",
    "ProtoTransform",
    "Resource",
    "ResourceConfig",
    "ResourceExtraWeightEntry",
    "ResourceRuntime",
    "Transform",
    "TransformException",
    "build_resource_runtime",
    "collect_vertex_names_from_pipeline",
    "filter_vertex_config_for_resource",
    "resolve_type_caster",
    "strip_trivial_top_level_fields",
]

_RUNTIME_EXPORTS = frozenset(
    {
        "ResourceRuntime",
        "build_resource_runtime",
        "filter_vertex_config_for_resource",
        "strip_trivial_top_level_fields",
    }
)


def __getattr__(name: str) -> Any:
    if name in _RUNTIME_EXPORTS:
        from ..runtime.resource import (
            ResourceRuntime,
            build_resource_runtime,
            filter_vertex_config_for_resource,
            strip_trivial_top_level_fields,
        )

        globals().update(
            {
                "ResourceRuntime": ResourceRuntime,
                "build_resource_runtime": build_resource_runtime,
                "filter_vertex_config_for_resource": filter_vertex_config_for_resource,
                "strip_trivial_top_level_fields": strip_trivial_top_level_fields,
            }
        )
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
