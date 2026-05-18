"""Declarative ingestion contract: resources, transforms, and ingestion model."""

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
from ..runtime.resource import (
    ResourceRuntime,
    build_resource_runtime,
    filter_vertex_config_for_resource,
    strip_trivial_top_level_fields,
)
from graflo.util.casting import resolve_type_caster

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
