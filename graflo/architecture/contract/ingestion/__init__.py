"""Declarative ingestion contract: resources, transforms, steps, and ingestion model.

Runtime execution (``ResourceRuntime``, ``build_resource_runtime``) lives in
:mod:`graflo.architecture.pipeline.runtime.resource`.
"""

from __future__ import annotations

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
    "Transform",
    "TransformException",
    "collect_vertex_names_from_pipeline",
    "resolve_type_caster",
]
