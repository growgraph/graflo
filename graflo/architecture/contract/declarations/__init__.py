"""Ingestion declarations: resources, transforms, and ingestion model."""

from .ingestion_model import IngestionModel
from .resource import (
    Resource,
    ResourceConfig,
    ResourceRuntime,
    build_resource_runtime,
)
from .resource_config import EdgeInferSpec
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
    "ResourceRuntime",
    "Transform",
    "TransformException",
    "build_resource_runtime",
]
