"""Resource contract and runtime execution.

Declarative configuration lives in :class:`ResourceConfig`.
Schema-bound execution uses :class:`ResourceRuntime`.
"""

from __future__ import annotations

from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig
from graflo.onto import DBType
from graflo.util.casting import resolve_type_caster

from .resource_config import (
    EdgeInferSpec,
    ResourceConfig,
    ResourceExtraWeightEntry,
    collect_vertex_names_from_pipeline,
)
from .resource_runtime import (
    ResourceRuntime,
    filter_vertex_config_for_resource,
    strip_trivial_top_level_fields,
)
from .transform import ProtoTransform

# Internal-only alias; prefer ResourceConfig / ResourceRuntime in new code.
Resource = ResourceConfig

_collect_vertex_names_from_pipeline = collect_vertex_names_from_pipeline
_strip_trivial_top_level_fields = strip_trivial_top_level_fields
_filter_vertex_config_for_resource = filter_vertex_config_for_resource
_resolve_type_caster = resolve_type_caster


def build_resource_runtime(
    config: ResourceConfig,
    vertex_config: VertexConfig,
    edge_config: EdgeConfig,
    transforms: dict[str, ProtoTransform] | None = None,
    *,
    strict_references: bool = False,
    dynamic_edge_feedback: bool = False,
    allowed_vertex_names: set[str] | None = None,
    target_db_flavor: DBType | None = None,
) -> ResourceRuntime:
    """Construct a fully initialized :class:`ResourceRuntime` from declarative config."""
    return ResourceRuntime(
        config,
        vertex_config,
        edge_config,
        transforms or {},
        strict_references=strict_references,
        dynamic_edge_feedback=dynamic_edge_feedback,
        allowed_vertex_names=allowed_vertex_names,
        target_db_flavor=target_db_flavor,
    )


__all__ = [
    "EdgeInferSpec",
    "Resource",
    "ResourceConfig",
    "ResourceExtraWeightEntry",
    "ResourceRuntime",
    "build_resource_runtime",
    "collect_vertex_names_from_pipeline",
    "filter_vertex_config_for_resource",
    "resolve_type_caster",
    "strip_trivial_top_level_fields",
]
