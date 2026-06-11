"""Manifest evolution: apply high-level schema + ingestion transforms to :class:`~graflo.architecture.contract.manifest.GraphManifest`.

Use :func:`~graflo.migrate.io.manifest_hash` to compare contract identity before and after.
"""

from __future__ import annotations

from typing import Any

from .ops import (
    AddInverseEdgesOp,
    AddEdgePropertiesOp,
    AddVertexPropertiesOp,
    EdgeSelector,
    ManifestOp,
    MergeEdgesOp,
    MergeVerticesOp,
    ProjectManifestOp,
    RemoveEdgePropertiesOp,
    RemoveEdgesOp,
    RemoveVertexPropertiesOp,
    RemoveVerticesOp,
    RenameEdgePropertiesOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    SanitizeOp,
)

_APPLY_EXPORTS = frozenset(
    {
        "apply_evolution",
        "apply_add_edge_properties",
        "apply_add_inverse_edges",
        "apply_add_vertex_properties",
        "apply_merge_edges",
        "apply_merge_vertices",
        "apply_project_manifest",
        "apply_remove_edge_properties",
        "apply_remove_edges",
        "apply_remove_edge_ids",
        "apply_remove_vertex_properties",
        "apply_remove_vertices",
        "apply_rename_edge_properties",
        "apply_rename_relations",
        "apply_rename_resources",
        "apply_rename_vertex_properties",
        "apply_rename_vertices",
        "apply_sanitize",
    }
)

__all__ = [
    "AddEdgePropertiesOp",
    "AddInverseEdgesOp",
    "AddVertexPropertiesOp",
    "EdgeSelector",
    "ManifestOp",
    "MergeEdgesOp",
    "MergeVerticesOp",
    "ProjectManifestOp",
    "RemoveEdgePropertiesOp",
    "RemoveEdgesOp",
    "RemoveVertexPropertiesOp",
    "RemoveVerticesOp",
    "RenameEdgePropertiesOp",
    "RenameRelationsOp",
    "RenameResourcesOp",
    "RenameVertexPropertiesOp",
    "RenameVerticesOp",
    "SanitizeOp",
    "apply_evolution",
    "apply_add_edge_properties",
    "apply_add_inverse_edges",
    "apply_add_vertex_properties",
    "apply_merge_edges",
    "apply_merge_vertices",
    "apply_project_manifest",
    "apply_remove_edge_properties",
    "apply_remove_edges",
    "apply_remove_edge_ids",
    "apply_remove_vertex_properties",
    "apply_remove_vertices",
    "apply_rename_edge_properties",
    "apply_rename_relations",
    "apply_rename_resources",
    "apply_rename_vertex_properties",
    "apply_rename_vertices",
    "apply_sanitize",
]


def __getattr__(name: str) -> Any:
    if name in _APPLY_EXPORTS:
        from . import apply as apply_mod

        return getattr(apply_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
