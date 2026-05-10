"""Manifest evolution: apply high-level schema + ingestion transforms to :class:`~graflo.architecture.contract.manifest.GraphManifest`.

Use :func:`~graflo.migrate.io.manifest_hash` to compare contract identity before and after.
"""

from __future__ import annotations

from typing import Any

from .ops import (
    ManifestOp,
    MergeVerticesOp,
    RemoveVerticesOp,
    RenameVertexFieldsOp,
    SanitizeOp,
)

_APPLY_EXPORTS = frozenset(
    {
        "apply_evolution",
        "apply_merge_vertices",
        "apply_remove_vertices",
        "apply_rename_vertex_fields",
        "apply_sanitize",
    }
)

__all__ = [
    "ManifestOp",
    "MergeVerticesOp",
    "RemoveVerticesOp",
    "RenameVertexFieldsOp",
    "SanitizeOp",
    "apply_evolution",
    "apply_merge_vertices",
    "apply_remove_vertices",
    "apply_rename_vertex_fields",
    "apply_sanitize",
]


def __getattr__(name: str) -> Any:
    if name in _APPLY_EXPORTS:
        from . import apply as apply_mod

        return getattr(apply_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
