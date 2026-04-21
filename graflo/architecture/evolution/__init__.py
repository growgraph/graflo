"""Manifest evolution: apply high-level schema + ingestion transforms to :class:`~graflo.architecture.contract.manifest.GraphManifest`.

Use :func:`~graflo.migrate.io.manifest_hash` to compare contract identity before and after.
"""

from __future__ import annotations

from .apply import apply_evolution, apply_merge_vertices, apply_remove_vertices
from .ops import ManifestOp, MergeVerticesOp, RemoveVerticesOp

__all__ = [
    "ManifestOp",
    "MergeVerticesOp",
    "RemoveVerticesOp",
    "apply_evolution",
    "apply_merge_vertices",
    "apply_remove_vertices",
]
