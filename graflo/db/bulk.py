"""The protocol a backend implements to load an empty target in bulk.

The caller folds the whole input first, so every vertex and every edge arrives
exactly once and a writer never looks for an existing element. Rows are in the
names the target stores. The order of calls is: ``bulk_begin``, every vertex
chunk, ``bulk_prepare_edges``, every edge chunk, ``bulk_finish``. Chunks of one
phase may be written concurrently, each through its own connection, so a
writer keeps no state between calls beyond what the target holds. A writer
that declares ``bulk_partition_edges`` gets its edges in rounds: the chunks
written at once share no endpoint vertex.

A chunk with ``replay`` set may already be in the target (its commit was not
recorded); the writer must make writing it again harmless.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

#: ``(source class, relation name, target class)`` as the target stores them.
StoredEdgeId = tuple[str, str | None, str]


class BulkPlan(BaseModel):
    """What a writer asks of the load once it has begun."""

    model_config = ConfigDict(frozen=True)

    max_parallel: int | None = None
    """Chunks the target takes at once; the configured parallelism when ``None``."""
    restart: bool = False
    """On resume: the target lost every chunk written before, so all are written again."""


class BulkVertexChunk(BaseModel):
    """Folded vertices of one type."""

    chunk_id: str
    class_name: str
    match_keys: list[str]
    rows: list[dict[str, Any]]
    dense: list[int] | None = None
    """The rows' dense ids, when the writer declares ``bulk_dense_ids``."""
    replay: bool = False


class BulkEdgeChunk(BaseModel):
    """Folded edges of one concrete relation, endpoints keyed on their identity."""

    chunk_id: str
    source_class: str
    target_class: str
    relation_name: str | None
    match_keys_source: list[str]
    match_keys_target: list[str]
    collection_name: str | None
    merge_properties: list[str] = []
    """Edge properties that, with the endpoints, tell parallel edges apart."""
    rows: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]
    dense: list[tuple[int, int]] | None = None
    """``(source, target)`` dense ids, when the writer declares ``bulk_dense_ids``."""
    replay: bool = False

    @property
    def stored_id(self) -> StoredEdgeId:
        return (self.source_class, self.relation_name, self.target_class)


class BulkCounts(BaseModel):
    """Elements the target holds after a load, per stored type."""

    vertices: dict[str, int]
    edges: dict[StoredEdgeId, int]
