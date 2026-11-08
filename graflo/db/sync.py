"""The protocol a backend implements to sync owner records into a loaded target.

The caller folds the whole sync input first and groups it by owner. The order
of calls is: ``sync_begin``, every vertex chunk (``sync_ensure_vertices``),
then every owner chunk (``sync_write_owners``). Rows are in the names the
target stores. Every call is safe to repeat: a chunk whose commit was not
recorded is simply written again.

An owner chunk is written as one unit: for each owner whose stored sync hash
differs from the one it carries, its properties are replaced, the edges it
owns are deleted and its edges created, and its hash is stored last.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class SyncEdgeType(BaseModel):
    """One edge type an owner type owns, as stored, and where its owner is read."""

    source_class: str
    target_class: str
    relation_name: str
    match_keys_source: list[str]
    match_keys_target: list[str]
    side: Literal["source", "target", "field"]
    """The endpoint that is the owner, or ``field``: the owner is ``owner_property``."""
    owner_property: str | None = None
    """For ``side == "field"``: the edge property holding the owner's identity value."""


class SyncOwner(BaseModel):
    """One owner record: its vertex and, per edge type, the edges it owns."""

    doc: dict[str, Any]
    """The vertex as stored, its sync hash included."""
    edges: list[list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]]
    """``edges[i]``: ``(source keys, target keys, properties)`` of edge type ``i``."""


class SyncOwnerChunk(BaseModel):
    """Owners of one vertex type, with every edge type that type owns."""

    chunk_id: str
    class_name: str
    match_keys: list[str]
    hash_field: str
    edge_types: list[SyncEdgeType]
    owners: list[SyncOwner]


class SyncWritten(BaseModel):
    """What an owner chunk did: owners rewritten, and owners left as they were."""

    written: int = 0
    skipped: int = 0
