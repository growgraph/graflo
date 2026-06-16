"""Manifest model for on-disk GraFlo backend directories."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.document import Schema


def backend_schema_hash(schema: Schema) -> str:
    """Stable hash over schema deployment contract (graph + DB profile)."""
    payload = {
        "core_schema": schema.core_schema.to_minimal_canonical_dict(),
        "db_profile": schema.db_profile.to_minimal_canonical_dict(),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class CollectionEntry(ConfigBaseModel):
    """Inventory for one vertex type or edge collection on disk."""

    chunks: list[str] = Field(
        default_factory=list,
        description="Relative paths to gzip JSONL chunk files.",
    )
    record_count: int = Field(default=0, ge=0)


class GraFloIndex(ConfigBaseModel):
    """Self-describing manifest for a GraFlo file backend directory."""

    graflo_version: str = Field(
        ..., description="GraFlo package version at export time."
    )
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="UTC timestamp when the export was finalized.",
    )
    schema_hash: str = Field(
        ..., description="Stable hash of the stored schema document."
    )
    vertices: dict[str, CollectionEntry] = Field(default_factory=dict)
    edges: dict[str, CollectionEntry] = Field(default_factory=dict)
