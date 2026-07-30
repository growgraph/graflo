"""I/O utilities for migration workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from suthing import FileHandle

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.hashing import (
    full_hash,
    graph_hash,
    ingestion_hash,
    manifest_hash,
    schema_hash,
    stable_hash,
)
from graflo.architecture.schema import Schema

__all__ = [
    "full_hash",
    "graph_hash",
    "ingestion_hash",
    "load_ingestion_model",
    "load_manifest",
    "load_schema",
    "manifest_hash",
    "plan_to_json_serializable",
    "schema_hash",
    "stable_hash",
]


def load_manifest(path: str | Path) -> GraphManifest:
    """Load and initialize graph manifest from YAML path."""
    manifest = GraphManifest.from_config(FileHandle.load(path))
    manifest.finish_init()
    return manifest


def load_schema(path: str | Path) -> Schema:
    """Load schema block from a manifest path."""
    return load_manifest(path).require_schema()


def load_ingestion_model(
    path: str | Path, schema: Schema | None = None
) -> IngestionModel:
    """Load ingestion block from a manifest path."""
    manifest = load_manifest(path)
    ingestion_model = manifest.require_ingestion_model()
    if schema is not None:
        ingestion_model.finish_init(
            schema.core_schema,
            target_db_flavor=schema.db_profile.db_flavor,
        )
    return ingestion_model


# Hashing moved down to ``architecture.evolution`` (L4): the revision chain
# verifies replayed manifests and cannot import ``migrate`` (L6). Re-exported
# here, where these have always been imported from.
_stable_hash = stable_hash


def plan_to_json_serializable(plan: Any) -> dict[str, Any]:
    """Convert pydantic plan-like object to JSON payload."""
    if hasattr(plan, "model_dump"):
        return plan.model_dump()
    if hasattr(plan, "to_dict"):
        return plan.to_dict()
    raise TypeError(f"Unsupported plan object type: {type(plan)}")
