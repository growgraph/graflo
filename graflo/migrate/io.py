"""I/O utilities for migration workflows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.schema import Schema


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


def _stable_hash(payload_obj: Any) -> str:
    payload = json.dumps(payload_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def graph_hash(schema: Schema) -> str:
    """Stable hash over logical graph model only."""
    return _stable_hash(schema.core_schema.to_minimal_canonical_dict())


def schema_hash(schema: Schema) -> str:
    """Stable hash over schema deployment contract (graph + DB profile)."""
    payload = {
        "core_schema": schema.core_schema.to_minimal_canonical_dict(),
        "db_profile": schema.db_profile.to_minimal_canonical_dict(),
    }
    return _stable_hash(payload)


def ingestion_hash(ingestion_model: IngestionModel) -> str:
    """Stable hash over ingestion model (resources + transforms)."""
    return _stable_hash(ingestion_model.to_minimal_canonical_dict())


def full_hash(schema: Schema, ingestion_model: IngestionModel, bindings: Any) -> str:
    """Stable hash over composed deployment object."""
    payload = {
        "schema": schema.to_minimal_canonical_dict(),
        "ingestion": ingestion_model.to_minimal_canonical_dict(),
        "bindings": (
            bindings.to_minimal_canonical_dict()
            if hasattr(bindings, "to_minimal_canonical_dict")
            else (bindings.to_dict() if hasattr(bindings, "to_dict") else bindings)
        ),
    }
    return _stable_hash(payload)


def manifest_hash(manifest: GraphManifest) -> str:
    """Stable hash over manifest blocks."""
    payload = {
        "schema": manifest.graph_schema.to_minimal_canonical_dict()
        if manifest.graph_schema is not None
        else None,
        "ingestion_model": (
            manifest.ingestion_model.to_minimal_canonical_dict()
            if manifest.ingestion_model is not None
            else None
        ),
        "bindings": manifest.bindings.to_minimal_canonical_dict()
        if manifest.bindings is not None
        else None,
    }
    return _stable_hash(payload)


def plan_to_json_serializable(plan: Any) -> dict[str, Any]:
    """Convert pydantic plan-like object to JSON payload."""
    if hasattr(plan, "model_dump"):
        return plan.model_dump()
    if hasattr(plan, "to_dict"):
        return plan.to_dict()
    raise TypeError(f"Unsupported plan object type: {type(plan)}")
