"""I/O utilities for migration workflows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from suthing import FileHandle

from graflo.architecture.schema import IngestionModel, Schema


def load_schema(path: str | Path) -> Schema:
    """Load and initialize schema from YAML path."""
    schema_raw = FileHandle.load(path)
    schema = Schema.from_config(schema_raw)
    return schema


def load_ingestion_model(path: str | Path, schema: Schema) -> IngestionModel:
    """Load and initialize ingestion model from YAML path."""
    schema_raw = FileHandle.load(path)
    ingestion = IngestionModel.from_config(schema_raw)
    schema.bind_ingestion_model(ingestion)
    return ingestion


def _stable_hash(payload_obj: Any) -> str:
    payload = json.dumps(payload_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def graph_hash(schema: Schema) -> str:
    """Stable hash over logical graph model only."""
    return _stable_hash(schema.graph.to_dict())


def schema_hash(schema: Schema) -> str:
    """Stable hash over schema deployment contract (graph + DB profile)."""
    payload = {
        "graph": schema.graph.to_dict(),
        "db_profile": schema.db_profile.to_dict(),
    }
    return _stable_hash(payload)


def ingestion_hash(ingestion_model: IngestionModel) -> str:
    """Stable hash over ingestion model (resources + transforms)."""
    return _stable_hash(ingestion_model.to_dict())


def full_hash(schema: Schema, ingestion_model: IngestionModel, bindings: Any) -> str:
    """Stable hash over composed deployment object."""
    payload = {
        "schema": schema.to_dict(),
        "ingestion": ingestion_model.to_dict(),
        "bindings": bindings.to_dict() if hasattr(bindings, "to_dict") else bindings,
    }
    return _stable_hash(payload)


def plan_to_json_serializable(plan: Any) -> dict[str, Any]:
    """Convert pydantic plan-like object to JSON payload."""
    if hasattr(plan, "model_dump"):
        return plan.model_dump()
    if hasattr(plan, "to_dict"):
        return plan.to_dict()
    raise TypeError(f"Unsupported plan object type: {type(plan)}")
