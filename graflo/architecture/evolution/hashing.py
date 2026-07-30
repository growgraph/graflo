"""Content hashes over manifest blocks.

Lives at L4 rather than in ``migrate`` (L6) because the revision chain needs to
verify a replayed manifest, and ``architecture.evolution`` may not import
``migrate``. ``migrate.io`` re-exports these, which is where they have always
been imported from.

Every hash is SHA-256 over the model's ``to_minimal_canonical_dict()``, so two
manifests that differ only in default-valued fields or key order hash equal.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def stable_hash(payload_obj: Any) -> str:
    """SHA-256 over a canonical JSON rendering of *payload_obj*."""
    payload = json.dumps(payload_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def graph_hash(schema: Any) -> str:
    """Stable hash over the logical graph model only."""
    return stable_hash(schema.core_schema.to_minimal_canonical_dict())


def schema_hash(schema: Any) -> str:
    """Stable hash over the schema deployment contract (graph + DB profile)."""
    payload = {
        "core_schema": schema.core_schema.to_minimal_canonical_dict(),
        "db_profile": schema.db_profile.to_minimal_canonical_dict(),
    }
    return stable_hash(payload)


def ingestion_hash(ingestion_model: Any) -> str:
    """Stable hash over the ingestion model (resources + transforms)."""
    return stable_hash(ingestion_model.to_minimal_canonical_dict())


def full_hash(schema: Any, ingestion_model: Any, bindings: Any) -> str:
    """Stable hash over a composed deployment object."""
    payload = {
        "schema": schema.to_minimal_canonical_dict(),
        "ingestion": ingestion_model.to_minimal_canonical_dict(),
        "bindings": (
            bindings.to_minimal_canonical_dict()
            if hasattr(bindings, "to_minimal_canonical_dict")
            else (bindings.to_dict() if hasattr(bindings, "to_dict") else bindings)
        ),
    }
    return stable_hash(payload)


def manifest_hash(manifest: Any) -> str:
    """Stable hash over all three manifest blocks.

    This is the identity a revision chain verifies against: replaying a chain
    from its base must reproduce the recorded hash at every step.
    """
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
    return stable_hash(payload)
