"""Content hashes over manifest blocks.

Lives at L4 rather than in ``migrate`` (L6) because the revision chain needs to
verify a replayed manifest, and ``architecture.evolution`` may not import
``migrate``. ``migrate.io`` re-exports these, which is where they have always
been imported from.

Every hash is SHA-256 over :func:`~graflo.architecture.evolution.canonicalize.canonical_payload`,
so two manifests that differ only in default-valued fields, key order, or the
order of an order-insignificant list hash equal. ``CANON_VERSION`` is mixed into
the hashed bytes: a future change to the canonicalization rules produces
different hashes by construction rather than reinterpreting old ones.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from graflo.architecture.evolution.canonicalize import CANON_VERSION, canonical_payload


def stable_hash(payload_obj: Any) -> str:
    """SHA-256 over a canonical JSON rendering of *payload_obj*.

    The canonicalization version is part of the hashed bytes, not a wrapper
    around them, so it cannot be stripped by a caller that re-serializes.
    """
    payload = json.dumps(
        {"canon": CANON_VERSION, "payload": payload_obj},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def graph_hash(schema: Any) -> str:
    """Stable hash over the logical graph model only."""
    return stable_hash(canonical_payload(schema.core_schema))


def schema_hash(schema: Any) -> str:
    """Stable hash over the schema deployment contract (graph + DB profile).

    ``metadata`` is deliberately excluded: name, semver and description are how
    a schema is *labelled*, not what it *is*, and a content address that moves
    when the version bumps cannot recognise that two versions carry identical
    content. :func:`manifest_hash` excludes it for the same reason.
    """
    payload = {
        "core_schema": canonical_payload(schema.core_schema),
        "db_profile": canonical_payload(schema.db_profile),
    }
    return stable_hash(payload)


def ingestion_hash(ingestion_model: Any) -> str:
    """Stable hash over the ingestion model (resources + transforms)."""
    return stable_hash(canonical_payload(ingestion_model))


def full_hash(schema: Any, ingestion_model: Any, bindings: Any) -> str:
    """Stable hash over a composed deployment object."""
    payload = {
        "schema": _schema_payload(schema),
        "ingestion": canonical_payload(ingestion_model),
        "bindings": (
            canonical_payload(bindings)
            if hasattr(bindings, "to_minimal_canonical_dict")
            else (bindings.to_dict() if hasattr(bindings, "to_dict") else bindings)
        ),
    }
    return stable_hash(payload)


def _schema_payload(schema: Any) -> dict[str, Any] | None:
    """Canonical payload of a ``Schema``, minus its metadata block.

    Mirrors :func:`schema_hash`, so a manifest's hash covers its schema's hash
    by construction (Merkle-style) and no redundant schema hash has to be
    stored alongside it.
    """
    if schema is None:
        return None
    return {
        "core_schema": canonical_payload(schema.core_schema),
        "db_profile": canonical_payload(schema.db_profile),
    }


def manifest_hash(manifest: Any) -> str:
    """Stable hash over all three manifest blocks.

    This is the identity a revision chain verifies against: replaying a chain
    from its base must reproduce the recorded hash at every step.
    """
    payload = {
        "schema": _schema_payload(manifest.graph_schema),
        "ingestion_model": (
            canonical_payload(manifest.ingestion_model)
            if manifest.ingestion_model is not None
            else None
        ),
        "bindings": (
            canonical_payload(manifest.bindings)
            if manifest.bindings is not None
            else None
        ),
    }
    return stable_hash(payload)
