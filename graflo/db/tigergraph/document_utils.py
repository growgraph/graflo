"""Document helpers for TigerGraph REST upsert and fetch operations."""

from __future__ import annotations

from typing import Any

from graflo.db.util import json_serializer

# Alias for backward compatibility
json_serializer_alias = json_serializer


def clean_document(doc: dict[str, Any]) -> dict[str, Any]:
    """
    Remove internal keys that shouldn't be stored in the database.

    Removes keys starting with "_" except "_key".
    """
    return {k: v for k, v in doc.items() if not k.startswith("_") or k == "_key"}


def extract_id(
    doc: dict[str, Any] | None,
    match_keys: list[str] | tuple[str, ...],
) -> str | None:
    """
    Extract vertex ID from document based on match keys.

    For composite keys, concatenates values with an underscore '_'.
    Prefers '_key' if present.
    """
    if not doc:
        return None

    if "_key" in doc and doc["_key"]:
        return str(doc["_key"])

    if len(match_keys) > 1:
        try:
            id_parts = [str(doc[key]) for key in match_keys]
            return "_".join(id_parts)
        except KeyError:
            return None

    if len(match_keys) == 1:
        key = match_keys[0]
        if key in doc and doc[key] is not None:
            return str(doc[key])

    return None
