"""Vertex id extraction aligned with TigerGraph REST/bulk loading."""

from __future__ import annotations

from typing import Any


def extract_vertex_id(
    doc: dict[str, Any], match_keys: tuple[str, ...] | list[str]
) -> str | None:
    """Build primary id string the same way as :meth:`TigerGraphConnection._extract_id`."""
    if not doc:
        return None
    keys = tuple(match_keys)
    if "_key" in doc and doc["_key"]:
        return str(doc["_key"])
    if len(keys) > 1:
        try:
            return "_".join(str(doc[key]) for key in keys)
        except KeyError:
            return None
    if len(keys) == 1:
        k = keys[0]
        if k in doc and doc[k] is not None:
            return str(doc[k])
    return None


def clean_document_for_staging(doc: dict[str, Any]) -> dict[str, Any]:
    """Strip internal keys (underscore-prefixed) except ``_key``."""
    return {k: v for k, v in doc.items() if not k.startswith("_") or k == "_key"}
