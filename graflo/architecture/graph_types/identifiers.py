"""Graph identifier aliases and edge-key serialization."""

from __future__ import annotations

import json
from typing import TypeAlias

# Kept here to avoid importing schema, which creates cycles.
VertexName: TypeAlias = str

# Edge identifier layers:
# - EdgeId: schema-level edge definition key (source, target, relation)
EdgeId: TypeAlias = tuple[str, str, str | None]
# - EdgePhysicalKey: physical edge specification key (source, target, relation, purpose)
EdgePhysicalKey: TypeAlias = tuple[str, str, str | None, str | None]
GraphEntity: TypeAlias = str | EdgeId


def serialize_edge_key(edge_id: tuple[str, str, str | None]) -> str:
    """Serialize an edge id tuple to a JSON-safe string key.

    Uses a JSON array ``[source, target, relation]`` so vertex or relation names
    may contain ``|`` or other special characters without ambiguity.
    """
    source, target, relation = edge_id
    return json.dumps([source, target, relation], separators=(",", ":"))


def deserialize_edge_key(key: str) -> tuple[str, str, str | None]:
    """Deserialize a JSON-array edge key back to an edge id tuple."""
    try:
        parsed = json.loads(key)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid edge key JSON: {key!r}") from exc
    if not isinstance(parsed, list) or len(parsed) != 3:
        raise ValueError(f"Invalid edge key JSON array: {key!r}")
    if not all(isinstance(part, str) for part in parsed[:2]):
        raise ValueError(f"Invalid edge key JSON array: {key!r}")
    relation_part = parsed[2]
    if relation_part is not None and not isinstance(relation_part, str):
        raise ValueError(f"Invalid edge key JSON array: {key!r}")
    relation = relation_part if relation_part else None
    return (parsed[0], parsed[1], relation)


def serialize_entity_key(key: GraphEntity) -> str:
    if isinstance(key, str):
        return key
    return serialize_edge_key(key)


def deserialize_entity_key(key: str | tuple[str, str, str | None]) -> GraphEntity:
    if isinstance(key, tuple):
        return key
    if key.startswith("["):
        return deserialize_edge_key(key)
    return key
