"""UUID identity helpers for assigned mode and UUID-typed natural keys."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from graflo.architecture.schema.vertex import (
    FieldType,
    Vertex,
    VertexConfig,
    field_type_value,
    is_list_field_type,
)

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _identity_value_is_empty(value: Any) -> bool:
    return value is None or value == ""


def validate_uuid_value(value: Any, *, context: str = "UUID") -> str:
    """Return a normalized UUID string; raise if *value* is non-empty but invalid."""
    if _identity_value_is_empty(value):
        raise ValueError(f"{context}: expected a UUID string, got empty value")
    text = str(value)
    if not UUID_PATTERN.match(text):
        raise ValueError(f"{context}: invalid UUID value {value!r}")
    return text


def ensure_assigned_uuid(doc: dict[str, Any], field: str) -> None:
    """Fill empty *field* with ``uuid4()``; validate non-empty values in place.

    Never overwrites a present valid UUID.
    """
    current = doc.get(field)
    if _identity_value_is_empty(current):
        doc[field] = str(uuid4())
        return
    doc[field] = validate_uuid_value(
        current, context=f"assigned identity field '{field}'"
    )


def ensure_assigned_uuids_in_acc_vertex(
    acc_vertex: Mapping[str, Any],
    vertex_config: VertexConfig,
) -> None:
    """Mint/validate assigned UUIDs on ``acc_vertex`` docs before edge assembly.

    ``acc_vertex`` maps vertex name -> location -> list of ``VertexRep`` (or
    objects with a ``.vertex`` dict attribute).
    """
    for vname in vertex_config.assigned_vertices:
        by_loc = acc_vertex.get(vname)
        if not by_loc:
            continue
        identity_fields = vertex_config.identity_fields(vname)
        preferred = identity_fields[0] if identity_fields else "id"
        for _lindex, reps in by_loc.items():
            for rep in reps:
                doc = rep.vertex if hasattr(rep, "vertex") else rep
                if isinstance(doc, dict):
                    ensure_assigned_uuid(doc, preferred)


def ensure_assigned_uuids_on_docs(
    data: list[dict[str, Any]],
    *,
    preferred_field: str,
    arango_key_mirror: bool = False,
) -> None:
    """Idempotent assigned-UUID ensure for a flat doc list (writer safety net)."""
    for doc in data:
        ensure_assigned_uuid(doc, preferred_field)
        if arango_key_mirror and "_key" not in doc:
            doc["_key"] = doc[preferred_field]


def validate_uuid_typed_identity_fields(
    doc: dict[str, Any],
    vertex: Vertex,
) -> None:
    """Validate present values on UUID-typed identity fields; do not invent."""
    prop_by_name = {f.name: f for f in vertex.properties}
    for name in vertex.identity:
        field = prop_by_name.get(name)
        if field is None or is_list_field_type(field.type):
            continue
        if field_type_value(field.type) != FieldType.UUID.value:
            continue
        current = doc.get(name)
        if _identity_value_is_empty(current):
            continue
        doc[name] = validate_uuid_value(
            current,
            context=f"vertex '{vertex.name}' identity field '{name}'",
        )
