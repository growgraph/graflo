"""NebulaGraph utility functions.

Type mapping, filter rendering, value escaping, and schema-propagation helpers.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal
from typing import Any

from graflo.architecture.schema.vertex import Field, FieldType, is_list_field_type
from graflo.db.field_type_support import (
    UnsupportedFieldTypeError,
    assert_field_type_supported,
)
from graflo.db.nebula.adapter import NebulaClientAdapter
from graflo.filter.onto import FilterExpression
from graflo.onto import DBType, ExpressionFlavor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FieldType -> NebulaGraph type string
# ---------------------------------------------------------------------------

FIELD_TYPE_TO_NEBULA: dict[FieldType, str] = {
    FieldType.INT: "int64",
    FieldType.UINT: "int64",
    FieldType.FLOAT: "float",
    FieldType.DOUBLE: "double",
    FieldType.BOOL: "bool",
    FieldType.STRING: "string",
    FieldType.DATETIME: "string",
    FieldType.UUID: "string",
}

DEFAULT_NEBULA_TYPE = "string"

#: NebulaGraph DDL type -> graflo ``FieldType``, for reading a catalogue back.
#: Not the inverse of :data:`FIELD_TYPE_TO_NEBULA`: that map is lossy in the
#: outbound direction (``UINT``, ``DATETIME`` and ``UUID`` all land on a Nebula
#: type shared with something else), so a recovered ``string`` column is
#: reported as ``STRING`` and any narrower intent has to be re-declared by the
#: modeller rather than guessed at here.
NEBULA_TYPE_TO_FIELD_TYPE: dict[str, FieldType] = {
    "int": FieldType.INT,
    "int8": FieldType.INT,
    "int16": FieldType.INT,
    "int32": FieldType.INT,
    "int64": FieldType.INT,
    "float": FieldType.FLOAT,
    "double": FieldType.DOUBLE,
    "bool": FieldType.BOOL,
    "string": FieldType.STRING,
    "date": FieldType.DATETIME,
    "time": FieldType.DATETIME,
    "datetime": FieldType.DATETIME,
    "timestamp": FieldType.DATETIME,
}


def field_type_from_nebula(declared: str | None) -> FieldType | None:
    """Map a ``DESCRIBE TAG`` / ``DESCRIBE EDGE`` type back to a ``FieldType``.

    Returns ``None`` for a type this mapping does not cover, so the caller can
    leave the field untyped rather than assert a wrong one.
    """
    if not declared:
        return None
    # `fixed_string(32)` and friends carry a length the graflo model does not model.
    base = declared.strip().lower().split("(", 1)[0].strip()
    if base == "fixed_string":
        return FieldType.STRING
    return NEBULA_TYPE_TO_FIELD_TYPE.get(base)


def nebula_type(ft: FieldType | None) -> str:
    """Map a scalar graflo ``FieldType`` to the corresponding NebulaGraph type name.

    ``LIST`` is not storable as a Nebula property — use :func:`nebula_type_for_field`.
    """
    if ft is None:
        return DEFAULT_NEBULA_TYPE
    if is_list_field_type(ft):
        raise UnsupportedFieldTypeError(
            "LIST cannot be stored as a NebulaGraph vertex/edge property "
            "(composite types are query-only). No soft conversion to string/JSON."
        )
    return FIELD_TYPE_TO_NEBULA.get(ft, DEFAULT_NEBULA_TYPE)


def nebula_type_for_field(field: Field) -> str:
    """Map a ``Field`` to a NebulaGraph DDL type, raising for unsupported types."""
    assert_field_type_supported(DBType.NEBULA, field)
    return nebula_type(field.type)


def is_nebula_string_field(field: Field) -> bool:
    """Whether *field* becomes a variable-length ``string`` column.

    Index DDL must ask the same question the column DDL answered: Nebula
    rejects ``CREATE TAG INDEX`` on a string column given without a length.
    Testing ``field.type == FieldType.STRING`` is not that question — an
    untyped field, a ``DATETIME`` and a ``UUID`` all land on ``string`` too,
    and an index over any of them is rejected as ``Invalid param!``.
    """
    return nebula_type(field.type) == DEFAULT_NEBULA_TYPE


# ---------------------------------------------------------------------------
# Value serialisation
# ---------------------------------------------------------------------------


def escape_nebula_string(value: str) -> str:
    """Escape a string value for safe embedding in nGQL / GQL literals."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def serialize_nebula_value(value: Any) -> str:
    """Serialise a Python value into an nGQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return str(float(value))
    if isinstance(value, datetime):
        return f'"{value.isoformat()}"'
    if isinstance(value, date):
        return f'"{value.isoformat()}"'
    if isinstance(value, dt_time):
        return f'"{value.isoformat()}"'
    if isinstance(value, (list, dict)):
        return f'"{escape_nebula_string(json.dumps(value, default=str))}"'
    return f'"{escape_nebula_string(str(value))}"'


# ---------------------------------------------------------------------------
# VID helpers
# ---------------------------------------------------------------------------


def make_vid(doc: dict[str, Any], match_keys: list[str] | tuple[str, ...]) -> str:
    """Derive a VID string from a document's match-key values.

    When a single match key is used the raw value is taken.  When multiple keys
    are present the values are joined with ``::`` so the VID is deterministic
    and unique for the combination.
    """
    parts = [str(doc.get(k, "")) for k in match_keys]
    return "::".join(parts)


# ---------------------------------------------------------------------------
# Filter rendering
# ---------------------------------------------------------------------------


def render_filters_ngql(
    filters: list | dict | FilterExpression | None,
    doc_name: str,
) -> str:
    """Render a ``FilterExpression`` as an nGQL ``WHERE`` clause (without the keyword)."""
    if filters is None:
        return ""
    if not isinstance(filters, FilterExpression):
        ff = FilterExpression.from_dict(filters)
    else:
        ff = filters
    return str(ff(doc_name=doc_name, kind=ExpressionFlavor.NGQL))


def render_filters_cypher(
    filters: list | dict | FilterExpression | None,
    doc_name: str,
) -> str:
    """Render a ``FilterExpression`` as a Cypher ``WHERE`` clause (without the keyword)."""
    if filters is None:
        return ""
    if not isinstance(filters, FilterExpression):
        ff = FilterExpression.from_dict(filters)
    else:
        ff = filters
    return str(ff(doc_name=doc_name, kind=ExpressionFlavor.CYPHER))


# ---------------------------------------------------------------------------
# Schema propagation wait
# ---------------------------------------------------------------------------


def wait_for_schema_propagation(
    adapter: NebulaClientAdapter,
    check_statement: str,
    *,
    max_retries: int = 30,
    interval: float = 1.0,
) -> None:
    """Poll *check_statement* until it succeeds or retries are exhausted.

    NebulaGraph propagates schema changes asynchronously across the cluster.
    After ``CREATE SPACE`` / ``CREATE TAG`` / ``CREATE EDGE``, subsequent
    statements may fail until propagation completes (typically within two
    heartbeat cycles, ~20 s for default settings).
    """
    for attempt in range(max_retries):
        try:
            adapter.execute(check_statement)
            return
        except Exception:
            if attempt == max_retries - 1:
                raise
            logger.debug(
                "Schema not yet propagated (attempt %d/%d), retrying in %.1fs …",
                attempt + 1,
                max_retries,
                interval,
            )
            time.sleep(interval)


def wait_for_space_ready(
    adapter: NebulaClientAdapter,
    space_name: str,
    *,
    max_retries: int = 30,
    interval: float = 1.0,
) -> None:
    """Wait until ``USE `space_name``` succeeds."""
    wait_for_schema_propagation(
        adapter,
        f"USE `{space_name}`",
        max_retries=max_retries,
        interval=interval,
    )
