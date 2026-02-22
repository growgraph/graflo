"""NebulaGraph utility functions.

Type mapping, filter rendering, value escaping, and schema-propagation helpers.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any

from graflo.architecture.vertex import FieldType
from graflo.db.nebula.adapter import NebulaClientAdapter
from graflo.filter.onto import FilterExpression
from graflo.onto import ExpressionFlavor

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
}

DEFAULT_NEBULA_TYPE = "string"


def nebula_type(ft: FieldType | None) -> str:
    """Map a graflo ``FieldType`` to the corresponding NebulaGraph type name."""
    if ft is None:
        return DEFAULT_NEBULA_TYPE
    return FIELD_TYPE_TO_NEBULA.get(ft, DEFAULT_NEBULA_TYPE)


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
