"""GSQL literal formatting for schema DDL (e.g. DEFAULT clauses)."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any


def gsql_default_literal(value: Any) -> str:
    """Format a manifest/JSON-friendly value as a GSQL literal for ``DEFAULT``."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:  # NaN
            raise ValueError("NaN is not a valid GSQL default literal")
        return json.dumps(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    raise TypeError(
        f"Unsupported type for GSQL DEFAULT literal: {type(value).__name__}. "
        "Use bool, int, float, Decimal, or str."
    )
