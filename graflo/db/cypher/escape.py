"""Cypher string and identifier escaping for safe query fragments."""

from __future__ import annotations


def cypher_string_literal(value: str) -> str:
    """Return a single-quoted Cypher string literal with escapes."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def cypher_map_key(name: str) -> str:
    """Return a backtick-quoted map-key / property name for Cypher patterns.

    Strips embedded backticks from *name* so callers cannot break out of quotes.
    """
    key = name.strip().replace("`", "")
    if not key:
        raise ValueError("Cypher property name must be non-empty")
    return f"`{key}`"
