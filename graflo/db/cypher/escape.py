"""Cypher string and identifier escaping for safe query fragments."""

from __future__ import annotations

import re
from collections.abc import Sequence

#: A name Cypher reads as one token without quoting.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

#: openCypher reserved words (compared case-insensitively): quoted even though
#: they are identifiers, since stricter parsers refuse them as bare names.
_RESERVED = frozenset(
    [
        "ADD",
        "ALL",
        "AND",
        "AS",
        "ASC",
        "ASCENDING",
        "BY",
        "CALL",
        "CASE",
        "CONSTRAINT",
        "CONTAINS",
        "CREATE",
        "DELETE",
        "DESC",
        "DESCENDING",
        "DETACH",
        "DISTINCT",
        "DO",
        "DROP",
        "ELSE",
        "END",
        "ENDS",
        "EXISTS",
        "FALSE",
        "FOR",
        "IN",
        "IS",
        "LIMIT",
        "MANDATORY",
        "MATCH",
        "MERGE",
        "NOT",
        "NULL",
        "OF",
        "ON",
        "OPTIONAL",
        "OR",
        "ORDER",
        "REMOVE",
        "REQUIRE",
        "RETURN",
        "SCALAR",
        "SET",
        "SKIP",
        "STARTS",
        "THEN",
        "TRUE",
        "UNION",
        "UNIQUE",
        "UNWIND",
        "WHEN",
        "WHERE",
        "WITH",
        "XOR",
        "YIELD",
    ]
)


def cypher_name(name: str, *, quoted: bool = False) -> str:
    """A label, relationship type, property key or alias as one Cypher token.

    An identifier that is not a reserved word is written as is unless
    *quoted*; any other name is backtick-quoted with embedded backticks doubled,
    so a name taken from a schema can never end the token and add query text of
    its own.

    Raises:
        ValueError: if *name* is empty.
    """
    if not name:
        raise ValueError("a Cypher name must be non-empty")
    if not quoted and _IDENTIFIER.fullmatch(name) and name.upper() not in _RESERVED:
        return name
    return "`" + name.replace("`", "``") + "`"


def cypher_names(names: str | Sequence[str]) -> str:
    """One relationship type, or several joined with ``|``, each one token."""
    if isinstance(names, str):
        return cypher_name(names)
    return "|".join(cypher_name(name) for name in names)


def cypher_string_literal(value: str) -> str:
    """Return a single-quoted Cypher string literal with escapes."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def cypher_map_key(name: str) -> str:
    """Return a backtick-quoted map-key / property name for Cypher patterns.

    Surrounding whitespace is dropped first; embedded backticks are doubled,
    so two names differing only by a backtick stay two keys.
    """
    key = name.strip()
    if not key:
        raise ValueError("Cypher property name must be non-empty")
    return cypher_name(key, quoted=True)
