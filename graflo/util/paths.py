"""Dot paths into JSON-like documents.

One syntax for every place a manifest names a nested value: a connector's
``records_path``, ``cursor_path`` and watermark ``field``, a vertex step's
``from`` and an edge step's ``relation_field``. Segments are separated by
``.``; a segment indexes a list when it is an integer.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast


def get_at_path(obj: object, path: str | None) -> object | None:
    """Resolve a dot-separated path against a JSON-like object.

    A missing segment, an out-of-range index or a ``None`` along the way
    yields ``None``.
    """
    if path is None:
        return None

    data: object = obj
    for part in path.split("."):
        if isinstance(data, Mapping):
            mapping = cast(Mapping[str, Any], data)
            data = mapping.get(part)
        elif isinstance(data, list):
            try:
                data = data[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if data is None:
            return None
    return data


def lookup_field(doc: Mapping[str, Any], key: str) -> tuple[bool, Any]:
    """Read *key* from *doc*: the key itself, else the dot path it spells.

    A key present in *doc* wins, so a field whose name contains a dot reads as
    it always has. Otherwise a dotted *key* is followed as a path; a path that
    resolves to ``None`` counts as absent.

    Returns:
        ``(found, value)``.
    """
    if key in doc:
        return True, doc[key]
    if "." in key:
        value = get_at_path(doc, key)
        if value is not None:
            return True, value
    return False, None
