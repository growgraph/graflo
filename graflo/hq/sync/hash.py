"""The sync hash: a digest of what one owner record wrote.

It covers the owner vertex's folded properties, except the fields set by sync,
and every edge the owner owns: the edge's type, both endpoint keys and its
properties. Two runs that fold the same records compute the same hash, so a
sync can skip a record a bulk load or an earlier sync already wrote.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Iterable, Mapping
from typing import Any

import suthing

from graflo.util.typed_values import encode


def sync_hash(
    doc: Mapping[str, Any],
    edges: Iterable[Mapping[str, Any]],
    skip: Collection[str] = (),
) -> str:
    """The hash of an owner vertex *doc* and its owned *edges*.

    Args:
        doc: The owner vertex's folded document, logical names.
        edges: Its owned edges, each ``{"edge": [source type, target type,
            relation], "source": {...}, "target": {...}, "doc": {...}}``; their
            order does not matter.
        skip: Fields of *doc* left out: the ones set by sync.
    """
    vertex = encode({k: v for k, v in doc.items() if k not in skip})
    entries = sorted(
        json.dumps(encode(dict(e)), sort_keys=True, separators=(",", ":"))
        for e in edges
    )
    return suthing.stable_hash([vertex, entries])
