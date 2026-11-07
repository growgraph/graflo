"""The ``GRAPH.BULK`` wire format: binary blobs of nodes of one label or edges of one type.

A blob is a header (the label or type name, a uint32 property count, then the
property names, each null-terminated) followed by one entry per element. A node
entry is its property values. An edge entry is its source and target node ids
(uint64), then its property values. A value is a type byte followed by its
payload; a null value means the property is not set.

FalkorDB numbers the nodes of a new graph in the order they are inserted. The
fold's dense ids are therefore the node ids, provided every node is sent in
dense-id order.
"""

from __future__ import annotations

import math
import struct
from collections.abc import Sequence
from typing import Any

_NULL, _BOOL, _DOUBLE, _STRING, _LONG, _ARRAY = 0, 1, 2, 3, 4, 5
_INT64 = (-(2**63), 2**63 - 1)


class BulkEncodeError(ValueError):
    """A value ``GRAPH.BULK`` cannot carry; raised before anything is sent."""


def _cstring(text: str) -> bytes:
    raw = text.encode()
    if b"\x00" in raw:
        raise BulkEncodeError(f"a NUL byte cannot be sent: {text!r}")
    return raw + b"\x00"


def encode_value(value: Any) -> bytes:
    """One property value: its type byte, then its payload."""
    if value is None:
        return struct.pack("<B", _NULL)
    if isinstance(value, bool):
        return struct.pack("<B?", _BOOL, value)
    if isinstance(value, int):
        if not _INT64[0] <= value <= _INT64[1]:
            raise BulkEncodeError(f"integer out of 64-bit range: {value}")
        return struct.pack("<Bq", _LONG, value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise BulkEncodeError(f"a non-finite float cannot be stored: {value}")
        return struct.pack("<Bd", _DOUBLE, value)
    if isinstance(value, str):
        return struct.pack("<B", _STRING) + _cstring(value)
    if isinstance(value, list):
        return struct.pack("<Bq", _ARRAY, len(value)) + b"".join(
            encode_value(v) for v in value
        )
    raise BulkEncodeError(f"GRAPH.BULK cannot carry a {type(value).__name__} value")


def _properties(docs: Sequence[dict[str, Any]]) -> list[str]:
    names: dict[str, None] = {}
    for doc in docs:
        names.update(dict.fromkeys(doc))
    return list(names)


def _header(name: str, properties: Sequence[str]) -> bytes:
    return (
        _cstring(name)
        + struct.pack("<I", len(properties))
        + b"".join(_cstring(p) for p in properties)
    )


def _values(doc: dict[str, Any], properties: Sequence[str]) -> bytes:
    return b"".join(encode_value(doc.get(p)) for p in properties)


def node_blob(label: str, docs: Sequence[dict[str, Any]]) -> bytes:
    """Nodes of one label, in the order they get their ids."""
    properties = _properties(docs)
    return _header(label, properties) + b"".join(_values(d, properties) for d in docs)


def edge_blob(relation: str, rows: Sequence[tuple[int, int, dict[str, Any]]]) -> bytes:
    """Edges of one type, each ``(source node id, target node id, properties)``."""
    properties = _properties([props for _, _, props in rows])
    return _header(relation, properties) + b"".join(
        struct.pack("<QQ", source, target) + _values(props, properties)
        for source, target, props in rows
    )


def bulk_args(
    *,
    begin: bool,
    nodes: Sequence[tuple[bytes, int]] = (),
    edges: Sequence[tuple[bytes, int]] = (),
) -> list[Any]:
    """The arguments of one ``GRAPH.BULK`` call after the graph name.

    Args:
        begin: The call creates the graph key; only the first call of a load.
        nodes: ``(blob, element count)`` per label.
        edges: ``(blob, element count)`` per edge type.
    """
    args: list[Any] = ["BEGIN"] if begin else []
    args += [
        sum(n for _, n in nodes),
        sum(n for _, n in edges),
        len(nodes),
        len(edges),
    ]
    return args + [blob for blob, _ in nodes] + [blob for blob, _ in edges]
