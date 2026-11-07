"""Rows on disk: typed identity keys, lossless values, compressed JSON Lines.

A staged or folded row is one JSON object per line, in zstd frames. JSON keeps
an absent field apart from a null one. Values JSON cannot hold (dates, times,
UUIDs, decimals) are written as ``{"$graflo": <type>, "v": <text>}`` and read
back as the same Python value.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
from collections.abc import Iterable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from graflo.hq.bulk import _deps

TAG = "$graflo"


class IdentityKeyError(ValueError):
    """An identity value that cannot key a row: null, empty, NaN or not a scalar."""


def key_member(value: Any) -> list[Any]:
    """One identity value as ``[kind, value]``, compared as the targets compare it.

    Numbers compare by value (``1`` and ``1.0`` are one key), booleans apart
    from numbers, strings apart from numbers (``1`` and ``"1"`` are two keys).
    A UUID keys as its text, the way every backend stores it.

    Raises:
        IdentityKeyError: The value cannot key a row.
    """
    if value is None or value == "":
        raise IdentityKeyError("empty identity value")
    if isinstance(value, bool):
        return ["b", value]
    if isinstance(value, int):
        return ["n", value]
    if isinstance(value, float | Decimal):
        if isinstance(value, float) and not math.isfinite(value):
            raise IdentityKeyError(f"identity value {value!r} is not finite")
        if value == int(value):
            return ["n", int(value)]
        return ["n", float(value)]
    if isinstance(value, str):
        return ["s", value]
    if isinstance(value, UUID):
        return ["s", str(value)]
    if isinstance(value, dt.datetime):
        return ["t", value.isoformat()]
    if isinstance(value, dt.date):
        return ["d", value.isoformat()]
    if isinstance(value, dt.time):
        return ["h", value.isoformat()]
    raise IdentityKeyError(
        f"identity value of type {type(value).__name__} is not a scalar"
    )


def typed_key(values: Iterable[Any]) -> str:
    """The canonical text of a tuple of identity values; equal keys, equal text.

    Raises:
        IdentityKeyError: A value cannot key a row.
    """
    return json.dumps(
        [key_member(v) for v in values], separators=(",", ":"), ensure_ascii=False
    )


def encode(value: Any) -> Any:
    """*value* as JSON-ready data, non-JSON scalars tagged.

    Raises:
        TypeError: A value graflo does not write.
    """
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {TAG: "float", "v": repr(value)}
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [encode(v) for v in value]
    if isinstance(value, dt.datetime):
        return {TAG: "datetime", "v": value.isoformat()}
    if isinstance(value, dt.date):
        return {TAG: "date", "v": value.isoformat()}
    if isinstance(value, dt.time):
        return {TAG: "time", "v": value.isoformat()}
    if isinstance(value, UUID):
        return {TAG: "uuid", "v": str(value)}
    if isinstance(value, Decimal):
        return {TAG: "decimal", "v": str(value)}
    raise TypeError(f"cannot stage a value of type {type(value).__name__}")


_DECODERS = {
    "float": float,
    "datetime": dt.datetime.fromisoformat,
    "date": dt.date.fromisoformat,
    "time": dt.time.fromisoformat,
    "uuid": UUID,
    "decimal": Decimal,
}


def decode(value: Any) -> Any:
    """The inverse of :func:`encode`."""
    if isinstance(value, dict):
        if len(value) == 2 and TAG in value and "v" in value:
            return _DECODERS[value[TAG]](value["v"])
        return {k: decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode(v) for v in value]
    return value


def dumps(row: dict[str, Any]) -> bytes:
    """One row as a JSON line."""
    return (
        json.dumps(row, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode()


def write_rows(path: Path, rows: Iterable[bytes]) -> int:
    """Write JSON lines to *path* as one zstd frame, atomically; return the count.

    The file appears complete or not at all, so a crash never leaves a part.
    """
    zstd = _deps.zstandard()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    count = 0
    with open(tmp, "wb") as raw:
        with zstd.ZstdCompressor().stream_writer(raw, closefd=False) as out:
            for line in rows:
                out.write(line)
                count += 1
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(tmp, path)
    return count


def read_rows(path: Path) -> Iterator[dict[str, Any]]:
    """The rows of a zstd JSON Lines file, values decoded."""
    zstd = _deps.zstandard()
    with open(path, "rb") as raw:
        reader = zstd.ZstdDecompressor().stream_reader(raw, read_across_frames=True)
        buffer = b""
        while chunk := reader.read(1 << 20):
            buffer += chunk
            *lines, buffer = buffer.split(b"\n")
            for line in lines:
                if line:
                    yield decode(json.loads(line))
        if buffer.strip():
            yield decode(json.loads(buffer))
