"""Rows on disk: typed identity keys, lossless values, compressed JSON Lines.

A staged or folded row is one JSON object per line, in zstd frames. JSON keeps
an absent field apart from a null one. Values JSON cannot hold (dates, times,
UUIDs, decimals) are written as ``{"$graflo": <type>, "v": <text>}`` and read
back as the same Python value.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from graflo.hq.bulk import _deps
from graflo.util.typed_values import (
    TAG,
    IdentityKeyError,
    decode,
    encode,
    identity_member,
    key_member,
    typed_key,
)

__all__ = [
    "TAG",
    "IdentityKeyError",
    "decode",
    "dumps",
    "encode",
    "identity_member",
    "key_member",
    "read_rows",
    "typed_key",
    "write_rows",
]


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
