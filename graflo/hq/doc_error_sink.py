"""Pluggable sinks for persisting per-document cast failures.

Append-only I/O is required for dead-letter logs. ``suthing.FileHandle.dump`` replaces
the whole file, so it is not used here.
"""

from __future__ import annotations

import asyncio
import gzip
from pathlib import Path
from typing import Protocol, runtime_checkable

from graflo.hq.ingestion_parameters import DocCastFailure, IngestionParams


@runtime_checkable
class DocErrorSink(Protocol):
    """Append structured cast failures (e.g. JSONL or compressed JSONL)."""

    async def write_failures(self, failures: list[DocCastFailure]) -> None:
        """Persist *failures*; must be safe to call under a single async lock."""


class JsonlGzDocErrorSink:
    """Append gzip-compressed JSON lines (one member per write batch)."""

    def __init__(self, path: Path) -> None:
        self._path = path

    async def write_failures(self, failures: list[DocCastFailure]) -> None:
        if not failures:
            return

        def _write_sync() -> None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            # Each append opens a new gzip member; gzip concatenation is standard for log-style files.
            with gzip.open(self._path, "ab") as f:
                for fail in failures:
                    f.write((fail.model_dump_json() + "\n").encode("utf-8"))

        await asyncio.to_thread(_write_sync)


def failure_sinks_from_ingestion_params(params: IngestionParams) -> list[DocErrorSink]:
    """Build file sinks from :class:`~graflo.hq.ingestion_parameters.IngestionParams`."""

    sinks: list[DocErrorSink] = []
    if params.doc_error_sink_path is not None:
        sinks.append(JsonlGzDocErrorSink(params.doc_error_sink_path))
    return sinks
