"""The run ledger: which phases and chunks of a bulk or sync run are done, durably.

One JSON line per event, flushed and synced before the call returns, so after a
crash the ledger never claims more than happened. A chunk recorded as started
but not committed may or may not be in the target: it is written again, as a
replay. A restart forgets every chunk, for a target that lost them.
"""

from __future__ import annotations

import json
import os
import threading
from enum import StrEnum
from pathlib import Path

LEDGER = "ledger.jsonl"


class Phase(StrEnum):
    """Phases of a bulk or sync run, in order."""

    OPENED = "opened"
    STAGED = "staged"
    FOLDED = "folded"
    GROUPED = "grouped"
    BEGINNING = "beginning"
    BEGUN = "begun"
    EDGES_PREPARED = "edges_prepared"
    FINISHED = "finished"


class ChunkState(StrEnum):
    NEW = "new"
    UNCERTAIN = "uncertain"
    COMMITTED = "committed"


class RunLedger:
    """The ledger of the bulk run in *staging_dir*, read on open."""

    def __init__(self, staging_dir: Path) -> None:
        self.path = Path(staging_dir) / LEDGER
        self._lock = threading.Lock()
        self._phases: set[str] = set()
        self._started: set[str] = set()
        self._committed: set[str] = set()
        self.mode: str | None = None
        if self.path.exists():
            for line in self.path.read_text().splitlines():
                if not line.strip():
                    continue
                event = json.loads(line)
                if "mode" in event:
                    self.mode = event["mode"]
                elif "phase" in event:
                    self._phases.add(event["phase"])
                elif "restart" in event:
                    self._forget()
                elif event.get("state") == "started":
                    self._started.add(event["chunk"])
                elif event.get("state") == "committed":
                    self._committed.add(event["chunk"])

    @property
    def exists(self) -> bool:
        return self.path.exists()

    def has(self, phase: Phase) -> bool:
        return phase.value in self._phases

    def set_mode(self, mode: str) -> None:
        """Record the run's mode, ``bulk`` or ``sync``; first, in a new ledger."""
        self._append({"mode": mode})
        self.mode = mode

    def mark(self, phase: Phase) -> None:
        if phase.value not in self._phases:
            self._append({"phase": phase.value})
            self._phases.add(phase.value)

    def chunk(self, chunk_id: str) -> ChunkState:
        if chunk_id in self._committed:
            return ChunkState.COMMITTED
        if chunk_id in self._started:
            return ChunkState.UNCERTAIN
        return ChunkState.NEW

    def started(self, chunk_id: str) -> None:
        self._append({"chunk": chunk_id, "state": "started"})
        self._started.add(chunk_id)

    def committed(self, chunk_id: str) -> None:
        self._append({"chunk": chunk_id, "state": "committed"})
        self._committed.add(chunk_id)

    def restart(self) -> None:
        """Forget every chunk and the phases after the load began."""
        self._append({"restart": "chunks"})
        self._forget()

    def _forget(self) -> None:
        self._started.clear()
        self._committed.clear()
        self._phases -= {Phase.EDGES_PREPARED.value, Phase.FINISHED.value}

    def _append(self, event: dict[str, str]) -> None:
        line = json.dumps(event, separators=(",", ":")) + "\n"
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a") as out:
                out.write(line)
                out.flush()
                os.fsync(out.fileno())
