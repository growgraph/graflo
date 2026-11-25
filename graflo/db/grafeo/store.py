"""Open Grafeo databases, one per file (or one in memory) per process.

A Grafeo file can be open once per process (a second open raises
``GrafeoLockedError``), and opening one decodes it into memory. So the database
is opened once and kept: every connection to the same path shares it, and the
in-memory database lives as long as the process. :func:`close_database` closes
one explicitly; every open database is closed at interpreter exit.

Grafeo runs one query at a time per store, and two concurrent ``MERGE``s on one
pattern fail with a write conflict, so each store carries a lock that every
query of a :class:`~graflo.db.grafeo.conn.GrafeoConnection` holds.
"""

from __future__ import annotations

import atexit
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from graflo.util.extras import lazy_module

if TYPE_CHECKING:
    import grafeo as _grafeo
else:
    _grafeo = lazy_module("grafeo", "grafeo")

#: Registry key of the process's in-memory database.
MEMORY = ":memory:"


@dataclass
class GrafeoStore:
    """One open database and the lock its queries take."""

    db: _grafeo.GrafeoDB
    lock: threading.RLock = field(default_factory=threading.RLock)


_OPEN: dict[str, GrafeoStore] = {}
_REGISTRY_LOCK = threading.Lock()


def _key(path: Path | None) -> str:
    return MEMORY if path is None else str(Path(path).expanduser().resolve())


def open_store(path: Path | None) -> GrafeoStore:
    """The open database at *path* (``None``: in memory), opening it once.

    Raises:
        MissingExtraError: The ``grafeo`` extra is not installed.
    """
    key = _key(path)
    with _REGISTRY_LOCK:
        store = _OPEN.get(key)
        if store is None:
            if key == MEMORY:
                db = _grafeo.GrafeoDB()
            else:
                Path(key).parent.mkdir(parents=True, exist_ok=True)
                db = _grafeo.GrafeoDB(key)
            store = GrafeoStore(db=db)
            _OPEN[key] = store
        return store


def close_database(path: Path | None = None) -> None:
    """Close the database at *path* (``None``: the in-memory one), if open.

    A file is flushed and released, so another process may open it; the next
    connection reopens it. The in-memory database is discarded.
    """
    key = _key(path)
    with _REGISTRY_LOCK:
        store = _OPEN.pop(key, None)
    if store is not None:
        with store.lock:
            store.db.close()


def close_all() -> None:
    """Close every open database."""
    with _REGISTRY_LOCK:
        stores = list(_OPEN.values())
        _OPEN.clear()
    for store in stores:
        with store.lock:
            store.db.close()


atexit.register(close_all)
