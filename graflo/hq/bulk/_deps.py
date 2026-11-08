"""The optional ``bulk`` extra, imported only when a bulk run needs it."""

from __future__ import annotations

import sys
from types import ModuleType

_MISSING_EXTRA = (
    "bulk mode needs the `bulk` extra, which is not installed for {python} "
    "(missing module {module!r}): install it with `uv add 'graflo[bulk]'` "
    "(or `pip install 'graflo[bulk]'`)"
)


def _import(name: str) -> ModuleType:
    try:
        return __import__(name)
    except ImportError as exc:
        raise RuntimeError(
            _MISSING_EXTRA.format(python=sys.executable, module=exc.name or name)
        ) from exc


def duckdb() -> ModuleType:
    """The ``duckdb`` module.

    Raises:
        RuntimeError: The ``bulk`` extra is not installed.
    """
    return _import("duckdb")


def zstandard() -> ModuleType:
    """The ``zstandard`` module.

    Raises:
        RuntimeError: The ``bulk`` extra is not installed.
    """
    return _import("zstandard")


def require() -> None:
    """Fail now, naming the extra, if a bulk run could not fold.

    Raises:
        RuntimeError: The ``bulk`` extra is not installed.
    """
    duckdb()
    zstandard()
