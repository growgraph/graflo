"""The optional ``bulk`` extra, imported only when a bulk run needs it."""

from __future__ import annotations

from types import ModuleType

from graflo.util.extras import require as _require

_FEATURE = "bulk mode"


def duckdb() -> ModuleType:
    """The ``duckdb`` module.

    Raises:
        MissingExtraError: The ``bulk`` extra is not installed.
    """
    return _require("duckdb", "bulk", feature=_FEATURE)


def zstandard() -> ModuleType:
    """The ``zstandard`` module.

    Raises:
        MissingExtraError: The ``bulk`` extra is not installed.
    """
    return _require("zstandard", "bulk", feature=_FEATURE)


def require() -> None:
    """Fail now, naming the extra, if a bulk run could not fold.

    Raises:
        MissingExtraError: The ``bulk`` extra is not installed.
    """
    duckdb()
    zstandard()
