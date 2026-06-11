"""Compatibility shims for third-party libraries used by TigerGraph connection."""

from __future__ import annotations


def _add_note_shim(self: Exception, note: str) -> None:
    """Add a note to the exception (compatibility shim for exceptions without add_note())."""
    notes: list[str] = getattr(self, "_notes", [])
    notes.append(note)
    self._notes = notes  # type: ignore[attr-defined]


def _patch_exception_class(cls: type[Exception]) -> None:
    """Patch an exception class to add add_note() if it doesn't exist."""
    if not hasattr(cls, "add_note"):
        cls.add_note = _add_note_shim


def patch_requests_exceptions() -> None:
    """Monkey-patch requests exception classes to add add_note() if missing."""
    try:
        from requests.exceptions import (
            ConnectionError,
            HTTPError,
            RequestException,
            Timeout,
        )

        _patch_exception_class(HTTPError)
        _patch_exception_class(ConnectionError)
        _patch_exception_class(Timeout)
        _patch_exception_class(RequestException)
    except (ImportError, AttributeError):
        pass


patch_requests_exceptions()
