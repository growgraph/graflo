"""Deprecation helpers for graflo's own public API.

A name or config key is deprecated in a minor release and removed in the next
major; every warning names both versions and the replacement.

Example:
    >>> @deprecated(since="1.19.0", removed_in="2.0.0", use="new_fn")
    ... def old_fn(): ...

    Inside a ``mode="before"`` model validator::

        data = rename_key(data, "old", "new", owner="Resource",
                          since="1.19.0", removed_in="2.0.0")
"""

from __future__ import annotations

import functools
import warnings
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def warn_deprecated(
    what: str,
    *,
    since: str,
    removed_in: str,
    use: str | None = None,
    stacklevel: int = 2,
) -> None:
    """Emit a ``DeprecationWarning`` for ``what``.

    Args:
        what: The deprecated name, as a user would write it.
        since: The graflo version that deprecated it.
        removed_in: The graflo version that removes it.
        use: The replacement, if there is one.
        stacklevel: Frames above the caller to attribute the warning to.
    """
    message = (
        f"{what} is deprecated since graflo {since} and will be removed in {removed_in}"
    )
    if use:
        message += f"; use {use}"
    warnings.warn(message, DeprecationWarning, stacklevel=stacklevel + 1)


def deprecated(
    *, since: str, removed_in: str, use: str | None = None
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Mark a function or method deprecated; each call warns at the caller."""

    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        name = f"{fn.__module__}.{getattr(fn, '__qualname__', fn)}"

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            warn_deprecated(
                name,
                since=since,
                removed_in=removed_in,
                use=use,
                stacklevel=2,
            )
            return fn(*args, **kwargs)

        return wrapper

    return decorate


def rename_key(
    data: Any,
    old: str,
    new: str,
    *,
    owner: str,
    since: str,
    removed_in: str,
) -> Any:
    """Move a renamed config key to its new name, warning when the old one is used.

    Meant for a ``mode="before"`` validator: non-dict input passes through. Setting
    both names is an error, since the two values could disagree.

    Raises:
        ValueError: if ``data`` carries both ``old`` and ``new``.
    """
    if not isinstance(data, dict) or old not in data:
        return data
    if new in data:
        raise ValueError(f"{owner}: set {new!r} only; {old!r} is its deprecated name")
    warn_deprecated(
        f"{owner}.{old}",
        since=since,
        removed_in=removed_in,
        use=f"{owner}.{new}",
        stacklevel=4,
    )
    moved = dict(data)
    moved[new] = moved.pop(old)
    return moved
