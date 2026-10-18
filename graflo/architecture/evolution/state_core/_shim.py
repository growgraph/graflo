"""The module ``__getattr__`` behind every deprecated ``state_core`` name."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

from graflo._compat import warn_deprecated

_ASPECTS = "graflo.architecture.evolution.aspects"


def forwarder(module: str, targets: dict[str, tuple[str, str]]) -> Callable[[str], Any]:
    """A PEP 562 ``__getattr__`` resolving each old name to its aspects home.

    Args:
        module: the deprecated module's ``__name__``.
        targets: old name -> (module under ``aspects``, "" for the package
            itself; name there).
    """

    def __getattr__(name: str) -> Any:
        if name not in targets:
            raise AttributeError(f"module {module!r} has no attribute {name!r}")
        where, new = targets[name]
        home = f"{_ASPECTS}.{where}" if where else _ASPECTS
        warn_deprecated(
            f"{module}.{name}",
            since="1.19.0",
            removed_in="2.0.0",
            use=f"{home}.{new}",
        )
        return getattr(importlib.import_module(home), new)

    return __getattr__
