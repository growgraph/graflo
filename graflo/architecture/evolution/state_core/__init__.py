"""Deprecated: ``state-core`` is now the ``provenance`` and ``state`` aspects.

Every name here is re-exported from
:mod:`graflo.architecture.evolution.aspects` with a ``DeprecationWarning``.
``plan_lift`` keeps its signature and plans both aspects, as ``state-core``
did. Removed in graflo 2.0.
"""

from __future__ import annotations

from typing import Any

from graflo._compat import warn_deprecated

_NAMES = frozenset({"EdgeGrounding", "Grounding", "LiftError", "LiftSpec", "plan_lift"})


def __getattr__(name: str) -> Any:
    if name in _NAMES:
        warn_deprecated(
            f"graflo.architecture.evolution.state_core.{name}",
            since="1.19.0",
            removed_in="2.0.0",
            use=f"graflo.architecture.evolution.aspects.{name}",
        )
        from graflo.architecture.evolution import aspects

        return getattr(aspects, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["EdgeGrounding", "Grounding", "LiftError", "LiftSpec", "plan_lift"]
