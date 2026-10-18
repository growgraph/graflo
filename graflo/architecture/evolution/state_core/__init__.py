"""Deprecated: ``state-core`` is now the ``provenance`` and ``state`` aspects.

Every name here, and in the ``plan``, ``spec`` and ``vocabulary`` submodules,
resolves to :mod:`graflo.architecture.evolution.aspects` with a
``DeprecationWarning``. ``plan_lift`` keeps its signature and plans both
aspects, as ``state-core`` did. Removed in graflo 2.0.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._shim import forwarder

if TYPE_CHECKING:
    from graflo.architecture.evolution.aspects import (
        EdgeGrounding,
        Grounding,
        LiftError,
        LiftSpec,
        plan_lift,
    )

__getattr__ = forwarder(
    __name__,
    {
        name: ("", name)
        for name in ("EdgeGrounding", "Grounding", "LiftError", "LiftSpec", "plan_lift")
    },
)

__all__ = ["EdgeGrounding", "Grounding", "LiftError", "LiftSpec", "plan_lift"]
