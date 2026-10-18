"""Deprecated: use :mod:`graflo.architecture.evolution.aspects.spec`. Removed in graflo 2.0."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._shim import forwarder

if TYPE_CHECKING:
    from graflo.architecture.evolution.aspects.spec import (
        EdgeGrounding,
        Grounding,
        LiftSpec,
    )

__getattr__ = forwarder(
    __name__,
    {name: ("spec", name) for name in ("EdgeGrounding", "Grounding", "LiftSpec")},
)

__all__ = ["EdgeGrounding", "Grounding", "LiftSpec"]
