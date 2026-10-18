"""Deprecated: use :mod:`graflo.architecture.evolution.aspects`. Removed in graflo 2.0."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._shim import forwarder

if TYPE_CHECKING:
    from graflo.architecture.evolution.aspects import LiftError, plan_lift

__getattr__ = forwarder(
    __name__, {"LiftError": ("", "LiftError"), "plan_lift": ("", "plan_lift")}
)

__all__ = ["LiftError", "plan_lift"]
