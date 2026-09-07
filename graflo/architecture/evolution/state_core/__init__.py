"""``state-core``: lifting an arbitrary manifest into a twin-ready schema.

A meta-layer rather than a model. Given any manifest and a statement of what its
types *mean*, :func:`plan_lift` emits the operations that add temporal validity
and provenance to it: state reified onto its own types with a validity interval,
measurements carrying their unit, facts attributable to evidence and an agent.

The output is an op list, so the conversion is replayable, invertible and
reviewable through the machinery every other manifest change already uses.
"""

from graflo.architecture.evolution.state_core.plan import LiftError, plan_lift
from graflo.architecture.evolution.state_core.spec import (
    EdgeGrounding,
    Grounding,
    LiftSpec,
)

__all__ = [
    "EdgeGrounding",
    "Grounding",
    "LiftError",
    "LiftSpec",
    "plan_lift",
]
