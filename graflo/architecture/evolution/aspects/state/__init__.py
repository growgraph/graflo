"""The ``state`` aspect: how facts change over time, and what was measured (PROV-O, SOSA, QUDT)."""

from graflo.architecture.evolution.aspects.state.plan import StatePlan, plan_state
from graflo.architecture.evolution.aspects.state.vocabulary import (
    observation_type_name,
    observation_vertex,
    state_type_name,
    state_vertex,
)

__all__ = [
    "StatePlan",
    "observation_type_name",
    "observation_vertex",
    "plan_state",
    "state_type_name",
    "state_vertex",
]
