"""The ``provenance`` aspect: where a fact came from, and who produced it (PROV-O)."""

from graflo.architecture.evolution.aspects.provenance.plan import plan_provenance
from graflo.architecture.evolution.aspects.provenance.vocabulary import (
    AGENT,
    SOURCE_RECORD,
    agent_vertex,
    attributed_to_edge,
    derived_from_edge,
    source_record_vertex,
)

__all__ = [
    "AGENT",
    "SOURCE_RECORD",
    "agent_vertex",
    "attributed_to_edge",
    "derived_from_edge",
    "plan_provenance",
    "source_record_vertex",
]
