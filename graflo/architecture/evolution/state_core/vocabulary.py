"""Deprecated: the pieces now live in the aspects. Removed in graflo 2.0.

IRIs are in :mod:`graflo.architecture.evolution.aspects.iris`, ``Agent`` and
the PROV-O edges in :mod:`~graflo.architecture.evolution.aspects.provenance.vocabulary`,
the state and observation scaffolds in
:mod:`~graflo.architecture.evolution.aspects.state.vocabulary`. ``EVIDENCE``
and ``evidence_vertex`` resolve to ``SOURCE_RECORD`` and
``source_record_vertex``, the vertex a lift now mints in its place.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._shim import forwarder

if TYPE_CHECKING:
    from graflo.architecture.evolution.aspects.iris import (
        DCTERMS_IS_PART_OF,
        PROV_WAS_INFLUENCED_BY,
    )
    from graflo.architecture.evolution.aspects.provenance.vocabulary import (
        AGENT,
        agent_vertex,
        attributed_to_edge,
        derived_from_edge,
    )
    from graflo.architecture.evolution.aspects.provenance.vocabulary import (
        SOURCE_RECORD as EVIDENCE,
    )
    from graflo.architecture.evolution.aspects.provenance.vocabulary import (
        source_record_vertex as evidence_vertex,
    )
    from graflo.architecture.evolution.aspects.state.vocabulary import (
        RESULT_TIME,
        RESULT_UNIT,
        VALID_FROM,
        VALID_TO,
        feature_of_interest_edge,
        observation_type_name,
        observation_vertex,
        specialization_edge,
        state_type_name,
        state_vertex,
    )

_IRIS = "iris"
_PROVENANCE = "provenance.vocabulary"
_STATE = "state.vocabulary"

__getattr__ = forwarder(
    __name__,
    {
        "DCTERMS_IS_PART_OF": (_IRIS, "DCTERMS_IS_PART_OF"),
        "PROV_WAS_INFLUENCED_BY": (_IRIS, "PROV_WAS_INFLUENCED_BY"),
        "AGENT": (_PROVENANCE, "AGENT"),
        "EVIDENCE": (_PROVENANCE, "SOURCE_RECORD"),
        "agent_vertex": (_PROVENANCE, "agent_vertex"),
        "attributed_to_edge": (_PROVENANCE, "attributed_to_edge"),
        "derived_from_edge": (_PROVENANCE, "derived_from_edge"),
        "evidence_vertex": (_PROVENANCE, "source_record_vertex"),
        **{
            name: (_STATE, name)
            for name in (
                "RESULT_TIME",
                "RESULT_UNIT",
                "VALID_FROM",
                "VALID_TO",
                "feature_of_interest_edge",
                "observation_type_name",
                "observation_vertex",
                "specialization_edge",
                "state_type_name",
                "state_vertex",
            )
        },
    },
)

__all__ = [
    "AGENT",
    "DCTERMS_IS_PART_OF",
    "EVIDENCE",
    "PROV_WAS_INFLUENCED_BY",
    "RESULT_TIME",
    "RESULT_UNIT",
    "VALID_FROM",
    "VALID_TO",
    "agent_vertex",
    "attributed_to_edge",
    "derived_from_edge",
    "evidence_vertex",
    "feature_of_interest_edge",
    "observation_type_name",
    "observation_vertex",
    "specialization_edge",
    "state_type_name",
    "state_vertex",
]
