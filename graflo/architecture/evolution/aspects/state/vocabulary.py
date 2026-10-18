"""The state aspect's types: how a fact changes over time, and what was measured.

Not a manifest. These are the *pieces* the planner instantiates against
whatever types the input already has, which is the difference between a
reference model you compose onto and a transformation you apply. Time and unit
conventions are stated in :mod:`graflo.architecture.evolution.aspects.iris`.
"""

from __future__ import annotations

from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, FieldType, Vertex

from ..iris import (
    PROV_ENTITY,
    PROV_GENERATED_AT,
    PROV_INVALIDATED_AT,
    PROV_SPECIALIZATION_OF,
    QUDT_UCUM_CODE,
    SOSA_HAS_FEATURE_OF_INTEREST,
    SOSA_HAS_SIMPLE_RESULT,
    SOSA_OBSERVATION,
    SOSA_OBSERVED_PROPERTY,
    SOSA_RESULT_TIME,
    SSN_PROPERTY,
    TIME_PROPER_INTERVAL,
)
from ..provenance.vocabulary import grounded_edge

STATE_SUFFIX = "State"
OBSERVATION_SUFFIX = "Observation"

VALID_FROM = "valid_from"
VALID_TO = "valid_to"
OBSERVED_PROPERTY = "observed_property"
RESULT_VALUE = "result_value"
RESULT_UNIT = "result_unit"
RESULT_TIME = "result_time"


def state_type_name(vertex: str) -> str:
    """``Machine`` -> ``MachineState``."""
    return f"{vertex}{STATE_SUFFIX}"


def observation_type_name(vertex: str) -> str:
    """``Machine`` -> ``MachineObservation``."""
    return f"{vertex}{OBSERVATION_SUFFIX}"


def _validity_fields() -> list[Field]:
    return [
        Field(
            name=VALID_FROM,
            type=FieldType.DATETIME,
            semantics=FieldSemantics(iri=PROV_GENERATED_AT),
        ),
        Field(
            name=VALID_TO,
            type=FieldType.DATETIME,
            semantics=FieldSemantics(iri=PROV_INVALIDATED_AT),
        ),
    ]


def state_vertex(subject: str, key_fields: list[Field], moved: list[Field]) -> Vertex:
    """The ``State`` type for one subject: its key, its mutable facts, a validity interval.

    Keyed on the subject's own key *plus* ``valid_from``: the same property of
    the same entity holds many values over time, and those are different facts
    rather than revisions of one. Closing ``valid_to`` instead of overwriting is
    what makes the history queryable at all.
    """
    return Vertex(
        name=state_type_name(subject),
        description=(
            f"Mutable facts about `{subject}`, each holding over one interval. "
            "Closing `valid_to` rather than overwriting is what makes history "
            "queryable."
        ),
        semantics=Semantics(
            iri=PROV_ENTITY, exact_match=[PROV_ENTITY, TIME_PROPER_INTERVAL]
        ),
        properties=[
            *(field.model_copy(deep=True) for field in key_fields),
            *(field.model_copy(deep=True) for field in moved),
            *_validity_fields(),
        ],
        hash_identity_properties=[*(f.name for f in key_fields), VALID_FROM],
    )


def observation_vertex(subject: str, key_fields: list[Field]) -> Vertex:
    """The ``Observation`` type for one subject: a measurement at a time.

    ``result_unit`` is a property rather than a contract-level declaration
    because this type is abstract: one measuring temperature and one measuring
    pressure are the same type here, so the unit has to travel with the row.
    """
    return Vertex(
        name=observation_type_name(subject),
        description=f"A measurement of `{subject}` at a time.",
        semantics=Semantics(iri=SOSA_OBSERVATION, exact_match=[SOSA_OBSERVATION]),
        properties=[
            *(field.model_copy(deep=True) for field in key_fields),
            Field(
                name=OBSERVED_PROPERTY,
                type=FieldType.STRING,
                semantics=FieldSemantics(
                    exact_match=[SOSA_OBSERVED_PROPERTY, SSN_PROPERTY]
                ),
            ),
            Field(
                name=RESULT_VALUE,
                type=FieldType.FLOAT,
                semantics=FieldSemantics(exact_match=[SOSA_HAS_SIMPLE_RESULT]),
            ),
            Field(
                name=RESULT_UNIT,
                type=FieldType.STRING,
                semantics=FieldSemantics(iri=QUDT_UCUM_CODE),
            ),
            Field(
                name=RESULT_TIME,
                type=FieldType.DATETIME,
                semantics=FieldSemantics(iri=SOSA_RESULT_TIME),
            ),
        ],
        hash_identity_properties=[
            *(f.name for f in key_fields),
            OBSERVED_PROPERTY,
            RESULT_TIME,
        ],
    )


def specialization_edge(subject: str) -> Edge:
    """``<Subject>State -specializationOf-> <Subject>``."""
    return grounded_edge(
        state_type_name(subject),
        subject,
        "specializationOf",
        PROV_SPECIALIZATION_OF,
        f"The {subject} this state is a state of.",
    )


def feature_of_interest_edge(subject: str) -> Edge:
    """``<Subject>Observation -hasFeatureOfInterest-> <Subject>``."""
    return grounded_edge(
        observation_type_name(subject),
        subject,
        "hasFeatureOfInterest",
        SOSA_HAS_FEATURE_OF_INTEREST,
        f"The {subject} this observation measured.",
    )


__all__ = [
    "OBSERVED_PROPERTY",
    "RESULT_TIME",
    "RESULT_UNIT",
    "RESULT_VALUE",
    "VALID_FROM",
    "VALID_TO",
    "feature_of_interest_edge",
    "observation_type_name",
    "observation_vertex",
    "specialization_edge",
    "state_type_name",
    "state_vertex",
]
