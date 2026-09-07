"""The target shape a lift converts a manifest towards.

Not a manifest. These are the *pieces* the planner instantiates against
whatever types the input already has, which is the difference between a
reference model you compose onto and a transformation you apply.

Two conventions the whole package rests on, both stated here so a reader does
not have to infer them from the IRIs:

**Time is grounded in PROV-O and SOSA, not OWL-Time.** ``time:hasBeginning``
ranges over a ``time:Instant``, not a literal, so grounding a ``DATETIME``
column in it is a claim that becomes false the moment the schema is projected to
OWL. ``prov:generatedAtTime``, ``prov:invalidatedAtTime`` and ``sosa:resultTime``
are literal-ranged and say the same thing truthfully. OWL-Time stays in
``exact_match`` at the concept level, where it is about the *type*.

**Units are UCUM tokens, carried per row.** An abstract observation type serves
temperature and pressure alike, so it cannot name one unit in its contract
without lying; ``result_unit`` is grounded in ``qudt:ucumCode`` and travels with
the measurement. UCUM has no currency, so currency falls back to ISO-4217 alpha
codes (``USD``, ``EUR``).
"""

from __future__ import annotations

from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, FieldType, Vertex

# --- vocabulary ------------------------------------------------------------

PROV = "http://www.w3.org/ns/prov#"
SOSA = "http://www.w3.org/ns/sosa/"
SSN = "http://www.w3.org/ns/ssn/"
TIME = "http://www.w3.org/2006/time#"
QUDT = "http://qudt.org/schema/qudt/"
DCTERMS = "http://purl.org/dc/terms/"

PROV_ENTITY = f"{PROV}Entity"
PROV_ACTIVITY = f"{PROV}Activity"
PROV_AGENT = f"{PROV}Agent"
PROV_GENERATED_AT = f"{PROV}generatedAtTime"
PROV_INVALIDATED_AT = f"{PROV}invalidatedAtTime"
PROV_SPECIALIZATION_OF = f"{PROV}specializationOf"
PROV_WAS_DERIVED_FROM = f"{PROV}wasDerivedFrom"
PROV_WAS_ATTRIBUTED_TO = f"{PROV}wasAttributedTo"
PROV_WAS_INFLUENCED_BY = f"{PROV}wasInfluencedBy"

SOSA_OBSERVATION = f"{SOSA}Observation"
SOSA_SENSOR = f"{SOSA}Sensor"
SOSA_FEATURE_OF_INTEREST = f"{SOSA}FeatureOfInterest"
SOSA_HAS_FEATURE_OF_INTEREST = f"{SOSA}hasFeatureOfInterest"
SOSA_OBSERVED_PROPERTY = f"{SOSA}observedProperty"
SOSA_HAS_SIMPLE_RESULT = f"{SOSA}hasSimpleResult"
SOSA_RESULT_TIME = f"{SOSA}resultTime"

SSN_PROPERTY = f"{SSN}Property"
TIME_PROPER_INTERVAL = f"{TIME}ProperInterval"
QUDT_UCUM_CODE = f"{QUDT}ucumCode"
DCTERMS_IS_PART_OF = f"{DCTERMS}isPartOf"

#: Names the lift mints. Reserved: a manifest already using one cannot be
#: lifted without a rename first, and the planner says so rather than colliding.
EVIDENCE = "Evidence"
AGENT = "Agent"
STATE_SUFFIX = "State"
OBSERVATION_SUFFIX = "Observation"

VALID_FROM = "valid_from"
VALID_TO = "valid_to"
OBSERVED_PROPERTY = "observed_property"
RESULT_VALUE = "result_value"
RESULT_UNIT = "result_unit"
RESULT_TIME = "result_time"


def state_type_name(vertex: str) -> str:
    """``Device`` -> ``DeviceState``."""
    return f"{vertex}{STATE_SUFFIX}"


def observation_type_name(vertex: str) -> str:
    """``Device`` -> ``DeviceObservation``."""
    return f"{vertex}{OBSERVATION_SUFFIX}"


# --- the pieces ------------------------------------------------------------


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
            f"Mutable facts about a {subject}, each holding over one interval. "
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
        description=f"A measurement of a {subject} at a time.",
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


def evidence_vertex() -> Vertex:
    """What a fact was read from, and the attachment point for extracted data."""
    return Vertex(
        name=EVIDENCE,
        description=(
            "What a fact was read from: a document, an API response, a table row."
        ),
        semantics=Semantics(iri=PROV_ENTITY, exact_match=[PROV_ENTITY]),
        properties=[
            Field(name="evidence_id", type=FieldType.STRING),
            Field(name="source_uri", type=FieldType.STRING),
            Field(name="media_type", type=FieldType.STRING),
            Field(
                name="retrieved_at",
                type=FieldType.DATETIME,
                semantics=FieldSemantics(iri=PROV_GENERATED_AT),
            ),
        ],
        identity=["evidence_id"],
    )


def agent_vertex() -> Vertex:
    """Who or what acted or measured: a person, a team, a sensor, a system."""
    return Vertex(
        name=AGENT,
        description="Who or what acted or measured.",
        semantics=Semantics(iri=PROV_AGENT, exact_match=[PROV_AGENT, SOSA_SENSOR]),
        properties=[
            Field(name="agent_id", type=FieldType.STRING),
            Field(name="label", type=FieldType.STRING),
            Field(name="agent_kind", type=FieldType.STRING),
        ],
        identity=["agent_id"],
    )


def _edge(source: str, target: str, relation: str, iri: str, description: str) -> Edge:
    return Edge(
        source=source,
        target=target,
        relation=relation,
        directed=True,
        description=description,
        semantics=Semantics(iri=iri, exact_match=[iri]),
    )


def specialization_edge(subject: str) -> Edge:
    return _edge(
        state_type_name(subject),
        subject,
        "specializationOf",
        PROV_SPECIALIZATION_OF,
        f"The {subject} this state is a state of.",
    )


def feature_of_interest_edge(subject: str) -> Edge:
    return _edge(
        observation_type_name(subject),
        subject,
        "hasFeatureOfInterest",
        SOSA_HAS_FEATURE_OF_INTEREST,
        f"The {subject} this observation measured.",
    )


def derived_from_edge(source: str) -> Edge:
    return _edge(
        source,
        EVIDENCE,
        "wasDerivedFrom",
        PROV_WAS_DERIVED_FROM,
        "What this fact was read from.",
    )


def attributed_to_edge() -> Edge:
    return _edge(
        EVIDENCE,
        AGENT,
        "wasAttributedTo",
        PROV_WAS_ATTRIBUTED_TO,
        "Who or what produced this evidence.",
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
