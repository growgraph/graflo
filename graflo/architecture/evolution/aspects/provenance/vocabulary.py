"""The provenance aspect's types: where a fact came from, and who produced it.

``SourceRecord`` is the record a fact was read from -- a document, a message,
an API response, a table row. It carries the *address* of that record and a
hash of its content, never the content: the text or the row stays where it
was landed, and the graph points at it. ``Agent`` is who or what produced the
record. Both are ``prov:`` types; ``wasDerivedFrom`` and ``wasAttributedTo`` are
the PROV-O relations. ``mentions`` says a record names an entity, which holds
whether or not the entity's facts were read from that record.

``Assertion`` is one statement a record makes, one triple (``rdf:Statement``):
its ``subject`` edge names the entity, ``predicate`` the property or relation
stated, and the stated value is a literal ``value`` for a property or an
``object`` edge for a relation. It carries the quote it was read from, when the
statement says it holds, whether it reports something done, planned or
uncertain, whether it denies it, and how it was obtained (``basis``). A fact's
current value is resolved from its assertions; the assertions are never
overwritten.
"""

from __future__ import annotations

from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, FieldType, Vertex

from ..iris import (
    PROV_AGENT,
    PROV_ENTITY,
    PROV_GENERATED_AT,
    PROV_WAS_ATTRIBUTED_TO,
    PROV_WAS_DERIVED_FROM,
    RDF_OBJECT,
    RDF_PREDICATE,
    RDF_STATEMENT,
    RDF_SUBJECT,
    RDFS_LABEL,
    SCHEMA_MENTIONS,
    SOSA_SENSOR,
    TIME_HAS_BEGINNING,
    TIME_HAS_END,
)

SOURCE_RECORD = "SourceRecord"
AGENT = "Agent"
ASSERTION = "Assertion"

SOURCE_RECORD_ID = "source_record_id"
SOURCE_URI = "source_uri"
MEDIA_TYPE = "media_type"
CONTENT_HASH = "content_hash"
WRITTEN_AT = "written_at"
RETRIEVED_AT = "retrieved_at"
ROLE = "role"

MENTIONS = "mentions"
DERIVED_FROM = "wasDerivedFrom"
SUBJECT = "subject"
OBJECT = "object"

ASSERTION_ID = "assertion_id"
PREDICATE = "predicate"
VALUE = "value"
POLARITY = "polarity"
MODALITY = "modality"
VALID_FROM = "valid_from"
VALID_TO = "valid_to"
GRANULARITY = "granularity"
QUOTE = "quote"
SPAN_START = "span_start"
SPAN_END = "span_end"
BASIS = "basis"


def source_record_vertex() -> Vertex:
    """The record a fact was read from: its address and hash, never its content."""
    return Vertex(
        name=SOURCE_RECORD,
        description=(
            "The record a fact was read from: a document, a message, an API "
            "response, a table row. Carries where the record is and a hash of "
            "what it held, not the content itself; when it was written and when "
            "it was read."
        ),
        semantics=Semantics(iri=PROV_ENTITY, exact_match=[PROV_ENTITY]),
        properties=[
            Field(name=SOURCE_RECORD_ID, type=FieldType.STRING),
            Field(name=SOURCE_URI, type=FieldType.STRING),
            Field(name=MEDIA_TYPE, type=FieldType.STRING),
            Field(name=CONTENT_HASH, type=FieldType.STRING),
            # When the record itself was written, which is what relative times in
            # its text ("this morning") are read against.
            Field(
                name=WRITTEN_AT,
                type=FieldType.DATETIME,
                semantics=FieldSemantics(iri=PROV_GENERATED_AT),
            ),
            Field(name=RETRIEVED_AT, type=FieldType.DATETIME),
        ],
        identity=[SOURCE_RECORD_ID],
    )


def agent_vertex() -> Vertex:
    """Who or what acted or measured: a person, a team, a sensor, a system."""
    return Vertex(
        name=AGENT,
        description="Who or what acted or measured.",
        semantics=Semantics(iri=PROV_AGENT, exact_match=[PROV_AGENT, SOSA_SENSOR]),
        properties=[
            Field(name="agent_id", type=FieldType.STRING),
            Field(
                name="label",
                type=FieldType.STRING,
                semantics=FieldSemantics(iri=RDFS_LABEL),
            ),
            Field(name="agent_kind", type=FieldType.STRING),
        ],
        identity=["agent_id"],
    )


def assertion_vertex() -> Vertex:
    """One statement a record makes: one triple, with its quote, time and mood."""
    string = FieldType.STRING
    return Vertex(
        name=ASSERTION,
        description=(
            "One statement a record makes, one triple: the entity it is about "
            "(subject), the property or relation stated (predicate), and a "
            "property's value or a relation's target (object). With the quote it "
            "was read from, when it says the fact holds, whether it reports "
            "something done, planned or uncertain, whether it denies it, and how "
            "it was obtained."
        ),
        semantics=Semantics(iri=RDF_STATEMENT, exact_match=[RDF_STATEMENT]),
        properties=[
            Field(name=ASSERTION_ID, type=string),
            Field(
                name=PREDICATE,
                type=string,
                semantics=FieldSemantics(iri=RDF_PREDICATE),
            ),
            # A property's value as text, read back as the property's type; a
            # relation's target is its `object` edge instead.
            Field(name=VALUE, type=string),
            # `stated` or `denied`.
            Field(name=POLARITY, type=string),
            # `actual`, `planned` or `hedged`.
            Field(name=MODALITY, type=string),
            Field(
                name=VALID_FROM,
                type=FieldType.DATETIME,
                semantics=FieldSemantics(iri=TIME_HAS_BEGINNING),
            ),
            Field(
                name=VALID_TO,
                type=FieldType.DATETIME,
                semantics=FieldSemantics(iri=TIME_HAS_END),
            ),
            # `instant`, `day`, `week`, `month` or `year`.
            Field(name=GRANULARITY, type=string),
            Field(name=QUOTE, type=string),
            Field(name=SPAN_START, type=FieldType.INT),
            Field(name=SPAN_END, type=FieldType.INT),
            # `extracted` (read from text by a model), `confirmed` (by a
            # person) or `recorded` (by a system of record).
            Field(name=BASIS, type=string),
        ],
        identity=[ASSERTION_ID],
    )


def grounded_edge(
    source: str,
    target: str,
    relation: str,
    iri: str,
    description: str,
    *,
    properties: list[Field] | None = None,
) -> Edge:
    """A directed edge grounded in one IRI, as every aspect edge is."""
    return Edge(
        source=source,
        target=target,
        relation=relation,
        directed=True,
        description=description,
        semantics=Semantics(iri=iri, exact_match=[iri]),
        properties=list(properties or []),
    )


def derived_from_edge(source: str) -> Edge:
    """``<source> -wasDerivedFrom-> SourceRecord``."""
    return grounded_edge(
        source,
        SOURCE_RECORD,
        DERIVED_FROM,
        PROV_WAS_DERIVED_FROM,
        "What this fact was read from.",
    )


def mentions_edge(target: str) -> Edge:
    """``SourceRecord -mentions-> <target>``."""
    return grounded_edge(
        SOURCE_RECORD,
        target,
        MENTIONS,
        SCHEMA_MENTIONS,
        "An entity this record names.",
    )


def subject_edge(target: str) -> Edge:
    """``Assertion -subject-> <target>``: the vertex stated, or a relation's source."""
    return grounded_edge(
        ASSERTION,
        target,
        SUBJECT,
        RDF_SUBJECT,
        "The vertex this statement is about, or the source of the relation it states.",
    )


def object_edge(target: str) -> Edge:
    """``Assertion -object-> <target>``: the target of a stated relation."""
    return grounded_edge(
        ASSERTION,
        target,
        OBJECT,
        RDF_OBJECT,
        "The target of the relation this statement states.",
    )


def attributed_to_edge() -> Edge:
    """``SourceRecord -wasAttributedTo-> Agent``, with the agent's role on the edge."""
    return grounded_edge(
        SOURCE_RECORD,
        AGENT,
        "wasAttributedTo",
        PROV_WAS_ATTRIBUTED_TO,
        "Who or what produced this record, and in which role.",
        properties=[Field(name=ROLE, type=FieldType.STRING)],
    )


__all__ = [
    "AGENT",
    "ASSERTION",
    "ASSERTION_ID",
    "BASIS",
    "CONTENT_HASH",
    "DERIVED_FROM",
    "GRANULARITY",
    "MEDIA_TYPE",
    "MENTIONS",
    "MODALITY",
    "OBJECT",
    "POLARITY",
    "PREDICATE",
    "QUOTE",
    "RETRIEVED_AT",
    "ROLE",
    "SOURCE_RECORD",
    "SOURCE_RECORD_ID",
    "SOURCE_URI",
    "SPAN_END",
    "SPAN_START",
    "SUBJECT",
    "VALID_FROM",
    "VALID_TO",
    "VALUE",
    "WRITTEN_AT",
    "agent_vertex",
    "assertion_vertex",
    "attributed_to_edge",
    "derived_from_edge",
    "grounded_edge",
    "mentions_edge",
    "object_edge",
    "source_record_vertex",
    "subject_edge",
]
