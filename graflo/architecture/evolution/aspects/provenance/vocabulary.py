"""The provenance aspect's types: where a fact came from, and who produced it.

``SourceRecord`` is the record a fact was read from -- a document, a message,
an API response, a table row. It carries the *address* of that record and a
hash of its content, never the content: the text or the row stays where it
was landed, and the graph points at it. ``Agent`` is who or what produced the
record. Both are ``prov:`` types; the two edges are the PROV-O relations.
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
    RDFS_LABEL,
    SOSA_SENSOR,
)

SOURCE_RECORD = "SourceRecord"
AGENT = "Agent"

SOURCE_RECORD_ID = "source_record_id"
SOURCE_URI = "source_uri"
MEDIA_TYPE = "media_type"
CONTENT_HASH = "content_hash"
RETRIEVED_AT = "retrieved_at"
ROLE = "role"


def source_record_vertex() -> Vertex:
    """The record a fact was read from: its address and hash, never its content."""
    return Vertex(
        name=SOURCE_RECORD,
        description=(
            "The record a fact was read from: a document, a message, an API "
            "response, a table row. Carries where the record is and a hash of "
            "what it held, not the content itself."
        ),
        semantics=Semantics(iri=PROV_ENTITY, exact_match=[PROV_ENTITY]),
        properties=[
            Field(name=SOURCE_RECORD_ID, type=FieldType.STRING),
            Field(name=SOURCE_URI, type=FieldType.STRING),
            Field(name=MEDIA_TYPE, type=FieldType.STRING),
            Field(name=CONTENT_HASH, type=FieldType.STRING),
            Field(
                name=RETRIEVED_AT,
                type=FieldType.DATETIME,
                semantics=FieldSemantics(iri=PROV_GENERATED_AT),
            ),
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
        "wasDerivedFrom",
        PROV_WAS_DERIVED_FROM,
        "What this fact was read from.",
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
    "CONTENT_HASH",
    "MEDIA_TYPE",
    "RETRIEVED_AT",
    "ROLE",
    "SOURCE_RECORD",
    "SOURCE_RECORD_ID",
    "SOURCE_URI",
    "agent_vertex",
    "attributed_to_edge",
    "derived_from_edge",
    "grounded_edge",
    "source_record_vertex",
]
