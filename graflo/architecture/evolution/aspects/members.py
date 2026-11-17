"""Which parts of a schema an aspect added: its own types, its relations, and the types it was lifted onto.

Derived from the schema, not recorded on it. Each relation an aspect lifts has
a fixed shape -- a relation name, its two ends, and the IRI it is grounded in --
built by the aspect's vocabulary. A relation counts as the aspect's when it has
exactly the shape the vocabulary would build for its ends, and a type when the
vocabulary builds it (``SourceRecord``, ``Agent``) or names it after a type it
specialises (``<Type>State``, ``<Type>Observation``). The IRI alone is not
enough: a domain type or relation may be grounded in a PROV term too.

Only aspects that add types and relations have members. ``access`` and ``sync``
add reserved fields, which a caller reads from the fields themselves.
"""

from __future__ import annotations

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Vertex

from .base import PROVENANCE, STATE
from .provenance.vocabulary import (
    AGENT,
    ASSERTION,
    MENTIONS,
    OBJECT,
    SOURCE_RECORD,
    SUBJECT,
    agent_vertex,
    assertion_vertex,
    attributed_to_edge,
    derived_from_edge,
    mentions_edge,
    object_edge,
    source_record_vertex,
    subject_edge,
)
from .state.vocabulary import (
    feature_of_interest_edge,
    observation_type_name,
    specialization_edge,
    state_type_name,
)


class AspectEdge(ConfigBaseModel):
    """One relation an aspect lifted, by its ends and name."""

    source: str
    relation: str
    target: str
    aspect: str


class AspectMembers(ConfigBaseModel):
    """What a schema owes to its aspects.

    Attributes:
        types: Each type an aspect defines, and that aspect.
        edges: Each relation an aspect lifted.
        lifted_onto: Each other type an aspect was lifted onto, and those
            aspects in name order.
    """

    types: dict[str, str] = PydanticField(default_factory=dict)
    edges: list[AspectEdge] = PydanticField(default_factory=list)
    lifted_onto: dict[str, list[str]] = PydanticField(default_factory=dict)


def _iri(element: Vertex | Edge) -> str | None:
    return element.semantics.iri if element.semantics else None


def _same_shape(edge: Edge, built: Edge) -> bool:
    return (
        edge.source == built.source
        and edge.target == built.target
        and edge.relation == built.relation
        and _iri(edge) == _iri(built)
    )


def _lifted(edge: Edge) -> tuple[str, str | None, str | None] | None:
    """``(aspect, aspect type, domain type)`` when *edge* has an aspect's shape."""
    if edge.target == SOURCE_RECORD and _same_shape(
        edge, derived_from_edge(edge.source)
    ):
        return PROVENANCE, SOURCE_RECORD, edge.source
    if (
        edge.source == SOURCE_RECORD
        and edge.relation == MENTIONS
        and _same_shape(edge, mentions_edge(edge.target))
    ):
        return PROVENANCE, SOURCE_RECORD, edge.target
    if (
        edge.source == ASSERTION
        and edge.relation == SUBJECT
        and _same_shape(edge, subject_edge(edge.target))
    ) or (
        edge.source == ASSERTION
        and edge.relation == OBJECT
        and _same_shape(edge, object_edge(edge.target))
    ):
        return PROVENANCE, ASSERTION, edge.target
    if _same_shape(edge, attributed_to_edge()):
        return PROVENANCE, SOURCE_RECORD, None
    if edge.source == state_type_name(edge.target) and _same_shape(
        edge, specialization_edge(edge.target)
    ):
        return STATE, edge.source, edge.target
    if edge.source == observation_type_name(edge.target) and _same_shape(
        edge, feature_of_interest_edge(edge.target)
    ):
        return STATE, edge.source, edge.target
    return None


def aspect_members(schema: Schema) -> AspectMembers:
    """The types and relations *schema* owes to the provenance and state aspects.

    Args:
        schema: A schema, lifted or not.

    Returns:
        The aspects' own types, the relations they lifted, and the other types
        each was lifted onto. All empty for a schema no aspect was lifted onto.
    """
    core = schema.core_schema
    vertices = {v.name: v for v in core.vertex_config.vertices}
    types: dict[str, str] = {}
    for built in (source_record_vertex(), agent_vertex(), assertion_vertex()):
        found = vertices.get(built.name)
        if found is not None and _iri(found) == _iri(built):
            types[built.name] = PROVENANCE

    edges: list[AspectEdge] = []
    onto: dict[str, set[str]] = {}
    for edge in core.edge_config.edges:
        lifted = _lifted(edge)
        if lifted is None or edge.relation is None:
            continue
        aspect, own, domain = lifted
        edges.append(
            AspectEdge(
                source=edge.source,
                relation=edge.relation,
                target=edge.target,
                aspect=aspect,
            )
        )
        if own is not None and own in vertices:
            types.setdefault(own, aspect)
        if aspect == PROVENANCE and AGENT in vertices and edge.target == AGENT:
            types.setdefault(AGENT, aspect)
        if domain is not None and types.get(domain) != aspect:
            onto.setdefault(domain, set()).add(aspect)
    return AspectMembers(
        types=types,
        edges=edges,
        lifted_onto={name: sorted(aspects) for name, aspects in sorted(onto.items())},
    )


__all__ = ["AspectEdge", "AspectMembers", "aspect_members"]
