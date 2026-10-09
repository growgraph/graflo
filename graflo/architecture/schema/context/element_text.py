"""One text rendering per schema element, for lexical and dense retrieval alike.

Every index over schema elements — a search index, an embedding table, an
in-process lexical lane — must render an element the same way, or a query built
for one misses in another. This module is that rendering: one row per vertex,
per edge, and per property, each with its surface forms split out for lexical
matching and its text ready to embed.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Final, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.identifiers import EdgeId
from graflo.architecture.schema.context.graph import edge_sort_key
from graflo.architecture.schema.semantics import Semantics
from graflo.architecture.schema.vertex import format_field_type_label

if TYPE_CHECKING:
    from graflo.architecture.schema.document import Schema
    from graflo.architecture.schema.edge import Edge
    from graflo.architecture.schema.vertex import Field, Vertex

#: Version of the rendering. Anything fingerprinting element text (an embedding
#: contract, a persisted index) must include it; any change to the output bumps it.
ELEMENT_TEXT_VERSION: Final[int] = 1

ElementKind = Literal["vertex", "edge", "property"]


class ElementText(ConfigBaseModel):
    """One schema element, rendered for retrieval."""

    kind: ElementKind = PydanticField(..., description="Element kind.")
    name: str = PydanticField(
        ...,
        description=(
            "Vertex name, edge relation (``source_target`` when relation-less), "
            "or property name."
        ),
    )
    vertex: str | None = PydanticField(
        default=None, description="Owning vertex of a vertex property."
    )
    edge_id: EdgeId | None = PydanticField(
        default=None, description="The edge, or the owning edge of an edge property."
    )
    terms: list[str] = PydanticField(
        default_factory=list,
        description="Surface forms a text may use: the name, then synonyms.",
    )
    anchors: list[str] = PydanticField(
        default_factory=list, description="``iri`` then ``exact_match`` IRIs."
    )
    unit: str | None = PydanticField(default=None, description="Declared unit.")
    text: str = PydanticField(..., description="Rendered text.")


def _clean(text: str | None) -> str:
    return " ".join(text.split()) if text else ""


def _dedup(values: list[str]) -> list[str]:
    return [value for value in dict.fromkeys(values) if value]


def _terms(name: str, semantics: Semantics | None) -> list[str]:
    return _dedup([name, *(semantics.synonyms if semantics else [])])


def _anchors(semantics: Semantics | None) -> list[str]:
    if semantics is None:
        return []
    return _dedup([semantics.iri or "", *semantics.exact_match])


def _semantic_parts(semantics: Semantics | None) -> list[str]:
    parts: list[str] = []
    if semantics is None:
        return parts
    if semantics.synonyms:
        parts.append(f"synonyms: {'; '.join(semantics.synonyms)}")
    if semantics.iri:
        parts.append(f"iri: {semantics.iri}")
    if semantics.exact_match:
        parts.append(f"exact_match: {'; '.join(semantics.exact_match)}")
    return parts


def field_unit(field: Field) -> str | None:
    """The field's declared unit, if any."""
    return field.semantics.unit if field.semantics is not None else None


def field_label(field: Field) -> str:
    """``name (TYPE, unit) "description"``, parts omitted when absent."""
    type_parts = [format_field_type_label(field)] if field.type is not None else []
    unit = field_unit(field)
    if unit:
        type_parts.append(unit)
    label = field.name
    if type_parts:
        label += f" ({', '.join(type_parts)})"
    description = _clean(field.description)
    if description:
        label += f' "{description}"'
    return label


def minted_key_fields(vertex: Vertex) -> set[str]:
    """Key properties minted or derived by graflo, never read from a document.

    Empty for ``natural`` identity; the synthetic key of an ``assigned``,
    ``blank``, ``hash`` or funnel vertex otherwise.
    """
    if vertex.identity_mode == "natural":
        return set()
    return set(vertex.identity) - set(vertex.digest_source_fields)


def extractable_properties(vertex: Vertex) -> list[Field]:
    """The vertex's properties a document can supply, in declared order."""
    minted = minted_key_fields(vertex)
    return [field for field in vertex.properties if field.name not in minted]


def identity_label(vertex: Vertex) -> str:
    """How the vertex is keyed: ``a, b`` · ``hash(a, b)`` · ``hash(first of: a | b, c)`` · ``assigned`` · ``none``."""
    mode = vertex.identity_mode
    if mode == "blank":
        return "none"
    if mode == "assigned":
        return "assigned"
    if vertex.identity_funnel is not None:
        branches = " | ".join(
            ", ".join(branch.fields) for branch in vertex.identity_funnel.branches
        )
        return f"hash(first of: {branches})"
    if mode == "hash":
        return f"hash({', '.join(vertex.hash_identity_properties)})"
    return ", ".join(vertex.identity)


def edge_pattern(edge: Edge) -> str:
    """``(Source)-[RELATION]->(Target)``; ``-[R]-`` when undirected, ``[]`` when relation-less."""
    arrow = "->" if edge.directed else "-"
    return f"({edge.source})-[{edge.relation or ''}]{arrow}({edge.target})"


def edge_name(edge: Edge) -> str:
    """The edge's relation, or ``source_target`` when it has none."""
    return edge.relation or f"{edge.source}_{edge.target}"


def vertex_text(vertex: Vertex) -> str:
    """Render a vertex: name, description, anchors, typed fields, identity."""
    parts = [f"Vertex | name: {vertex.name}"]
    description = _clean(vertex.description)
    if description:
        parts.append(f'description: "{description}"')
    parts.extend(_semantic_parts(vertex.semantics))
    fields = ", ".join(field_label(field) for field in extractable_properties(vertex))
    parts.append(f"fields: {fields or 'none'}")
    parts.append(f"identity: {identity_label(vertex)}")
    for secondary in vertex.secondary_identities:
        parts.append(f"alt identity: {', '.join(secondary.fields)}")
    return " | ".join(parts)


def edge_text(edge: Edge) -> str:
    """Render an edge: relation, endpoints, direction, description, anchors, properties."""
    parts = [
        f"Edge | relation: {edge.relation or 'none'}",
        f"source: {edge.source}",
        f"target: {edge.target}",
    ]
    if not edge.directed:
        parts.append("undirected")
    description = _clean(edge.description)
    if description:
        parts.append(f'description: "{description}"')
    parts.extend(_semantic_parts(edge.semantics))
    if edge.properties:
        parts.append(
            f"properties: {', '.join(field_label(field) for field in edge.properties)}"
        )
    return " | ".join(parts)


def field_text(field: Field, owner: str) -> str:
    """Render a property of *owner* (a vertex name, or an edge pattern)."""
    parts = [f"Property | name: {field.name}", f"of: {owner}"]
    if field.type is not None:
        parts.append(f"type: {format_field_type_label(field)}")
    unit = field_unit(field)
    if unit:
        parts.append(f"unit: {unit}")
    description = _clean(field.description)
    if description:
        parts.append(f'description: "{description}"')
    parts.extend(_semantic_parts(field.semantics))
    return " | ".join(parts)


def _property_rows(
    fields: list[Field],
    owner: str,
    *,
    vertex: str | None = None,
    edge_id: EdgeId | None = None,
) -> Iterator[ElementText]:
    for field in fields:
        yield ElementText(
            kind="property",
            name=field.name,
            vertex=vertex,
            edge_id=edge_id,
            terms=_terms(field.name, field.semantics),
            anchors=_anchors(field.semantics),
            unit=field_unit(field),
            text=field_text(field, owner),
        )


def iter_elements(schema: Schema, *, properties: bool = True) -> Iterator[ElementText]:
    """Yield every element of *schema* in a deterministic order.

    Vertices by name, then edges by ``(source, target, relation)``; with
    *properties*, each owner's property rows follow it in declared order.
    Minted key properties (:func:`minted_key_fields`) are left out throughout.
    """
    core = schema.core_schema
    for vertex in sorted(core.vertex_config.vertices, key=lambda v: v.name):
        yield ElementText(
            kind="vertex",
            name=vertex.name,
            terms=_terms(vertex.name, vertex.semantics),
            anchors=_anchors(vertex.semantics),
            text=vertex_text(vertex),
        )
        if properties:
            yield from _property_rows(
                extractable_properties(vertex), vertex.name, vertex=vertex.name
            )
    for edge in sorted(
        core.edge_config.values(), key=lambda e: edge_sort_key(e.edge_id)
    ):
        name = edge_name(edge)
        yield ElementText(
            kind="edge",
            name=name,
            edge_id=edge.edge_id,
            terms=_terms(name, edge.semantics),
            anchors=_anchors(edge.semantics),
            text=edge_text(edge),
        )
        if properties:
            yield from _property_rows(
                edge.properties, edge_pattern(edge), edge_id=edge.edge_id
            )
