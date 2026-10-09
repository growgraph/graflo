"""Type sheet: a schema as a compact, closed-world vocabulary for a language model.

One line per vertex type and per edge, each followed by its typed properties.
The output is byte-deterministic — the same schema always renders the same
bytes, whatever order its elements were declared in — so a prompt built around
it keeps hitting response and prefix caches.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from graflo.architecture.schema.context.element_text import (
    edge_pattern,
    extractable_properties,
    field_unit,
    identity_label,
)
from graflo.architecture.schema.context.graph import edge_sort_key
from graflo.architecture.schema.vertex import format_field_type_label

if TYPE_CHECKING:
    from graflo.architecture.schema.document import Schema
    from graflo.architecture.schema.edge import Edge
    from graflo.architecture.schema.semantics import Semantics
    from graflo.architecture.schema.vertex import Field, Vertex

_HEADER = """\
Every type available to you, one per line, each followed by its properties.
  Vertex: `Name  "description"  ~ other names  id: key properties`.
    `id: hash(a, b)` derives the key from those properties;
    `id: hash(first of: a | b, c)` from the first complete group;
    `id: assigned` and `id: none` mean no key is taken from the text.
    `alt id:` is another property set that also identifies the vertex.
  Edge: `(Source)-[RELATION]->(Target)`; `-[R]-` is undirected, `-[]->` has no relation name.
  Property: `name TYPE [unit]`; a value with a unit is written with that unit."""

_FOOTER = (
    "Use these types, relations and properties only. "
    "One absent from this sheet does not exist."
)

_PROPERTY_INDENT = "    "


def _clean(text: str | None) -> str:
    return " ".join(text.split()) if text else ""


def _synonyms(semantics: Semantics | None) -> str:
    if semantics is None or not semantics.synonyms:
        return ""
    return "~ " + "; ".join(_clean(s) for s in semantics.synonyms)


def _join(parts: list[str]) -> str:
    return "  ".join(part for part in parts if part)


def _quoted(text: str | None) -> str:
    cleaned = _clean(text)
    return f'"{cleaned}"' if cleaned else ""


def _property(field: Field) -> str:
    parts = [field.name]
    if field.type is not None:
        parts.append(format_field_type_label(field))
    unit = field_unit(field)
    if unit:
        parts.append(f"[{unit}]")
    text = " ".join(parts)
    extras = _join([_quoted(field.description), _synonyms(field.semantics)])
    return f"{text} {extras}" if extras else text


def _properties_line(fields: list[Field]) -> list[str]:
    if not fields:
        return []
    return [_PROPERTY_INDENT + " · ".join(_property(field) for field in fields)]


def _vertex_lines(vertex: Vertex) -> list[str]:
    head = _join(
        [
            vertex.name,
            _quoted(vertex.description),
            _synonyms(vertex.semantics),
            f"id: {identity_label(vertex)}",
            *(
                f"alt id: {', '.join(secondary.fields)}"
                for secondary in vertex.secondary_identities
            ),
        ]
    )
    return [head, *_properties_line(extractable_properties(vertex))]


def _edge_lines(edge: Edge) -> list[str]:
    head = _join(
        [edge_pattern(edge), _quoted(edge.description), _synonyms(edge.semantics)]
    )
    return [head, *_properties_line(edge.properties)]


def render_type_sheet(schema: Schema) -> str:
    """Render *schema* as a type sheet.

    The notation legend comes first, then vertices sorted by name, then edges
    sorted by ``(source, target, relation)``. Properties keep their declared
    order; minted key properties are left out, since no text supplies them.
    The sheet ends with the closed-world sentence: a type, relation or
    property absent from it does not exist.
    """
    core = schema.core_schema
    lines = [_HEADER, "", "## Vertices"]
    for vertex in sorted(core.vertex_config.vertices, key=lambda v: v.name):
        lines.extend(_vertex_lines(vertex))
    edges = sorted(core.edge_config.values(), key=lambda e: edge_sort_key(e.edge_id))
    if edges:
        lines.extend(["", "## Edges"])
        for edge in edges:
            lines.extend(_edge_lines(edge))
    lines.extend(["", _FOOTER])
    return "\n".join(lines) + "\n"
