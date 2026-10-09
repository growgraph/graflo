"""Instance shape: the JSON Schema of one flat extraction against a schema.

What a language model returns when asked to read a text into *schema*: a flat
list of vertices, a flat list of edges, and an ``unmapped`` list for what it
saw but could not type. Vertex types, relations and property names are enums
taken from the schema. Properties are name/value pairs, edge endpoints are
``ref`` strings pointing at vertices of the same instance, and every assertion
carries the ``quote`` it was read from — offsets are left to the caller.

Every object is closed and lists all its keys as required, so the one JSON
Schema is valid for OpenAI strict ``json_schema`` output, as a ``json_object``
contract, and as an Ollama ``format``. The schema bounds *shape* only: which
property belongs to which type, and whether a value fits that property's type,
are for the caller to check after decoding.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from graflo.architecture.schema.context.element_text import extractable_properties

if TYPE_CHECKING:
    from graflo.architecture.schema.document import Schema

_SCALARS: list[dict[str, Any]] = [
    {"type": "string"},
    {"type": "number"},
    {"type": "boolean"},
]


def _closed(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def _string_enum(values: list[str]) -> dict[str, Any]:
    if not values:
        return {"type": "string"}
    return {"type": "string", "enum": values}


def _array(items: dict[str, Any], *, empty: bool = False) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "array", "items": items}
    if empty:
        schema["maxItems"] = 0
    return schema


def _props(names: list[str]) -> dict[str, Any]:
    value = {
        "anyOf": [
            *(dict(scalar) for scalar in _SCALARS),
            {"type": "array", "items": {"anyOf": [dict(s) for s in _SCALARS]}},
        ]
    }
    prop = _closed(
        {"name": _string_enum(names), "value": value, "quote": {"type": "string"}}
    )
    return _array(prop, empty=not names)


def instance_json_schema(schema: Schema) -> dict[str, Any]:
    """Build the flat extraction contract for *schema*.

    - ``vertices``: ``{ref, type, quote, mention, props}``. ``ref`` is the
      instance-local handle edges point at; ``mention`` marks a reference to an
      existing vertex rather than a new one.
    - ``edges``: ``{relation, from, to, quote, props}``, ``from`` and ``to``
      being vertex refs. ``relation`` admits ``null`` only when the schema
      declares a relation-less edge.
    - ``props``: ``{name, value, quote}``; a value is a scalar or a list of
      scalars. Vertex and edge properties have separate name enums. Minted key
      properties are not offered.
    - ``unmapped``: ``{quote, note}``.

    A list whose vocabulary is empty (no edges, no edge properties) is bounded
    to zero items rather than given an empty ``enum``. Enums are sorted and
    keys are in a fixed order, so the output is deterministic.
    """
    core = schema.core_schema
    vertices = core.vertex_config.vertices
    edges = list(core.edge_config.values())

    vertex_types = sorted(vertex.name for vertex in vertices)
    vertex_props = sorted(
        {field.name for vertex in vertices for field in extractable_properties(vertex)}
    )
    relations = sorted({edge.relation for edge in edges if edge.relation is not None})
    edge_props = sorted({field.name for edge in edges for field in edge.properties})

    relation: dict[str, Any]
    if any(edge.relation is None for edge in edges):
        relation = {"type": ["string", "null"], "enum": [*relations, None]}
    else:
        relation = _string_enum(relations)

    vertex = _closed(
        {
            "ref": {"type": "string"},
            "type": _string_enum(vertex_types),
            "quote": {"type": "string"},
            "mention": {"type": "boolean"},
            "props": _props(vertex_props),
        }
    )
    edge = _closed(
        {
            "relation": relation,
            "from": {"type": "string"},
            "to": {"type": "string"},
            "quote": {"type": "string"},
            "props": _props(edge_props),
        }
    )
    unmapped = _closed({"quote": {"type": "string"}, "note": {"type": "string"}})
    return _closed(
        {
            "vertices": _array(vertex, empty=not vertex_types),
            "edges": _array(edge, empty=not edges),
            "unmapped": _array(unmapped),
        }
    )
