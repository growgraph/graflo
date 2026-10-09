"""render_type_sheet: a schema as a byte-deterministic closed-world vocabulary."""

from graflo.architecture.schema.context import render_type_sheet
from graflo.architecture.schema.document import Schema

EXPECTED = """\
Every type available to you, one per line, each followed by its properties.
  Vertex: `Name  "description"  ~ other names  id: key properties`.
    `id: hash(a, b)` derives the key from those properties;
    `id: hash(first of: a | b, c)` from the first complete group;
    `id: assigned` and `id: none` mean no key is taken from the text.
    `alt id:` is another property set that also identifies the vertex.
  Edge: `(Source)-[RELATION]->(Target)`; `-[R]-` is undirected, `-[]->` has no relation name.
  Property: `name TYPE [unit]`; a value with a unit is written with that unit.

## Vertices
Company  id: tax_id
    tax_id STRING · title
Machine  id: hash(first of: serial | plate_no)
    serial · plate_no
Note  id: none
    body STRING
Person  "A human actor."  ~ employee; staff  id: email  alt id: phone
    email STRING · phone STRING · name STRING "Full name." · tags LIST<STRING>
Substation  id: hash(code)
    code STRING · voltage FLOAT [kV] ~ rating
WorkOrder  id: assigned
    summary STRING

## Edges
(Company)-[OPERATES]-(Substation)
(Note)-[]->(WorkOrder)
(Person)-[WORKS_AT]->(Company)  "Employment."
    since DATETIME

Use these types, relations and properties only. One absent from this sheet does not exist.
"""


def _reversed(schema: Schema) -> Schema:
    data = schema.to_dict()
    core = data["core_schema"]
    core["vertex_config"]["vertices"].reverse()
    core["edge_config"]["edges"].reverse()
    return Schema.model_validate(data)


def test_renders_the_expected_sheet(extraction_schema):
    assert render_type_sheet(extraction_schema) == EXPECTED


def test_declaration_order_does_not_change_the_bytes(extraction_schema):
    assert render_type_sheet(_reversed(extraction_schema)) == render_type_sheet(
        extraction_schema
    )


def test_minted_keys_are_not_offered(extraction_schema):
    sheet = render_type_sheet(extraction_schema)
    # WorkOrder (assigned), Note (blank) and the hash vertices mint `id`.
    assert " id STRING" not in sheet
    assert "· id" not in sheet
    assert "    id" not in sheet


def test_closed_world_sentence_comes_last(context_schema):
    sheet = render_type_sheet(context_schema)
    assert sheet.rstrip("\n").splitlines()[-1].endswith("does not exist.")


def test_schema_without_edges_has_no_edge_section(extraction_schema):
    data = extraction_schema.to_dict()
    data["core_schema"]["edge_config"]["edges"] = []
    sheet = render_type_sheet(Schema.model_validate(data))
    assert "## Edges" not in sheet
    assert "## Vertices" in sheet
