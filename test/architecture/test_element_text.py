"""Element text: one rendering per vertex, edge and property for retrieval."""

from graflo.architecture.schema.context import (
    ElementText,
    iter_elements,
    minted_key_fields,
)


def _by_key(schema) -> dict[tuple, ElementText]:
    return {
        (row.kind, row.vertex or row.edge_id, row.name): row
        for row in iter_elements(schema)
    }


def test_vertex_row_carries_semantics(extraction_schema):
    row = _by_key(extraction_schema)[("vertex", None, "Person")]
    assert row.terms == ["Person", "employee", "staff"]
    assert row.anchors == ["https://schema.org/Person"]
    assert 'description: "A human actor."' in row.text
    assert "synonyms: employee; staff" in row.text
    assert "iri: https://schema.org/Person" in row.text
    assert "identity: email" in row.text
    assert "alt identity: phone" in row.text
    assert "tags (LIST<STRING>)" in row.text


def test_property_row_names_its_owner_and_unit(extraction_schema):
    row = _by_key(extraction_schema)[("property", "Substation", "voltage")]
    assert row.unit == "kV"
    assert row.terms == ["voltage", "rating"]
    assert row.text == (
        "Property | name: voltage | of: Substation | type: FLOAT | unit: kV"
        " | synonyms: rating"
    )


def test_edge_property_row_is_owned_by_the_edge(extraction_schema):
    edge_id = ("Person", "Company", "WORKS_AT")
    row = _by_key(extraction_schema)[("property", edge_id, "since")]
    assert row.vertex is None
    assert "of: (Person)-[WORKS_AT]->(Company)" in row.text


def test_relationless_edge_is_named_by_its_endpoints(extraction_schema):
    rows = [row for row in iter_elements(extraction_schema) if row.kind == "edge"]
    names = {row.edge_id: row.name for row in rows}
    assert names[("Note", "WorkOrder", None)] == "Note_WorkOrder"


def test_minted_keys_have_no_rows(extraction_schema):
    for vertex in extraction_schema.core_schema.vertex_config.vertices:
        minted = minted_key_fields(vertex)
        if vertex.identity_mode == "natural":
            assert not minted
        else:
            assert minted
    names = {
        (row.vertex, row.name)
        for row in iter_elements(extraction_schema)
        if row.kind == "property"
    }
    assert ("WorkOrder", "id") not in names
    assert ("Note", "id") not in names
    assert ("Substation", "code") in names


def test_order_is_deterministic_and_owner_first(extraction_schema):
    rows = list(iter_elements(extraction_schema))
    vertices = [row.name for row in rows if row.kind == "vertex"]
    assert vertices == sorted(vertices)
    first_edge = next(i for i, row in enumerate(rows) if row.kind == "edge")
    assert all(row.kind != "vertex" for row in rows[first_edge:])
    assert [r.model_dump() for r in rows] == [
        r.model_dump() for r in iter_elements(extraction_schema)
    ]


def test_properties_can_be_left_out(extraction_schema):
    kinds = {row.kind for row in iter_elements(extraction_schema, properties=False)}
    assert kinds == {"vertex", "edge"}
