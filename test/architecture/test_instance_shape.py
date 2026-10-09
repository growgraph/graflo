"""instance_json_schema: one flat, closed JSON Schema for an extraction."""

import copy
import json

import jsonschema
import pytest

from graflo.architecture.schema.context import instance_json_schema
from graflo.architecture.schema.document import Schema

VALID = {
    "vertices": [
        {
            "ref": "v1",
            "type": "Person",
            "quote": "Ann Lee (ann@x.com)",
            "mention": False,
            "props": [
                {"name": "email", "value": "ann@x.com", "quote": "ann@x.com"},
                {"name": "tags", "value": ["ops", "lead"], "quote": "ops lead"},
            ],
        },
        {
            "ref": "v2",
            "type": "Company",
            "quote": "Acme",
            "mention": True,
            "props": [],
        },
    ],
    "edges": [
        {
            "relation": "WORKS_AT",
            "from": "v1",
            "to": "v2",
            "quote": "Ann joined Acme",
            "props": [{"name": "since", "value": "2024-03", "quote": "2024-03"}],
        }
    ],
    "unmapped": [{"quote": "the north conveyor", "note": "no conveyor type"}],
}


def _objects(node):
    if isinstance(node, dict):
        if node.get("type") == "object":
            yield node
        for value in node.values():
            yield from _objects(value)
    elif isinstance(node, list):
        for value in node:
            yield from _objects(value)


def test_every_object_is_closed_and_fully_required(extraction_schema):
    shape = instance_json_schema(extraction_schema)
    objects = list(_objects(shape))
    assert len(objects) >= 6
    for obj in objects:
        assert obj["additionalProperties"] is False
        assert obj["required"] == list(obj["properties"])


def test_is_a_valid_json_schema(extraction_schema):
    jsonschema.Draft202012Validator.check_schema(
        instance_json_schema(extraction_schema)
    )


def test_accepts_a_valid_instance(extraction_schema):
    jsonschema.validate(VALID, instance_json_schema(extraction_schema))


def _mutated(path, value):
    doc = copy.deepcopy(VALID)
    target = doc
    for key in path[:-1]:
        target = target[key]
    if value is KeyError:
        del target[path[-1]]
    else:
        target[path[-1]] = value
    return doc


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("vertices", 0, "type"), "Robot"),
        (("edges", 0, "relation"), "OWNS"),
        (("vertices", 0, "props", 0, "name"), "since"),
        (("edges", 0, "props", 0, "name"), "email"),
        (("vertices", 0, "extra"), 1),
        (("vertices", 0, "quote"), KeyError),
        (("vertices", 0, "props", 0, "value"), {"nested": 1}),
        (("vertices", 0, "props", 0, "value"), [["nested"]]),
        (("unmapped",), KeyError),
    ],
)
def test_rejects_malformed_instances(extraction_schema, path, value):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            _mutated(path, value), instance_json_schema(extraction_schema)
        )


def test_minted_keys_are_not_property_names(extraction_schema):
    shape = instance_json_schema(extraction_schema)
    vertex = shape["properties"]["vertices"]["items"]
    names = vertex["properties"]["props"]["items"]["properties"]["name"]["enum"]
    assert "id" not in names
    assert names == sorted(names)


def test_null_relation_only_with_a_relationless_edge(extraction_schema, context_schema):
    def relation(schema):
        edge = instance_json_schema(schema)["properties"]["edges"]["items"]
        return edge["properties"]["relation"]

    assert None in relation(extraction_schema)["enum"]
    data = extraction_schema.to_dict()
    data["core_schema"]["edge_config"]["edges"] = [
        edge
        for edge in data["core_schema"]["edge_config"]["edges"]
        if edge.get("relation")
    ]
    without = relation(Schema.model_validate(data))
    assert without["type"] == "string"
    assert None not in without["enum"]


def test_edgeless_schema_bounds_edges_to_zero(extraction_schema):
    data = extraction_schema.to_dict()
    data["core_schema"]["edge_config"]["edges"] = []
    shape = instance_json_schema(Schema.model_validate(data))
    edges = shape["properties"]["edges"]
    assert edges["maxItems"] == 0
    assert "enum" not in edges["items"]["properties"]["relation"]
    jsonschema.validate({**VALID, "edges": []}, shape)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(VALID, shape)


def test_is_deterministic(extraction_schema):
    first = json.dumps(instance_json_schema(extraction_schema))
    assert json.dumps(instance_json_schema(extraction_schema)) == first
