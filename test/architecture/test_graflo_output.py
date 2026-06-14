"""Tests for GraFloOutput and GraphContainer JSON serialization."""

from __future__ import annotations

import json
from collections import defaultdict

import pytest

from graflo.architecture.graph_types import (
    GraphContainer,
    deserialize_edge_key,
    serialize_edge_key,
)
from graflo.architecture.schema import CoreSchema, GraFloOutput, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig


def test_serialize_deserialize_edge_key() -> None:
    edge_id = ("person", "department", "works_in")
    assert deserialize_edge_key(serialize_edge_key(edge_id)) == edge_id

    null_relation = ("person", "department", None)
    assert deserialize_edge_key(serialize_edge_key(null_relation)) == null_relation


def test_edge_key_json_handles_pipe_in_names() -> None:
    edge_id = ("user|admin", "dept", "works|in")
    serialized = serialize_edge_key(edge_id)
    assert "|" not in serialized or serialized.startswith("[")
    assert deserialize_edge_key(serialized) == edge_id


def test_graph_container_edges_json_roundtrip() -> None:
    gc = GraphContainer(
        vertices={"person": [{"id": "1", "name": "Alice"}]},
        edges={("person", "department", "works_in"): [[{"id": "1"}, {"id": "d1"}, {}]]},
        linear=[],
    )
    payload = gc.model_dump(mode="json")
    json_key = serialize_edge_key(("person", "department", "works_in"))
    assert json_key in payload["edges"]
    restored = GraphContainer.model_validate(payload)
    assert ("person", "department", "works_in") in restored.edges
    assert restored.vertices["person"][0]["name"] == "Alice"


def test_graph_container_linear_json_roundtrip() -> None:
    linear_item: defaultdict = defaultdict(list)
    linear_item["person"] = [{"id": "1"}]
    linear_item[("person", "department", "works_in")] = [
        [{"id": "1"}, {"id": "d1"}, {}]
    ]
    gc = GraphContainer(vertices={}, edges={}, linear=[linear_item])
    payload = gc.model_dump(mode="json")
    json_key = serialize_edge_key(("person", "department", "works_in"))
    assert json_key in payload["linear"][0]
    restored = GraphContainer.model_validate(payload)
    assert ("person", "department", "works_in") in restored.linear[0]


def test_graflo_output_roundtrip() -> None:
    schema = Schema(
        metadata=GraphMetadata(name="demo"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[Field(name="id"), Field(name="name")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(source="person", target="person", relation="knows"),
                ]
            ),
        ),
    )
    data = GraphContainer(
        vertices={"person": [{"id": "1", "name": "Alice"}]},
        edges={("person", "person", "knows"): [[{"id": "1"}, {"id": "2"}, {}]]},
    )
    output = GraFloOutput(graph_schema=schema, data=data)
    payload = output.model_dump(mode="json")
    restored = GraFloOutput.model_validate(payload)
    assert restored.graph_schema.metadata.name == "demo"
    assert restored.core_schema.vertex_config.vertices[0].name == "person"
    assert ("person", "person", "knows") in restored.data.edges


def test_invalid_edge_key_raises() -> None:
    with pytest.raises(ValueError, match="Invalid edge key"):
        deserialize_edge_key("only|two")

    with pytest.raises(ValueError, match="Invalid edge key"):
        deserialize_edge_key(json.dumps(["only", "two"]))
