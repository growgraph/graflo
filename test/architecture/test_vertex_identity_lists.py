"""A list under an identity field names several vertices."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.ingestion.resource import Resource
from graflo.architecture.pipeline.runtime.actor.vertex import explode_identity_lists
from graflo.architecture.pipeline.runtime.resource import build_resource_runtime
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


def test_a_document_without_a_list_is_returned_as_is() -> None:
    doc = {"id": "a", "tags": ["x", "y"]}
    assert explode_identity_lists(doc, ["id"]) == [doc]


def test_each_element_becomes_a_document() -> None:
    assert explode_identity_lists({"id": ["a", "b"], "kind": "k"}, ["id"]) == [
        {"id": "a", "kind": "k"},
        {"id": "b", "kind": "k"},
    ]


def test_missing_elements_name_no_vertex() -> None:
    assert explode_identity_lists({"id": ["a", None]}, ["id"]) == [{"id": "a"}]
    assert explode_identity_lists({"id": []}, ["id"]) == []


def test_two_list_valued_identity_fields_are_refused() -> None:
    with pytest.raises(ValueError, match=r"\['a', 'b'\]"):
        explode_identity_lists({"a": [1, 2], "b": [3, 4]}, ["a", "b"])


def test_a_vertex_step_maps_a_list_field_to_one_vertex_per_element() -> None:
    vertex_config = VertexConfig.from_dict(
        {
            "vertices": [
                {"name": "author", "properties": ["id"], "identity": ["id"]},
                {"name": "paper", "properties": ["id"], "identity": ["id"]},
            ]
        }
    )
    edge_config = EdgeConfig.from_dict(
        {"edges": [{"source": "author", "target": "paper", "relation": "wrote"}]}
    )
    runtime = build_resource_runtime(
        Resource.from_dict(
            {
                "name": "authors",
                "pipeline": [
                    {"vertex": "author"},
                    {
                        "vertex": "paper",
                        "from": {"id": "papers"},
                        "extraction_scope": "mapped_only",
                    },
                ],
            }
        ),
        vertex_config,
        edge_config,
        {},
    )

    entities = runtime({"id": "a1", "papers": ["p1", "p2", "p1"]})

    assert [v["id"] for v in entities["paper"]] == ["p1", "p2"]
    assert [
        (source["id"], target["id"])
        for source, target, _ in entities["author", "paper", "wrote"]
    ] == [("a1", "p1"), ("a1", "p2")]
