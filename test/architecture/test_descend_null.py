"""A descend step that reaches a null has nothing to run on."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest


def manifest(step: dict[str, Any]) -> GraphManifest:
    built = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "nulls"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "parent",
                                "properties": ["id"],
                                "identity": ["id"],
                            },
                            {
                                "name": "child",
                                "properties": ["id"],
                                "identity": ["id"],
                            },
                        ]
                    },
                    "edge_config": {"edges": [{"source": "parent", "target": "child"}]},
                },
                "db_profile": {},
            },
            "ingestion_model": {
                "resources": [
                    {"name": "records", "pipeline": [{"vertex": "parent"}, step]}
                ],
                "transforms": [],
            },
        }
    )
    built.finish_init()
    return built


#: A transform is what a null used to fail on: it has no document to read.
TO_TEXT = {
    "call": {"module": "builtins", "foo": "str", "input": ["id"], "output": ["id"]}
}


def children(step: dict[str, Any], doc: dict[str, Any]) -> list[dict[str, Any]]:
    model = manifest(step).ingestion_model
    assert model is not None
    runtime = model.fetch_resource("records")
    return list(runtime.cast_document(doc).entities.get("child", []))


@pytest.mark.parametrize(
    "value",
    [None, [None]],
    ids=["null", "list of null"],
)
def test_a_key_that_holds_null_writes_nothing(value: object):
    step = {"key": "child", "pipeline": [{"transform": TO_TEXT}, {"vertex": "child"}]}
    assert children(step, {"id": "p1", "child": value}) == []


def test_a_null_among_values_is_skipped_and_the_others_run():
    step = {"key": "child", "pipeline": [{"transform": TO_TEXT}, {"vertex": "child"}]}
    doc = {"id": "p1", "child": [{"id": "c1"}, None, {"id": "c2"}]}
    assert children(step, doc) == [{"id": "c1"}, {"id": "c2"}]


def test_any_key_skips_a_null_value():
    step = {"any_key": True, "pipeline": [{"transform": TO_TEXT}, {"vertex": "child"}]}
    doc = {"id": "p1", "a": None, "b": {"id": "c1"}}
    assert {"id": "c1"} in children(step, doc)
