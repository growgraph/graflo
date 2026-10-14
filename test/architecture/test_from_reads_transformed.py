"""An explicit ``from`` reads a transform's output even after another step took it.

A vertex step consumes the transform outputs named like its properties, so one
output does not land on several vertices. A later step that names the field in
its ``from`` must still see it, or its lookup is empty and every edge to it is
dropped without a word.
"""

from __future__ import annotations

from typing import Any

from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.pipeline.runtime.resource import build_resource_runtime
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig

VERTICES = VertexConfig.model_validate(
    {
        "vertices": [
            # The order keeps its reference columns as properties of its own.
            {
                "name": "Order",
                "properties": ["orderNo", "itemCode"],
                "identity": ["orderNo"],
            },
            {"name": "Product", "properties": ["sku"], "identity": ["sku"]},
        ]
    }
)
EDGES = EdgeConfig.model_validate(
    {"edges": [{"source": "Order", "target": "Product", "relation": "contains"}]}
)
RENAME = {"transform": {"rename": {"order_no": "orderNo", "item_code": "itemCode"}}}
LOOKUP = {"vertex": "Product", "from": {"sku": "itemCode"}, "lookup_only": True}
EDGE = {"edge": {"source": "Order", "target": "Product", "relation": "contains"}}


def _run(pipeline: list[dict[str, Any]]) -> dict[Any, Any]:
    runtime = build_resource_runtime(
        ResourceConfig.model_validate({"name": "orders", "pipeline": pipeline}),
        VERTICES,
        EDGES,
        strict_references=True,
    )
    return dict(runtime({"order_no": 1, "item_code": "LAP-13"}))


def test_a_lookup_after_the_step_that_took_its_field_still_finds_it() -> None:
    out = _run([RENAME, {"vertex": "Order"}, LOOKUP, EDGE])

    assert out[("Order", "Product", "contains")] == [
        ({"orderNo": 1}, {"sku": "LAP-13"}, {})
    ]
    assert out["Order"] == [{"orderNo": 1, "itemCode": "LAP-13"}]


def test_the_order_of_the_steps_does_not_decide_the_edge() -> None:
    before = _run([RENAME, LOOKUP, {"vertex": "Order"}, EDGE])
    after = _run([RENAME, {"vertex": "Order"}, LOOKUP, EDGE])

    assert (
        before[("Order", "Product", "contains")]
        == after[("Order", "Product", "contains")]
    )
