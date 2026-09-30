"""A vertex weight's ``filter`` selects the documents the weight is read from."""

from __future__ import annotations

from collections import defaultdict

from graflo.architecture.graph_types import LocationIndex, VertexRep, Weight
from graflo.architecture.pipeline.runtime.actor.edge_render import render_weights
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import VertexConfig

VERTEX_CONFIG = VertexConfig.from_dict(
    {
        "vertices": [
            {"name": "person", "properties": ["id"], "identity": ["id"]},
            {
                "name": "badge",
                "properties": ["code", "kind"],
                "identity": ["code"],
            },
        ]
    }
)
EDGE = Edge(source="person", target="person", relation="knows")


def _weights(badges: list[dict], weight: Weight) -> list[dict]:
    acc_vertex: defaultdict = defaultdict(lambda: defaultdict(list))
    acc_vertex["badge"][LocationIndex((0,))] = [VertexRep(vertex=doc) for doc in badges]
    edges: defaultdict = defaultdict(list)
    edges["knows"] = [({"id": 1}, {"id": 2}, {})]
    rendered = render_weights(
        EDGE, VERTEX_CONFIG, acc_vertex, edges, vertex_weights=[weight]
    )
    return [attributes for _, _, attributes in rendered["knows"]]


def test_filter_keeps_the_document_that_matches() -> None:
    weight = Weight(name="badge", fields=["code"], filter={"kind": "main"})

    attributes = _weights(
        [{"code": "b-other", "kind": "spare"}, {"code": "b-main", "kind": "main"}],
        weight,
    )

    assert attributes == [{weight.cfield("code"): "b-main"}]


def test_filter_skips_a_document_without_the_field() -> None:
    weight = Weight(name="badge", fields=["code"], filter={"kind": "main"})

    attributes = _weights([{"code": "b-untyped"}], weight)

    assert attributes == [{}]


def test_no_filter_reads_every_document() -> None:
    weight = Weight(name="badge", fields=["code"])

    attributes = _weights([{"code": "b-1", "kind": "spare"}], weight)

    assert attributes == [{weight.cfield("code"): "b-1"}]
