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


# -- pairing a weight with a record's edges --------------------------------------

VERTEX_CONFIG_TWO = VertexConfig.from_dict(
    {
        "vertices": [
            {"name": "person", "properties": ["id"], "identity": ["id"]},
            {"name": "badge", "properties": ["code"], "identity": ["code"]},
            {"name": "site", "properties": ["sid"], "identity": ["sid"]},
        ]
    }
)


def _render(vertices: dict[str, list[dict]], edge_count: int, weights: list[Weight]):
    acc_vertex: defaultdict = defaultdict(lambda: defaultdict(list))
    for name, docs in vertices.items():
        acc_vertex[name][LocationIndex((0,))] = [VertexRep(vertex=d) for d in docs]
    edges: defaultdict = defaultdict(list)
    edges["knows"] = [({"id": i}, {"id": i + 1}, {}) for i in range(edge_count)]
    rendered = render_weights(
        EDGE, VERTEX_CONFIG_TWO, acc_vertex, edges, vertex_weights=weights
    )
    return [attributes for _, _, attributes in rendered["knows"]]


def test_one_weight_vertex_goes_on_every_edge() -> None:
    weight = Weight(name="badge", fields=["code"])

    attributes = _render({"badge": [{"code": "b1"}]}, 2, [weight])

    assert attributes == [{weight.cfield("code"): "b1"}] * 2


def test_every_weight_entry_goes_on_every_edge() -> None:
    badge = Weight(name="badge", fields=["code"])
    site = Weight(name="site", fields=["sid"])

    attributes = _render(
        {"badge": [{"code": "b1"}], "site": [{"sid": "s1"}]}, 2, [badge, site]
    )

    assert attributes == [{badge.cfield("code"): "b1", site.cfield("sid"): "s1"}] * 2


def test_as_many_weight_vertices_as_edges_pair_by_position() -> None:
    weight = Weight(name="badge", fields=["code"])

    attributes = _render({"badge": [{"code": "b1"}, {"code": "b2"}]}, 2, [weight])

    assert attributes == [{weight.cfield("code"): "b1"}, {weight.cfield("code"): "b2"}]


def test_a_count_mismatch_keeps_every_edge_and_says_so(caplog) -> None:
    weight = Weight(name="badge", fields=["code"])

    attributes = _render({"badge": [{"code": "b1"}, {"code": "b2"}]}, 3, [weight])

    assert attributes == [{weight.cfield("code"): "b1"}] * 3
    assert "badge" in caplog.text
