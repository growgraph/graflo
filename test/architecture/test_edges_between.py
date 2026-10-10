"""``EdgeConfig.ids_between``: the declared edges among a set of vertex types."""

from __future__ import annotations

from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


def _config() -> EdgeConfig:
    return EdgeConfig.from_dict(
        {
            "edges": [
                {"source": "b", "target": "a", "relation": "r1"},
                {"source": "a", "target": "b", "relation": "r2"},
                {"source": "a", "target": "c", "relation": "r3"},
                {"source": "c", "target": "a", "relation": "r4"},
                {"source": "a", "target": "b", "relation": "r5"},
            ]
        }
    )


def test_only_edges_with_both_endpoints_present_are_returned() -> None:
    assert _config().ids_between({"a", "b"}) == [
        ("b", "a", "r1"),
        ("a", "b", "r2"),
        ("a", "b", "r5"),
    ]


def test_edges_come_in_declaration_order() -> None:
    config = _config()

    assert config.ids_between({"a", "b", "c"}) == [eid for eid, _ in config.items()]


def test_an_edge_registered_later_is_found() -> None:
    config = _config()
    vertex_config = VertexConfig.from_dict(
        {"vertices": [{"name": n, "properties": ["id"]} for n in "abcd"]}
    )

    config.update_edges(Edge(source="d", target="a", relation="r6"), vertex_config)

    assert config.ids_between({"a", "d"}) == [("d", "a", "r6")]


def test_reassigning_the_edges_rebuilds_the_lookup() -> None:
    config = _config()

    config.edges = [Edge(source="c", target="b", relation="r7")]

    assert config.ids_between({"a", "b"}) == []
    assert config.ids_between({"b", "c"}) == [("c", "b", "r7")]
