from __future__ import annotations

from graflo.architecture.schema.edge import Edge


def test_edge_directed_defaults_true() -> None:
    edge = Edge.model_validate({"source": "a", "target": "b"})
    assert edge.directed is True


def test_edge_directed_false_round_trip() -> None:
    edge = Edge.model_validate({"source": "a", "target": "b", "directed": False})
    payload = edge.to_dict()
    restored = Edge.from_dict(payload)
    assert restored.directed is False
