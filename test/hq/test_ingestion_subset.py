from __future__ import annotations

from graflo.architecture.graph_types import GraphContainer
from graflo.hq.caster import (
    IngestionParams,
    _filter_graph_container_by_vertices_inplace,
)


def test_filter_graph_container_by_vertices_keeps_allowed_vertices() -> None:
    gc = GraphContainer(
        vertices={
            "A": [{"id": 1}],
            "B": [{"id": 2}],
            "C": [{"id": 3}],
        },
        edges={
            ("A", "B", None): [({"id": 1}, {"id": 2}, {})],
            ("A", "C", None): [({"id": 1}, {"id": 3}, {})],
        },
        linear=[],
    )

    _filter_graph_container_by_vertices_inplace(gc, allowed_vertex_names={"A", "B"})

    assert set(gc.vertices.keys()) == {"A", "B"}
    assert set(gc.edges.keys()) == {("A", "B", None)}


def test_filter_graph_container_by_vertices_empty_ingests_nothing() -> None:
    gc = GraphContainer(
        vertices={
            "A": [{"id": 1}],
            "B": [{"id": 2}],
        },
        edges={
            ("A", "B", None): [({"id": 1}, {"id": 2}, {})],
        },
        linear=[],
    )

    _filter_graph_container_by_vertices_inplace(gc, allowed_vertex_names=set())

    assert gc.vertices == {}
    assert gc.edges == {}


def test_ingestion_params_accepts_resource_and_vertex_filters() -> None:
    params = IngestionParams(resources=["r1"], vertices=["v1"])
    assert params.resources == ["r1"]
    assert params.vertices == ["v1"]
