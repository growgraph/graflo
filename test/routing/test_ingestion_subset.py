from __future__ import annotations

import asyncio

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.resource import Resource
from graflo.architecture.contract.runtime import build_resource_runtime
from graflo.architecture.contract.runtime.resource import resolve_effective_vertex_names
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig
from graflo.hq.document_caster import (
    DocumentCaster,
    filter_graph_container_by_vertices_inplace,
    filter_graph_container_drop_empty_identity_inplace,
)
from graflo.hq.ingestion_parameters import IngestionParams


def _runtime(
    data: dict,
    vertex_config: VertexConfig,
    edge_config: EdgeConfig,
    **kwargs,
):
    return build_resource_runtime(
        Resource.from_dict(data),
        vertex_config,
        edge_config,
        {},
        **kwargs,
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

    filter_graph_container_by_vertices_inplace(gc, allowed_vertex_names={"A", "B"})

    assert set(gc.vertices.keys()) == {"A", "B"}
    assert set(gc.edges.keys()) == {("A", "B", None)}


def test_filter_graph_container_drops_empty_identity_vertices_and_edges() -> None:
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {
                    "name": "modifier",
                    "properties": ["modifier_id", "parent_id"],
                    "identity": ["modifier_id"],
                },
                {
                    "name": "metric",
                    "properties": ["metric_id"],
                    "identity": ["metric_id"],
                },
            ]
        }
    )
    gc = GraphContainer(
        vertices={
            "modifier": [{"modifier_id": 123}, {"parent_id": 456}],
            "metric": [{"metric_id": 981}],
        },
        edges={
            ("modifier", "modifier", "has_parent"): [
                ({"modifier_id": 123}, {"parent_id": 456}, {}),
            ],
            ("modifier", "metric", "has_metric"): [
                ({"modifier_id": 123}, {"metric_id": 981}, {}),
            ],
        },
        linear=[],
    )

    filter_graph_container_drop_empty_identity_inplace(gc, vertex_config=vc)

    assert gc.vertices["modifier"] == [{"modifier_id": 123}]
    assert gc.vertices["metric"] == [{"metric_id": 981}]
    assert set(gc.edges.keys()) == {("modifier", "metric", "has_metric")}
    assert gc.edges[("modifier", "metric", "has_metric")] == [
        ({"modifier_id": 123}, {"metric_id": 981}, {}),
    ]


def test_resolve_effective_vertex_names_returns_none_for_dynamic_resource_without_subset() -> (
    None
):
    assert resolve_effective_vertex_names(set(), allowed_vertex_names=None) is None


def test_resolve_effective_vertex_names_uses_allowed_when_resource_names_empty() -> (
    None
):
    assert resolve_effective_vertex_names(set(), allowed_vertex_names={"A", "B"}) == {
        "A",
        "B",
    }


def test_resolve_effective_vertex_names_intersects_static_resource_names() -> None:
    assert resolve_effective_vertex_names({"A", "B"}, allowed_vertex_names={"A"}) == {
        "A"
    }
    assert resolve_effective_vertex_names({"A", "B"}, allowed_vertex_names=None) == {
        "A",
        "B",
    }


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

    filter_graph_container_by_vertices_inplace(gc, allowed_vertex_names=set())

    assert gc.vertices == {}
    assert gc.edges == {}


def test_ingestion_params_accepts_resource_and_vertex_filters() -> None:
    params = IngestionParams(resources=["r1"], vertices=["v1"])
    assert params.resources == ["r1"]
    assert params.vertices == ["v1"]


def test_ingestion_params_accepts_connectors_filter() -> None:
    params = IngestionParams(
        resources=["r1"], connectors=["users_api"], vertices=["v1"]
    )
    assert params.connectors == ["users_api"]


def _vertex_config_a_b_c() -> VertexConfig:
    return VertexConfig.from_dict(
        {
            "vertices": [
                {"name": "A", "properties": ["id"], "identity": ["id"]},
                {"name": "B", "properties": ["id"], "identity": ["id"]},
                {"name": "C", "properties": ["id"], "identity": ["id"]},
            ]
        }
    )


def _edge_config_a_b_and_a_c() -> EdgeConfig:
    return EdgeConfig.from_dict(
        {"edges": [{"source": "A", "target": "B"}, {"source": "A", "target": "C"}]}
    )


def test_vertex_actor_early_exit_skips_disallowed_vertices() -> None:
    vc = _vertex_config_a_b_c()
    ec = EdgeConfig.from_dict({"edges": []})

    resource = _runtime(
        {
            "name": "r",
            "pipeline": [
                {"vertex": "A", "from": {"id": "a_id"}},
                {"vertex": "B", "from": {"id": "b_id"}},
            ],
        },
        vc,
        ec,
        allowed_vertex_names={"A"},
    )

    extraction_ctx = resource._executor.extract({"a_id": "a1", "b_id": "b1"})
    assert "A" in extraction_ctx.acc_vertex
    assert "B" not in extraction_ctx.acc_vertex


def _vertex_router_only_type_field_resource() -> dict:
    return {
        "name": "r",
        "pipeline": [
            {
                "vertex_router": {
                    "type_field": "vtype",
                    "from": {"id": "id"},
                }
            }
        ],
    }


def test_document_caster_preserves_vertex_router_without_static_names() -> None:
    """Post-cast filtering must not drop dynamically routed vertices."""
    vc = _vertex_config_a_b_c()
    ec = EdgeConfig.from_dict({"edges": []})
    core = CoreSchema(vertex_config=vc, edge_config=ec)
    ingestion_model = IngestionModel.model_validate(
        {"resources": [_vertex_router_only_type_field_resource()]}
    )
    ingestion_model.finish_init(core)

    caster = DocumentCaster(ingestion_model)
    result = asyncio.run(
        caster.cast_batch(
            [{"vtype": "A", "id": "a1"}, {"vtype": "B", "id": "b1"}],
            "r",
            params=IngestionParams(),
        )
    )

    assert set(result.graph.vertices.keys()) == {"A", "B"}
    assert result.graph.vertices["A"] == [{"id": "a1"}]
    assert result.graph.vertices["B"] == [{"id": "b1"}]


def test_document_caster_vertex_router_respects_allowed_vertices_without_static_names() -> (
    None
):
    vc = _vertex_config_a_b_c()
    ec = EdgeConfig.from_dict({"edges": []})
    core = CoreSchema(vertex_config=vc, edge_config=ec)
    ingestion_model = IngestionModel.model_validate(
        {"resources": [_vertex_router_only_type_field_resource()]}
    )
    ingestion_model.finish_init(core, allowed_vertex_names={"A"})

    caster = DocumentCaster(ingestion_model)
    result = asyncio.run(
        caster.cast_batch(
            [{"vtype": "A", "id": "a1"}, {"vtype": "B", "id": "b1"}],
            "r",
            params=IngestionParams(),
            allowed_vertex_names={"A"},
        )
    )

    assert set(result.graph.vertices.keys()) == {"A"}
    assert result.graph.vertices["A"] == [{"id": "a1"}]


def test_vertex_router_early_exit_skips_disallowed_types() -> None:
    vc = _vertex_config_a_b_c()
    ec = EdgeConfig.from_dict({"edges": []})

    resource = _runtime(
        {
            "name": "r",
            "pipeline": [
                {
                    "vertex_router": {
                        "type_field": "vtype",
                        "vertex_from_map": {
                            "A": {"id": "id"},
                            "B": {"id": "id"},
                        },
                    }
                }
            ],
        },
        vc,
        ec,
        allowed_vertex_names={"A"},
    )

    extraction_ctx = resource._executor.extract({"vtype": "B", "id": "b1"})
    assert "A" not in extraction_ctx.acc_vertex
    assert "B" not in extraction_ctx.acc_vertex

    extraction_ctx = resource._executor.extract({"vtype": "A", "id": "a1"})
    assert "A" in extraction_ctx.acc_vertex
    assert "B" not in extraction_ctx.acc_vertex


def test_dynamic_edge_early_exit_skips_disallowed_endpoints() -> None:
    """Dynamic EdgeActor skips rows where a resolved type is not in allowed_vertex_names."""
    vc = _vertex_config_a_b_c()
    ec = EdgeConfig.from_dict({"edges": []})

    resource = _runtime(
        {
            "name": "r",
            "pipeline": [
                {
                    "vertex_router": {
                        "type_field": "src_type",
                        "from": {"id": "src_id"},
                    }
                },
                {
                    "vertex_router": {
                        "type_field": "tgt_type",
                        "from": {"id": "tgt_id"},
                    }
                },
                {
                    "edge": {
                        "source_type_field": "src_type",
                        "target_type_field": "tgt_type",
                    }
                },
            ],
        },
        vc,
        ec,
        allowed_vertex_names={"A", "B"},
    )

    # C is disallowed → VRA skips it; edge actor finds no target slot → no intent.
    extraction_ctx = resource._executor.extract(
        {"src_type": "A", "src_id": "a1", "tgt_type": "C", "tgt_id": "c1"}
    )
    assert "C" not in extraction_ctx.acc_vertex
    assert not extraction_ctx.edge_intents

    # Both A and B are allowed → vertices accumulated, edge intent produced.
    extraction_ctx = resource._executor.extract(
        {"src_type": "A", "src_id": "a1", "tgt_type": "B", "tgt_id": "b1"}
    )
    assert "A" in extraction_ctx.acc_vertex
    assert "B" in extraction_ctx.acc_vertex
    assert extraction_ctx.edge_intents

    result = resource._executor.assemble_result(extraction_ctx).entities
    for k in result.keys():
        if not isinstance(k, tuple):
            continue
        s, t, _rel = k
        assert s in {"A", "B"}
        assert t in {"A", "B"}


def test_edge_inference_skips_edges_with_disallowed_vertices() -> None:
    vc = _vertex_config_a_b_c()
    ec = _edge_config_a_b_and_a_c()

    # Include an explicit A->C edge actor to ensure EdgeActor also early-exits.
    resource = _runtime(
        {
            "name": "r",
            "pipeline": [
                {"vertex": "A", "from": {"id": "a_id"}},
                {"vertex": "B", "from": {"id": "b_id"}},
                {"vertex": "C", "from": {"id": "c_id"}},
                {"edge": {"from": "A", "to": "C"}},
            ],
        },
        vc,
        ec,
        allowed_vertex_names={"A", "B"},
    )

    extraction_ctx = resource._executor.extract(
        {"a_id": "a1", "b_id": "b1", "c_id": "c1"}
    )
    assert "C" not in extraction_ctx.acc_vertex

    result = resource._executor.assemble_result(extraction_ctx).entities
    for k in result.keys():
        if not isinstance(k, tuple):
            continue
        s, t, _rel = k
        assert "C" not in (s, t)

    # Edge A->B should still exist (inferred).
    assert any(
        isinstance(k, tuple) and k[0] == "A" and k[1] == "B" for k in result.keys()
    )
