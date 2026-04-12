"""Tests for edge render grouping and edge_router identity validation."""

from __future__ import annotations

from graflo.architecture.graph_types import (
    AssemblyContext,
    ExtractionContext,
    LocationIndex,
    VertexRep,
)
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.config import EdgeRouterActorConfig
from graflo.architecture.pipeline.runtime.actor.edge_render import render_edge
from graflo.architecture.pipeline.runtime.actor.edge_router import EdgeRouterActor
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


def _vc_ab() -> VertexConfig:
    return VertexConfig.model_validate(
        {
            "vertices": [
                {"name": "a", "properties": ["id"]},
                {"name": "b", "properties": ["id"]},
            ]
        }
    )


def test_render_edge_keeps_heterogeneous_equal_projected_values() -> None:
    """Different vertex types with the same projected field values keep the edge."""
    vc = _vc_ab()
    edge = Edge(source="a", target="b", relation="r")
    edge.finish_init(vc)

    ext = ExtractionContext()
    loc = LocationIndex(())
    ext.acc_vertex["a"][loc] = [VertexRep(vertex={"id": 42}, ctx={})]
    ext.acc_vertex["b"][loc] = [VertexRep(vertex={"id": 42}, ctx={})]

    asm = AssemblyContext.from_extraction(ext)
    out = render_edge(edge, vc, asm, lindex=None, derivation=None)
    total = sum(len(v) for v in out.values())
    assert total == 1


def test_edge_router_skips_blank_string_identity() -> None:
    """Rows with blank string for an endpoint identity (e.g. parent_id '') emit no edge."""
    vc = VertexConfig.model_validate(
        {"vertices": [{"name": "node", "properties": ["id"]}]}
    )
    cfg = EdgeRouterActorConfig(
        type="edge_router",
        source="node",
        target_type_field="t",
        source_fields={"id": "parent_id"},
        target_fields={"id": "id"},
        relation="child_of",
    )
    actor = EdgeRouterActor.from_config(cfg)
    actor.finish_init(
        ActorInitContext(
            vertex_config=vc,
            edge_config=EdgeConfig(),
            transforms={},
        )
    )
    ctx = ExtractionContext()
    loc = LocationIndex((0,))
    actor(
        ctx,
        loc,
        doc={"parent_id": "", "id": "n1", "t": "node"},
    )
    assert len(ctx.edge_intents) == 0
    assert len(ctx.edge_requests) == 0
