"""VertexRouterActor merges same-location transform buffer into JSON observations."""

from __future__ import annotations

from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    TransformPayload,
)
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.config import VertexRouterActorConfig
from graflo.architecture.pipeline.runtime.actor.vertex_router import VertexRouterActor
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


def _vertices_widget_and_post() -> VertexConfig:
    return VertexConfig.model_validate(
        {
            "vertices": [
                {"name": "Widget", "properties": ["id"]},
                {"name": "Post", "properties": ["id"]},
            ]
        }
    )


def test_vertex_router_type_field_only_from_transform_buffer() -> None:
    cfg = VertexRouterActorConfig(
        type="vertex_router",
        type_field="kind",
    )
    actor = VertexRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_vertices_widget_and_post(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex(("items", 0))
    ctx.buffer_transforms[loc].append(
        TransformPayload(named={"kind": "Widget", "id": "w-1"})
    )
    out = actor(ctx, loc, doc={})

    reps = out.acc_vertex["Widget"][loc]
    assert len(reps) == 1
    assert reps[0].vertex == {"id": "w-1"}


def test_vertex_router_transform_overrides_doc_for_type_field() -> None:
    cfg = VertexRouterActorConfig(
        type="vertex_router",
        type_field="kind",
    )
    actor = VertexRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_vertices_widget_and_post(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex(())
    ctx.buffer_transforms[loc].append(TransformPayload(named={"kind": "Post"}))
    out = actor(ctx, loc, doc={"kind": "Widget", "id": "p1"})

    assert "Widget" not in out.acc_vertex or not out.acc_vertex["Widget"].get(loc)
    reps = out.acc_vertex["Post"][loc]
    assert len(reps) == 1
    assert reps[0].vertex == {"id": "p1"}


def test_vertex_router_field_map_reads_keys_from_transform_buffer() -> None:
    cfg = VertexRouterActorConfig(
        type="vertex_router",
        type_field="t",
        field_map={"external_id": "id"},
    )
    actor = VertexRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_vertices_widget_and_post(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex((1,))
    ctx.buffer_transforms[loc].append(
        TransformPayload(named={"t": "Widget", "external_id": "ext-99"})
    )
    out = actor(ctx, loc, doc={})

    assert out.acc_vertex["Widget"][loc][0].vertex == {"id": "ext-99"}


def test_vertex_router_prefix_keys_from_transform_buffer() -> None:
    cfg = VertexRouterActorConfig(
        type="vertex_router",
        type_field="kind",
        prefix="p_",
    )
    actor = VertexRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_vertices_widget_and_post(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex((2,))
    ctx.buffer_transforms[loc].append(
        TransformPayload(named={"p_kind": "Widget", "p_id": "z"})
    )
    out = actor(ctx, loc, doc={})

    assert out.acc_vertex["Widget"][loc][0].vertex == {"id": "z"}


def test_transform_buffer_isolation_by_location_index() -> None:
    """Buffers at a parent LocationIndex must not affect routers at a deeper index."""
    cfg = VertexRouterActorConfig(
        type="vertex_router",
        type_field="kind",
    )
    actor = VertexRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_vertices_widget_and_post(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    parent = LocationIndex(("a", 0))
    child = LocationIndex(("a", 0, "b", 0))
    ctx.buffer_transforms[parent].append(TransformPayload(named={"kind": "Widget"}))
    out = actor(ctx, child, doc={})

    assert not any(out.acc_vertex[v][child] for v in out.acc_vertex)


def test_vertex_router_skips_non_dict_observation() -> None:
    cfg = VertexRouterActorConfig(type="vertex_router", type_field="kind")
    actor = VertexRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_vertices_widget_and_post(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex(())
    ctx.buffer_transforms[loc].append(
        TransformPayload(named={"kind": "Widget", "id": "x"})
    )
    out = actor(ctx, loc, doc="not-a-dict")
    assert not any(out.acc_vertex[v][loc] for v in out.acc_vertex)
