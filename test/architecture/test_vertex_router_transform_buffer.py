"""VertexRouterActor merges same-location transform buffer into JSON observations."""

from __future__ import annotations

from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    TransformPayload,
)
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.config import (
    VertexActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.pipeline.runtime.actor.vertex import VertexActor
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
    ctx.transform_buffer[loc].append(
        TransformPayload(named={"kind": "Widget", "id": "w-1"})
    )
    out = actor(ctx, loc, doc={})

    slot = loc.extend(("kind", 0))
    reps = out.acc_vertex["Widget"][slot]
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
    ctx.transform_buffer[loc].append(TransformPayload(named={"kind": "Post"}))
    out = actor(ctx, loc, doc={"kind": "Widget", "id": "p1"})

    slot = loc.extend(("kind", 0))
    assert "Widget" not in out.acc_vertex or not out.acc_vertex["Widget"].get(slot)
    reps = out.acc_vertex["Post"][slot]
    assert len(reps) == 1
    assert reps[0].vertex == {"id": "p1"}


def test_vertex_router_from_reads_keys_from_transform_buffer() -> None:
    cfg = VertexRouterActorConfig.model_validate(
        {
            "type": "vertex_router",
            "type_field": "t",
            "from": {"id": "external_id"},
        }
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
    ctx.transform_buffer[loc].append(
        TransformPayload(named={"t": "Widget", "external_id": "ext-99"})
    )
    out = actor(ctx, loc, doc={})

    slot = loc.extend(("t", 0))
    assert out.acc_vertex["Widget"][slot][0].vertex == {"id": "ext-99"}


def test_vertex_router_type_field_uses_prefixed_column_from_transform_buffer() -> None:
    cfg = VertexRouterActorConfig.model_validate(
        {
            "type": "vertex_router",
            "type_field": "p_kind",
            "from": {"id": "p_id"},
        }
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
    ctx.transform_buffer[loc].append(
        TransformPayload(named={"p_kind": "Widget", "p_id": "z"})
    )
    out = actor(ctx, loc, doc={})

    slot = loc.extend(("p_kind", 0))
    assert out.acc_vertex["Widget"][slot][0].vertex == {"id": "z"}


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
    ctx.transform_buffer[parent].append(TransformPayload(named={"kind": "Widget"}))
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
    ctx.transform_buffer[loc].append(
        TransformPayload(named={"kind": "Widget", "id": "x"})
    )
    out = actor(ctx, loc, doc="not-a-dict")
    assert not any(out.acc_vertex[v][loc] for v in out.acc_vertex)


def test_vertex_router_mapped_only_limits_to_from_fields() -> None:
    cfg = VertexRouterActorConfig.model_validate(
        {
            "type": "vertex_router",
            "type_field": "kind",
            "from": {"id": "external_id"},
            "extraction_scope": "mapped_only",
        }
    )
    actor = VertexRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=VertexConfig.model_validate(
            {"vertices": [{"name": "Widget", "properties": ["id", "name"]}]}
        ),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex(("items", 0))
    out = actor(
        ctx,
        loc,
        doc={"kind": "Widget", "external_id": "w-1", "name": "Widget Name"},
    )

    slot = loc.extend(("kind", 0))
    reps = out.acc_vertex["Widget"][slot]
    assert len(reps) == 1
    assert reps[0].vertex == {"id": "w-1"}


def test_vertex_actor_and_router_overlap_extract_same_fields() -> None:
    vc = VertexConfig.model_validate(
        {"vertices": [{"name": "Widget", "properties": ["id", "name"]}]}
    )
    init = ActorInitContext(
        vertex_config=vc,
        edge_config=EdgeConfig(),
        transforms={},
    )

    va = VertexActor.from_config(
        VertexActorConfig.model_validate(
            {
                "type": "vertex",
                "vertex": "Widget",
                "from": {"id": "external_id"},
                "extraction_scope": "mapped_only",
            }
        )
    )
    va.finish_init(init)

    vra = VertexRouterActor.from_config(
        VertexRouterActorConfig.model_validate(
            {
                "type": "vertex_router",
                "type_field": "kind",
                "from": {"id": "external_id"},
                "extraction_scope": "mapped_only",
            }
        )
    )
    vra.finish_init(init)

    loc = LocationIndex(("items", 0))

    ctx_va = ExtractionContext()
    ctx_va.transform_buffer[loc].append(
        TransformPayload(named={"external_id": "from_tf", "kind": "Widget"})
    )
    va(ctx_va, loc, doc={"external_id": "from_doc", "name": "ignored"})
    assert ctx_va.acc_vertex["Widget"][loc][0].vertex == {"id": "from_tf"}

    ctx_vra = ExtractionContext()
    ctx_vra.transform_buffer[loc].append(
        TransformPayload(named={"external_id": "from_tf", "kind": "Widget"})
    )
    vra(ctx_vra, loc, doc={"kind": "Widget", "external_id": "from_doc"})
    slot = loc.extend(("kind", 0))
    assert ctx_vra.acc_vertex["Widget"][slot][0].vertex == {"id": "from_tf"}
