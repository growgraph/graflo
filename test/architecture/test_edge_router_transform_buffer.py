"""EdgeRouterActor merges same-location transform buffer into JSON observations."""

from __future__ import annotations

from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    TransformPayload,
    merge_observation_with_transform_buffer,
)
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.config import EdgeRouterActorConfig
from graflo.architecture.pipeline.runtime.actor.edge_router import EdgeRouterActor
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


def _minimal_vertices() -> VertexConfig:
    return VertexConfig.model_validate(
        {
            "vertices": [
                {"name": "Person", "properties": ["id"]},
                {"name": "Post", "properties": ["id"]},
            ]
        }
    )


def test_merge_observation_with_transform_buffer_order_and_override() -> None:
    doc = {"a": 1, "b": 2}
    p1 = TransformPayload(named={"b": 20, "c": 3})
    p2 = TransformPayload(named={"c": 30, "d": 4})
    merged = merge_observation_with_transform_buffer(doc, [p1, p2])
    assert merged == {"a": 1, "b": 20, "c": 30, "d": 4}


def test_merge_row_doc_alias_matches_merge_observation() -> None:
    from graflo.architecture.graph_types import merge_row_doc_with_transform_buffer

    obs = {"x": 1}
    buf = [TransformPayload(named={"y": 2})]
    assert merge_row_doc_with_transform_buffer(
        obs, buf
    ) == merge_observation_with_transform_buffer(obs, buf)


def test_edge_router_uses_relation_from_transform_buffer() -> None:
    cfg = EdgeRouterActorConfig(
        type="edge_router",
        source="Person",
        target="Post",
        source_type_field="unused_type_hint",
        source_fields={"id": "src_id"},
        target_fields={"id": "tgt_id"},
        relation_field="rel",
    )
    actor = EdgeRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_minimal_vertices(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex((0,))
    ctx.buffer_transforms[loc].append(
        TransformPayload(named={"rel": "likes", "src_id": "u1", "tgt_id": "p9"})
    )
    observation = {"ignored": True}
    out = actor(ctx, loc, doc=observation)

    assert len(out.edge_requests) == 1
    edge, eloc = out.edge_requests[0]
    assert eloc == loc
    assert edge.relation == "likes"
    assert edge.source == "Person"
    assert edge.target == "Post"

    src_items = out.acc_vertex["Person"][loc.extend(("src", 0))]
    tgt_items = out.acc_vertex["Post"][loc.extend(("tgt", 0))]
    assert src_items[0].vertex == {"id": "u1"}
    assert tgt_items[0].vertex == {"id": "p9"}


def test_edge_router_transform_overrides_doc_for_same_key() -> None:
    cfg = EdgeRouterActorConfig(
        type="edge_router",
        source="Person",
        target="Post",
        source_type_field="unused_type_hint",
        source_fields={"id": "src_id"},
        target_fields={"id": "tgt_id"},
        relation_field="rel",
    )
    actor = EdgeRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_minimal_vertices(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex(())
    ctx.buffer_transforms[loc].append(TransformPayload(named={"rel": "from_transform"}))
    observation = {"src_id": "a", "tgt_id": "b", "rel": "from_doc"}
    out = actor(ctx, loc, doc=observation)

    assert out.edge_requests[0][0].relation == "from_transform"


def test_edge_router_skips_non_dict_observation() -> None:
    cfg = EdgeRouterActorConfig(
        type="edge_router",
        source="Person",
        target="Post",
        source_type_field="unused_type_hint",
        source_fields={"id": "src_id"},
        target_fields={"id": "tgt_id"},
        relation_field="rel",
    )
    actor = EdgeRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_minimal_vertices(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex(())
    ctx.buffer_transforms[loc].append(
        TransformPayload(named={"rel": "x", "src_id": "1", "tgt_id": "2"})
    )
    out = actor(ctx, loc, doc=[])
    assert out.edge_requests == []


def test_edge_router_type_fields_from_transform_buffer() -> None:
    cfg = EdgeRouterActorConfig(
        type="edge_router",
        source_type_field="st",
        target_type_field="tt",
        source_fields={"id": "sid"},
        target_fields={"id": "tid"},
        relation_field="rel",
    )
    actor = EdgeRouterActor.from_config(cfg)
    init = ActorInitContext(
        vertex_config=_minimal_vertices(),
        edge_config=EdgeConfig(),
        transforms={},
    )
    actor.finish_init(init)

    ctx = ExtractionContext()
    loc = LocationIndex((1,))
    ctx.buffer_transforms[loc].append(
        TransformPayload(
            named={
                "st": "Person",
                "tt": "Post",
                "rel": "authored",
                "sid": "1",
                "tid": "2",
            }
        )
    )
    out = actor(ctx, loc, doc={})
    edge = out.edge_requests[0][0]
    assert edge.source == "Person"
    assert edge.target == "Post"
    assert edge.relation == "authored"
