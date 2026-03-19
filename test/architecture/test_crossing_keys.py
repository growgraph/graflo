import logging

from graflo.architecture.pipeline.runtime.actor import ActorInitContext, ActorWrapper
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.graph_types import ActionContext, LocationIndex, VertexRep

logger = logging.getLogger(__name__)


def test_actor_wrapper_openalex(resource_cross, vertex_config_cross, sample_cross):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_cross)
    anw.finish_init(
        init_ctx=ActorInitContext(
            vertex_config=vertex_config_cross,
            edge_config=EdgeConfig(),
            transforms={},
        )
    )
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.acc_vertex["person"][LocationIndex(path=(0,))] == [
        VertexRep(vertex={"id": "John"}, ctx={"name": "John", "id": "Apple"}),
    ]

    assert ctx.acc_vertex["person"][LocationIndex(path=(1,))] == [
        VertexRep(vertex={"id": "Mary"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]

    assert ctx.acc_vertex["company"][LocationIndex(path=(0,))] == [
        VertexRep(vertex={"name": "Apple"}, ctx={"name": "John", "id": "Apple"}),
    ]

    assert ctx.acc_vertex["company"][LocationIndex(path=(1,))] == [
        VertexRep(vertex={"name": "Oracle"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]


def test_actor_wrapper_openalex_implicit(
    resource_cross_implicit, vertex_config_cross, sample_cross
):
    ctx = ActionContext()
    anw = ActorWrapper(resource_cross_implicit)
    anw.finish_init(
        init_ctx=ActorInitContext(
            vertex_config=vertex_config_cross,
            edge_config=EdgeConfig(),
            transforms={},
        )
    )
    ctx = anw(ctx, doc=sample_cross)

    assert ctx.acc_vertex["person"][LocationIndex(path=(0,))] == [
        VertexRep(vertex={"id": "John"}, ctx={"name": "John", "id": "Apple"}),
    ]
    assert ctx.acc_vertex["person"][LocationIndex(path=(1,))] == [
        VertexRep(vertex={"id": "Mary"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]
    assert ctx.acc_vertex["company"][LocationIndex(path=(0,))] == [
        VertexRep(vertex={"name": "Apple"}, ctx={"name": "John", "id": "Apple"}),
    ]
    assert ctx.acc_vertex["company"][LocationIndex(path=(1,))] == [
        VertexRep(vertex={"name": "Oracle"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]
