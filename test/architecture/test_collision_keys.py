import logging

from graflo.architecture.actor import ActorWrapper
from graflo.architecture.onto import ActionContext, LocationIndex, VertexRep

logger = logging.getLogger(__name__)


def test_collision(resource_collision, vertex_config_collision, sample_cross):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_collision)
    anw.finish_init(transforms={}, vertex_config=vertex_config_collision)
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.acc_vertex["person"][LocationIndex(path=(0,))] == [
        VertexRep(vertex={"id": "John"}, ctx={"name": "John", "id": "Apple"}),
    ]
    assert ctx.acc_vertex["person"][LocationIndex(path=(1,))] == [
        VertexRep(vertex={"id": "Mary"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]

    assert ctx.acc_vertex["company"][LocationIndex(path=(0,))] == [
        VertexRep(vertex={"id": "Apple"}, ctx={"name": "John", "id": "Apple"}),
    ]

    assert ctx.acc_vertex["company"][LocationIndex(path=(1,))] == [
        VertexRep(vertex={"id": "Oracle"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]
