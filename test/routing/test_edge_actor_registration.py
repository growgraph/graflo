"""Edges a step names per document are registered while documents are cast.

Casting runs on several threads at once -- chunks of one batch, and batches in
flight -- so the registration has to hold under concurrency, and a resource
with such a step cannot be cast in worker processes.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from graflo.architecture.contract.ingestion.resource import Resource
from graflo.architecture.contract.ingestion.steps import EdgeActorConfig
from graflo.architecture.graph_types import (
    AssemblyContext,
    ExtractionContext,
    LocationIndex,
    VertexRep,
)
from graflo.architecture.pipeline.runtime import assemble
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.edge import EdgeActor
from graflo.architecture.pipeline.runtime.resource import build_resource_runtime
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


def _vertex_config(*names: str) -> VertexConfig:
    return VertexConfig.model_validate(
        {
            "vertices": [
                {"name": name, "properties": ["id"], "identity": ["id"]}
                for name in names
            ]
        }
    )


def _role_edge_actor(vertex_config: VertexConfig, edge_config: EdgeConfig) -> EdgeActor:
    actor = EdgeActor.from_config(
        EdgeActorConfig.model_validate(
            {
                "type": "edge",
                "source_role": "S",
                "target_role": "T",
                "relation": "uses",
            }
        )
    )
    actor.finish_init(
        ActorInitContext(
            vertex_config=vertex_config, edge_config=edge_config, transforms={}
        )
    )
    return actor


def test_two_threads_naming_one_edge_register_it_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vertex_config = _vertex_config("machine", "line")
    edge_config = EdgeConfig()
    actor = _role_edge_actor(vertex_config, edge_config)

    both_registering = threading.Barrier(2)
    registered: list[Edge] = []
    register = EdgeConfig.update_edges

    def observed_register(
        self: EdgeConfig, edge: Edge, vertex_config: VertexConfig
    ) -> None:
        registered.append(edge)
        try:
            # Released at once when both threads are in here; times out when
            # the second one is kept waiting outside.
            both_registering.wait(timeout=0.3)
        except threading.BrokenBarrierError:
            pass
        register(self, edge, vertex_config=vertex_config)

    monkeypatch.setattr(EdgeConfig, "update_edges", observed_register)

    with ThreadPoolExecutor(max_workers=2) as pool:
        edges = list(
            pool.map(
                lambda _: actor._get_or_create_edge("machine", "line", "uses"),
                range(2),
            )
        )

    assert len(registered) == 1
    assert edges[0] is edges[1]


def test_assembly_tolerates_an_edge_registered_while_it_infers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vertex_config = _vertex_config("machine", "line", "plant")
    edge_config = EdgeConfig(edges=[Edge(source="machine", target="line")])
    ctx = AssemblyContext.from_extraction(ExtractionContext())
    ctx.acc_vertex["machine"][LocationIndex()].append(VertexRep(vertex={"id": "m1"}))
    ctx.acc_vertex["line"][LocationIndex()].append(VertexRep(vertex={"id": "l1"}))

    emit = assemble._emit_edge_documents

    def emit_while_another_thread_registers(**kwargs: Any) -> set[tuple[str, str]]:
        edge_config.update_edges(
            Edge(source="line", target="plant"), vertex_config=vertex_config
        )
        return emit(**kwargs)

    monkeypatch.setattr(
        assemble, "_emit_edge_documents", emit_while_another_thread_registers
    )

    assemble.assemble_edges(
        ctx=ctx,
        vertex_config=vertex_config,
        edge_config=edge_config,
        infer_edges=True,
    )

    assert ctx.acc_global["machine", "line", None] == [({"id": "m1"}, {"id": "l1"}, {})]


def _runtime(pipeline: list[dict[str, Any]]):
    vertex_config = _vertex_config("machine", "line")
    return build_resource_runtime(
        Resource.from_dict({"name": "r", "pipeline": pipeline}),
        vertex_config,
        EdgeConfig(edges=[Edge(source="machine", target="line", relation="uses")]),
    )


ROUTERS: list[dict[str, Any]] = [
    {"vertex_router": {"type_field": "s_type", "role": "S", "from": {"id": "s_id"}}},
    {"vertex_router": {"type_field": "t_type", "role": "T", "from": {"id": "t_id"}}},
]


@pytest.mark.parametrize(
    "edge_step",
    [
        {"edge": {"source_role": "S", "target_role": "T", "relation": "uses"}},
        {"edge": {"links": [{"source_role": "S", "target_role": "T"}]}},
    ],
    ids=["role-step", "role-link"],
)
def test_a_resource_with_a_per_document_edge_step_says_so(
    edge_step: dict[str, Any],
) -> None:
    assert _runtime([*ROUTERS, edge_step]).has_dynamic_edge_steps


def test_a_resource_with_fixed_edge_steps_does_not() -> None:
    runtime = _runtime(
        [
            {"vertex": "machine", "from": {"id": "s_id"}},
            {"vertex": "line", "from": {"id": "t_id"}},
            {"edge": {"from": "machine", "to": "line", "relation": "uses"}},
        ]
    )
    assert not runtime.has_dynamic_edge_steps
