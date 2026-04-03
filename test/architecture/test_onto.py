from graflo.architecture.pipeline.runtime.actor import ActorInitContext, ActorWrapper
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.pipeline.runtime.executor import ActorExecutor
from graflo.architecture.graph_types import (
    AssemblyContext,
    ExtractionContext,
    GraphAssemblyResult,
    LocationIndex,
    ProvenancePath,
    TransformPayload,
)
from graflo.architecture.schema.vertex import VertexConfig


def test_provenance_path_from_lindex():
    lindex = LocationIndex(path=(0, "authors"))
    provenance = ProvenancePath.from_lindex(lindex)
    assert provenance.path == (0, "authors")


def test_extraction_context_record_helpers():
    ctx = ExtractionContext()
    lindex = LocationIndex(path=(1,))
    payload = TransformPayload.from_result({"id": "a"})

    ctx.record_transform_observation(location=lindex, payload=payload)
    ctx.record_vertex_observation(
        vertex_name="author",
        location=lindex,
        vertex={"id": "a"},
        ctx={"full_name": "A"},
    )
    ctx.record_edge_intent(edge=Edge(source="author", target="paper"), location=lindex)

    assert len(ctx.transform_observations) == 1
    assert len(ctx.vertex_observations) == 1
    assert len(ctx.edge_intents) == 1
    assert ctx.transform_observations[0].provenance.path == (1,)
    assert ctx.vertex_observations[0].provenance.path == (1,)
    assert ctx.edge_intents[0].provenance is not None


def test_assembly_context_from_extraction_shares_vertex_accumulator():
    extraction = ExtractionContext()
    assembly = AssemblyContext.from_extraction(extraction)
    assert assembly.acc_vertex is extraction.acc_vertex


def test_actor_executor_assemble_result_returns_graph_result():
    vc = VertexConfig.from_dict(
        {"vertices": [{"name": "author", "fields": ["id"], "identity": ["id"]}]}
    )
    ec = EdgeConfig.from_dict({"edges": []})

    wrapper = ActorWrapper(pipeline=[{"vertex": "author"}])
    wrapper.finish_init(
        init_ctx=ActorInitContext(
            vertex_config=vc,
            edge_config=ec,
            transforms={},
        )
    )
    executor = ActorExecutor(wrapper)

    extraction = executor.extract({"id": "42"})
    result = executor.assemble_result(extraction)

    assert isinstance(result, GraphAssemblyResult)
    assert result.entities["author"] == [{"id": "42"}]
