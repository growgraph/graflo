"""What a merge does to the graph a resource actually emits.

The rest of the evolution suite asserts on the *manifest*. That is what let a merge
quietly change ingestion: the manifest looked right while the emitted graph lost
nodes and edges. These tests cast a real document and assert on the container.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import MergeVerticesOp, apply_evolution
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

DOC = {"a_id": "a1", "b_id": "b1", "c_id": "c1"}


def _manifest(*, edges: list[Edge], pipeline: list[dict]) -> GraphManifest:
    schema = Schema(
        metadata=GraphMetadata(name="g", version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="A", properties=[Field(name="a_id")], identity=["a_id"]
                    ),
                    Vertex(
                        name="B", properties=[Field(name="b_id")], identity=["b_id"]
                    ),
                    Vertex(
                        name="C", properties=[Field(name="c_id")], identity=["c_id"]
                    ),
                ],
                force_types={},
            ),
            edge_config=EdgeConfig(edges=edges),
        ),
    )
    manifest = GraphManifest.from_config(
        {
            "schema": schema.to_dict(skip_defaults=False),
            "ingestion_model": {
                "resources": [{"name": "res", "apply": pipeline}],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _emit(manifest: GraphManifest) -> tuple[dict, dict]:
    caster = DocumentCaster(manifest.require_ingestion_model())
    result = asyncio.run(caster.cast_batch([DOC], "res", params=IngestionParams()))
    graph = result.graph
    return (
        {name: list(rows) for name, rows in graph.vertices.items() if rows},
        {edge_id: len(rows) for edge_id, rows in graph.edges.items() if rows},
    )


class TestEdgeInference:
    def test_two_relations_on_one_vertex_pair_are_both_inferred(self) -> None:
        """Inference used to key emitted edges by (source, target) only.

        With two relations declared between the same pair, whichever came first in
        edge_config suppressed the other — silent edge loss with no merge involved.
        """
        manifest = _manifest(
            edges=[
                Edge(source="A", target="C", relation="ac"),
                Edge(source="A", target="C", relation="ac2"),
            ],
            pipeline=[{"vertex": "A"}, {"vertex": "C"}],
        )
        _vertices, edges = _emit(manifest)
        assert edges == {("A", "C", "ac"): 1, ("A", "C", "ac2"): 1}

    def test_an_explicit_edge_still_suppresses_inference_for_its_pair(self) -> None:
        """Unchanged: authored edges win over inferred ones for the same pair."""
        manifest = _manifest(
            edges=[
                Edge(source="A", target="C", relation="ac"),
                Edge(source="A", target="C", relation="ac2"),
            ],
            pipeline=[
                {"vertex": "A"},
                {"vertex": "C"},
                {"edge": {"source": "A", "target": "C", "relation": "ac"}},
            ],
        )
        _vertices, edges = _emit(manifest)
        assert edges == {("A", "C", "ac"): 1}


class TestMergeGuardsProtectTheEmittedGraph:
    @staticmethod
    def _joined() -> GraphManifest:
        """A and B are joined by an edge and both produced by one resource."""
        return _manifest(
            edges=[
                Edge(source="A", target="C", relation="ac"),
                Edge(source="B", target="C", relation="bc"),
                Edge(source="A", target="B", relation="ab"),
            ],
            pipeline=[{"vertex": "A"}, {"vertex": "B"}, {"vertex": "C"}],
        )

    def test_the_emitted_graph_before_the_merge(self) -> None:
        vertices, edges = _emit(self._joined())
        assert vertices == {
            "A": [{"a_id": "a1"}],
            "B": [{"b_id": "b1"}],
            "C": [{"c_id": "c1"}],
        }
        assert edges == {
            ("A", "C", "ac"): 1,
            ("B", "C", "bc"): 1,
            ("A", "B", "ab"): 1,
        }

    def test_a_merge_that_would_fuse_rows_is_rejected_by_default(self) -> None:
        with pytest.raises(ValueError, match="self-relations"):
            apply_evolution(
                self._joined(),
                [MergeVerticesOp(sources=["B"], into="A")],
                bump_version=False,
            )

    def test_the_error_names_the_edge_that_becomes_a_self_relation(self) -> None:
        with pytest.raises(ValueError, match=r"\(A, B, ab\) -> \(A, A, ab\)"):
            apply_evolution(
                self._joined(),
                [MergeVerticesOp(sources=["B"], into="A")],
                bump_version=False,
            )

    def test_row_fusion_is_reported_separately_from_self_relations(self) -> None:
        """No edge joins A and B, so only the shared pipeline level is a problem."""
        manifest = _manifest(
            edges=[
                Edge(source="A", target="C", relation="ac"),
                Edge(source="B", target="C", relation="bc"),
            ],
            pipeline=[{"vertex": "A"}, {"vertex": "B"}, {"vertex": "C"}],
        )
        with pytest.raises(ValueError, match="more than once"):
            apply_evolution(
                manifest,
                [MergeVerticesOp(sources=["B"], into="A")],
                bump_version=False,
            )

    def test_affirming_both_hazards_lets_the_merge_through(self) -> None:
        """With the intent stated, the merge applies — and both relations survive.

        Before the inference fix, ('A', 'C', 'bc') was dropped here as well, so the
        merge cost an edge type on top of fusing the rows.
        """
        out = apply_evolution(
            self._joined(),
            [
                MergeVerticesOp(
                    sources=["B"],
                    into="A",
                    allow_self_relations=True,
                    allow_row_fusion=True,
                )
            ],
            bump_version=False,
        )
        vertices, edges = _emit(out)
        assert set(vertices) == {"A", "C"}
        assert edges == {("A", "C", "ac"): 1, ("A", "C", "bc"): 1}

    def test_a_merge_with_no_shared_level_and_no_joining_edge_is_clean(self) -> None:
        """The guards fire on real hazards, not on every merge."""
        manifest = _manifest(
            edges=[Edge(source="A", target="C", relation="ac")],
            pipeline=[{"vertex": "A"}, {"vertex": "C"}],
        )
        out = apply_evolution(
            manifest,
            [MergeVerticesOp(sources=["B"], into="A")],
            bump_version=False,
        )
        assert "B" not in out.require_schema().core_schema.vertex_config.vertex_set
