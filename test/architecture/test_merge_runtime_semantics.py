"""What a merge does to the graph a resource actually emits.

The rest of the evolution suite asserts on the *manifest*. That is what let a merge
quietly change ingestion: the manifest looked right while the emitted graph lost
nodes and edges. These tests cast a real document and assert on the container.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    MergeManifestsOp,
    MergeVerticesOp,
    VertexEquivalence,
    apply_evolution,
    merge_manifests,
)
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

DOC = {"a_id": "a1", "b_id": "b1", "c_id": "c1"}

# A flat DOC carries every key, so a ``full``-scope step would scoop all of the
# merged class's properties and every step would emit the same entity. Mapping
# each step to its own key is what real pipelines do (``from`` + ``keep_fields``).
_MAPPED = {"extraction_scope": "mapped_only"}


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
                    allow_observation_fusion=True,
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


class TestFusionIsJudgedPerSlot:
    """The runtime fuses per accumulator slot — level plus ``role`` — never per level.

    A vertex step with a ``role`` stores at ``lindex.extend((role, 0))``; a bare
    step at the level itself. Two same-level steps with distinct roles cannot
    share a bucket, so the guard must not read them as fusion.
    """

    @staticmethod
    def _edges() -> list[Edge]:
        return [
            Edge(source="A", target="C", relation="ac"),
            Edge(source="B", target="C", relation="bc"),
        ]

    def test_role_separated_steps_are_not_fusion(self) -> None:
        """Distinct roles: the merge applies unflagged and both nodes survive."""
        manifest = _manifest(
            edges=self._edges(),
            pipeline=[
                {"vertex": "A", "role": "a", "from": {"a_id": "a_id"}, **_MAPPED},
                {"vertex": "B", "role": "b", "from": {"b_id": "b_id"}, **_MAPPED},
                {"vertex": "C"},
            ],
        )
        out = apply_evolution(
            manifest,
            [MergeVerticesOp(sources=["B"], into="A")],
            bump_version=False,
        )
        vertices, _edges = _emit(out)
        assert vertices["A"] == [{"a_id": "a1"}, {"b_id": "b1"}]

    @staticmethod
    def _composed(pipeline: list[dict], *, allow_fusion: bool) -> GraphManifest:
        """A and B collapsed onto A under identity ``a_id`` by a merge.

        Under that identity the observation the former B step emits carries no
        key, which is the shape that fuses: ``fuse_doc_basis`` folds a keyless
        observation into the keyed one before it *in the same bucket*.
        """
        left = _manifest(edges=TestFusionIsJudgedPerSlot._edges(), pipeline=pipeline)
        right = GraphManifest.from_config(
            {
                "schema": Schema(
                    metadata=GraphMetadata(name="r", version="1.0.0"),
                    core_schema=CoreSchema(
                        vertex_config=VertexConfig(
                            vertices=[
                                Vertex(
                                    name="D",
                                    properties=[Field(name="d_id")],
                                    identity=["d_id"],
                                )
                            ],
                            force_types={},
                        ),
                        edge_config=EdgeConfig(edges=[]),
                    ),
                ).to_dict(skip_defaults=False),
                "ingestion_model": {
                    "resources": [{"name": "res_r", "apply": [{"vertex": "D"}]}],
                    "transforms": [],
                },
            }
        )
        right.finish_init()
        op = MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left=["A", "B"], right="D", into="A", identity=["a_id"]
                )
            ],
            allow_merges=True,
            allow_observation_fusion=allow_fusion,
        )
        return merge_manifests(left, right, op, bump_version=False)

    @staticmethod
    def _a_rows(manifest: GraphManifest) -> list[dict]:
        vertices, _edges = _emit(manifest)
        return [
            {k: v for k, v in row.items() if k in ("a_id", "b_id")}
            for row in vertices["A"]
        ]

    def test_bare_steps_of_two_members_fuse_once_acknowledged(self) -> None:
        """The hazard the flag acknowledges is real: one bucket, one node."""
        out = self._composed(
            [
                {"vertex": "A", "from": {"a_id": "a_id"}, **_MAPPED},
                {"vertex": "B", "from": {"b_id": "b_id"}, **_MAPPED},
                {"vertex": "C"},
            ],
            allow_fusion=True,
        )
        assert self._a_rows(out) == [{"a_id": "a1", "b_id": "b1"}]

    def test_roled_steps_of_two_members_stay_apart_unacknowledged(self) -> None:
        """Same collapse, distinct roles: two buckets, no flag, A's row intact.

        The former B observation is in its own bucket, so nothing folds it into
        A's row. Carrying only the demoted secondary key, it has no primary
        identity and the caster drops it — until an identity alignment derives
        ``a_id`` for it — so the emitted graph holds A's own row and nothing
        borrowed from B.
        """
        out = self._composed(
            [
                {"vertex": "A", "role": "a", "from": {"a_id": "a_id"}, **_MAPPED},
                {"vertex": "B", "role": "b", "from": {"b_id": "b_id"}, **_MAPPED},
                {"vertex": "C"},
            ],
            allow_fusion=False,
        )
        assert self._a_rows(out) == [{"a_id": "a1"}]

    def test_same_role_slot_is_still_fusion(self) -> None:
        manifest = _manifest(
            edges=self._edges(),
            pipeline=[
                {"vertex": "A", "role": "s"},
                {"vertex": "B", "role": "s"},
                {"vertex": "C"},
            ],
        )
        with pytest.raises(ValueError, match=r"more than once.*slot 's'.*'A', 'B'"):
            apply_evolution(
                manifest,
                [MergeVerticesOp(sources=["B"], into="A")],
                bump_version=False,
            )

    def test_the_refusal_points_at_roles(self) -> None:
        manifest = _manifest(
            edges=self._edges(),
            pipeline=[{"vertex": "A"}, {"vertex": "B"}, {"vertex": "C"}],
        )
        with pytest.raises(ValueError, match=r"slot <bare>.*its own `role`"):
            apply_evolution(
                manifest,
                [MergeVerticesOp(sources=["B"], into="A")],
                bump_version=False,
            )

    @pytest.mark.parametrize(
        "pipeline",
        [
            [{"vertex": "A"}, {"vertex": "A"}, {"vertex": "C"}],
            [
                {"vertex": "A", "role": "x"},
                {"vertex": "A", "role": "y"},
                {"vertex": "C"},
            ],
        ],
        ids=["bare", "roled"],
    )
    def test_a_class_already_produced_twice_is_not_the_merges_doing(
        self, pipeline: list[dict]
    ) -> None:
        """A level that repeated one class before the merge is unchanged by it."""
        manifest = _manifest(edges=self._edges(), pipeline=pipeline)
        out = apply_evolution(
            manifest,
            [MergeVerticesOp(sources=["B"], into="A")],
            bump_version=False,
        )
        assert "B" not in out.require_schema().core_schema.vertex_config.vertex_set

    def test_a_router_slot_is_distinct_from_a_bare_step(self) -> None:
        """A router stores at ``(role or type_field, 0)``; a bare step at the level."""
        router = {"type_field": "kind", "type_map": {"a": "A"}}
        manifest = _manifest(
            edges=self._edges(),
            pipeline=[router, {"vertex": "B"}, {"vertex": "C"}],
        )
        out = apply_evolution(
            manifest,
            [MergeVerticesOp(sources=["B"], into="A")],
            bump_version=False,
        )
        assert "B" not in out.require_schema().core_schema.vertex_config.vertex_set

        manifest = _manifest(
            edges=self._edges(),
            pipeline=[router, {"vertex": "B", "role": "kind"}, {"vertex": "C"}],
        )
        with pytest.raises(ValueError, match=r"more than once.*slot 'kind'"):
            apply_evolution(
                manifest,
                [MergeVerticesOp(sources=["B"], into="A")],
                bump_version=False,
            )
