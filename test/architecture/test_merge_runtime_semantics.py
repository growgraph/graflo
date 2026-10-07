"""What a merge does to the graph a resource actually emits.

The rest of the evolution suite asserts on the *manifest*. That is what let a merge
quietly change ingestion: the manifest looked right while the emitted graph lost
nodes and edges. These tests cast a real document and assert on the container.
"""

from __future__ import annotations

import asyncio
import logging

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


def _manifest(
    *, edges: list[Edge], pipeline: list[dict], b_declares_a_id: bool = False
) -> GraphManifest:
    schema = Schema(
        metadata=GraphMetadata(name="g", version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="A", properties=[Field(name="a_id")], identity=["a_id"]
                    ),
                    Vertex(
                        name="B",
                        properties=[Field(name="b_id")]
                        + ([Field(name="a_id")] if b_declares_a_id else []),
                        identity=["b_id"],
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

        B and D declare ``a_id`` -- their documents may carry it -- which is
        what lets merge key the class on ``a_id`` at all; its step maps only ``b_id``, so
        the observation it emits here is still keyless.
        """
        left = _manifest(
            edges=TestFusionIsJudgedPerSlot._edges(),
            pipeline=pipeline,
            b_declares_a_id=True,
        )
        right = GraphManifest.from_config(
            {
                "schema": Schema(
                    metadata=GraphMetadata(name="r", version="1.0.0"),
                    core_schema=CoreSchema(
                        vertex_config=VertexConfig(
                            vertices=[
                                Vertex(
                                    name="D",
                                    properties=[Field(name="d_id"), Field(name="a_id")],
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
                    left=["A", "B"],
                    right="D",
                    into="A",
                    identity=["a_id"],
                    allow=["observation_fusion"] if allow_fusion else [],
                )
            ],
        )
        return merge_manifests(left, right, op, bump_version=False)

    @staticmethod
    def _a_rows(manifest: GraphManifest) -> list[dict]:
        vertices, _edges = _emit(manifest)
        return [
            # B's demoted key is named by its origin, the schema name ``g``.
            {k: v for k, v in row.items() if k in ("a_id", "g__b_id")}
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
        assert self._a_rows(out) == [{"a_id": "a1", "g__b_id": "b1"}]

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


#: ``C`` is keyed by a digest ``id`` and found by ``by_k``; ``D`` by ``d_id``,
#: or by ``by_code`` when an edge step says so.
_ATTACH_DOC = {"k": "K1", "name": "n1", "d_id": "d1", "code": "X1"}


def _attach_manifest(pipeline: list[dict]) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "g", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "C",
                                "properties": ["k", "name"],
                                "hash_identity_properties": ["name"],
                                "secondary_identities": [
                                    {"name": "by_k", "fields": ["k"]}
                                ],
                            },
                            {
                                "name": "D",
                                "properties": ["d_id", "code"],
                                "identity": ["d_id"],
                                "secondary_identities": [
                                    {"name": "by_code", "fields": ["code"]}
                                ],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [{"source": "C", "target": "D", "relation": "cd"}]
                    },
                },
            },
            "ingestion_model": {"resources": [{"name": "res", "pipeline": pipeline}]},
        }
    )
    manifest.finish_init()
    return manifest


def _attach_cast(manifest: GraphManifest, docs: list[dict] | None = None):
    caster = DocumentCaster(manifest.require_ingestion_model())
    return asyncio.run(
        caster.cast_batch(docs or [_ATTACH_DOC], "res", params=IngestionParams())
    ).graph


def _endpoints(graph, edge_id=("C", "D", "cd")) -> list[tuple[dict, dict]]:
    return [(dict(s), dict(t)) for s, t, *_ in graph.edges.get(edge_id, [])]


_C_FINDS = {"vertex": "C", "find": "by_k", "extraction_scope": "mapped_only"}
_C_FROM = {"k": "k", "name": "name"}
_D_STEP = {"vertex": "D", "from": {"d_id": "d_id"}, "extraction_scope": "mapped_only"}
_EDGE = {"edge": {"source": "C", "target": "D", "relation": "cd"}}


class TestAttachedVertices:
    """A ``find`` step writes onto the vertex its secondary identity finds."""

    def test_an_attached_rep_keeps_its_find_fields_and_gets_no_digest(self) -> None:
        graph = _attach_cast(
            _attach_manifest([{**_C_FINDS, "from": _C_FROM}, _D_STEP, _EDGE])
        )
        assert graph.attached["C"] == [{"k": "K1", "name": "n1"}]
        assert graph.attached_by == {"C": "by_k"}
        assert not graph.vertices.get("C")
        assert graph.vertices["D"] == [{"d_id": "d1"}]

    def test_the_runtime_exposes_the_selector_per_class(self) -> None:
        manifest = _attach_manifest([{**_C_FINDS, "from": _C_FROM}, _D_STEP, _EDGE])
        runtime = manifest.require_ingestion_model().fetch_resource("res")
        assert runtime.attached_selectors == {"C": "by_k"}
        match = runtime.edge_derivation.endpoint_match_for(("C", "D", "cd"))
        assert match is not None and (match.source, match.target) == ("by_k", None)

    def test_an_explicit_edge_without_a_selector_follows_the_attached_vertex(
        self,
    ) -> None:
        graph = _attach_cast(
            _attach_manifest([{**_C_FINDS, "from": _C_FROM}, _D_STEP, _EDGE])
        )
        assert _endpoints(graph) == [({"k": "K1"}, {"d_id": "d1"})]

    def test_an_inferred_edge_follows_the_attached_vertex(self) -> None:
        graph = _attach_cast(_attach_manifest([{**_C_FINDS, "from": _C_FROM}, _D_STEP]))
        assert _endpoints(graph) == [({"k": "K1"}, {"d_id": "d1"})]

    def test_an_authored_selector_on_the_other_endpoint_is_kept(self) -> None:
        d_by_code = {
            "vertex": "D",
            "from": {"code": "code"},
            "lookup_only": True,
            "extraction_scope": "mapped_only",
        }
        edge = {
            "edge": {
                "source": "C",
                "target": "D",
                "relation": "cd",
                "target_match": "by_code",
            }
        }
        graph = _attach_cast(
            _attach_manifest([{**_C_FINDS, "from": _C_FROM}, d_by_code, edge])
        )
        assert _endpoints(graph) == [({"k": "K1"}, {"code": "X1"})]

    def test_identical_attached_rows_are_deduplicated(self) -> None:
        manifest = _attach_manifest(
            [
                {
                    "descend": {
                        "key": "items",
                        "pipeline": [{**_C_FINDS, "from": _C_FROM}],
                    }
                }
            ]
        )
        row = {"k": "K1", "name": "n1"}
        graph = _attach_cast(manifest, [{"items": [row, dict(row)]}])
        assert graph.attached["C"] == [row]

        graph = _attach_cast(manifest, [{"items": [row]}, {"items": [dict(row)]}])
        assert len(graph.attached["C"]) == 2
        graph.pick_unique()
        assert graph.attached["C"] == [row]

    def test_lookup_only_with_find_references_and_attaches_nothing(self) -> None:
        graph = _attach_cast(
            _attach_manifest(
                [{**_C_FINDS, "from": {"k": "k"}, "lookup_only": True}, _D_STEP, _EDGE]
            )
        )
        assert not graph.attached.get("C")
        assert not graph.vertices.get("C")
        assert _endpoints(graph) == [({"k": "K1"}, {"d_id": "d1"})]

    def test_attached_rows_without_their_find_fields_are_dropped(self) -> None:
        graph = _attach_cast(
            _attach_manifest([{**_C_FINDS, "from": _C_FROM}, _D_STEP, _EDGE]),
            [{"name": "n1", "d_id": "d1"}],
        )
        assert not graph.attached.get("C")
        assert not _endpoints(graph)

    def test_an_attached_row_missing_its_find_key_is_dropped_as_attached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Absent (not ``None``) find key: the row is still attached, never written."""
        manifest = _attach_manifest(
            [
                {"vertex": "C", "find": "by_k", "keep_fields": ["k", "name"]},
                _D_STEP,
                _EDGE,
            ]
        )
        with caplog.at_level(logging.WARNING):
            graph = _attach_cast(manifest, [{"name": "n1", "d_id": "d1"}])
        assert not graph.vertices.get("C")
        assert not graph.attached.get("C")
        assert "attached 'C'" in caplog.text and "'by_k'" in caplog.text

    def test_an_attached_row_missing_its_find_key_is_never_a_vertex(self) -> None:
        manifest = _attach_manifest(
            [
                {"vertex": "C", "find": "by_k", "keep_fields": ["k", "name"]},
                _D_STEP,
                _EDGE,
            ]
        )
        caster = DocumentCaster(manifest.require_ingestion_model())
        graph = asyncio.run(
            caster.cast_batch(
                [{"name": "n1", "d_id": "d1"}],
                "res",
                params=IngestionParams(drop_empty_identity_docs=False),
            )
        ).graph
        assert "C" not in graph.vertices
        assert graph.attached["C"] == [{"name": "n1"}]

    def test_worker_processes_carry_the_attached_channel(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        manifest = _attach_manifest([{**_C_FINDS, "from": _C_FROM}, _D_STEP, _EDGE])
        docs = [{**_ATTACH_DOC, "k": f"K{i}"} for i in range(4)]
        caster = DocumentCaster(manifest.require_ingestion_model())
        try:
            graph = asyncio.run(
                caster.cast_batch(
                    docs,
                    "res",
                    params=IngestionParams(n_cores=2, cast_executor="process"),
                )
            ).graph
        finally:
            caster.close()
        assert "falling back" not in caplog.text
        assert graph.attached["C"] == [{"k": f"K{i}", "name": "n1"} for i in range(4)]
        assert graph.attached_by == {"C": "by_k"}

    def test_the_container_round_trips_its_attached_channel(self) -> None:
        graph = _attach_cast(
            _attach_manifest([{**_C_FINDS, "from": _C_FROM}, _D_STEP, _EDGE])
        )
        restored = type(graph).model_validate(graph.model_dump(mode="json"))
        assert restored.attached == {"C": [{"k": "K1", "name": "n1"}]}
        assert restored.attached_by == {"C": "by_k"}

    def test_a_class_produced_with_and_without_find_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"res.*'C'.*find"):
            _attach_manifest([_C_FINDS, {"vertex": "C"}])

    def test_find_naming_an_undeclared_secondary_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"by_nothing"):
            _attach_manifest([{"vertex": "C", "find": "by_nothing"}])

    def test_find_naming_the_primary_identity_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"secondary identity"):
            _attach_manifest([{"vertex": "C", "find": "identity"}])
