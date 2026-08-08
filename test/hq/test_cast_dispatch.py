"""Cast dispatch modes must agree, and must not change per-document semantics.

Casting is spread over chunks (and optionally worker processes), so the invariants
worth pinning are that the mode is invisible in the output and that per-document
error handling still behaves per document rather than per chunk.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

# Above the chunking floor, so more than one chunk is actually produced.
N_DOCS = 500


@pytest.fixture
def manifest() -> GraphManifest:
    schema = Schema(
        metadata=GraphMetadata(name="g", version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[Field(name="pid"), Field(name="name")],
                        identity=["pid"],
                    ),
                    Vertex(
                        name="org", properties=[Field(name="oid")], identity=["oid"]
                    ),
                ],
                force_types={},
            ),
            edge_config=EdgeConfig(
                edges=[Edge(source="person", target="org", relation="works_at")]
            ),
        ),
    )
    out = GraphManifest.from_config(
        {
            "schema": schema.to_dict(skip_defaults=False),
            "ingestion_model": {
                "resources": [
                    {
                        "name": "r",
                        # A type caster gives a document that fails outright rather
                        # than one that degrades into a tolerated transform failure.
                        "types": {"rank": "int"},
                        "apply": [
                            {
                                "vertex": "person",
                                "from": {"pid": "pid", "name": "name"},
                            },
                            {"vertex": "org", "from": {"oid": "oid"}},
                            {
                                "edge": {
                                    "source": "person",
                                    "target": "org",
                                    "relation": "works_at",
                                }
                            },
                        ],
                    }
                ],
                "transforms": [],
            },
        }
    )
    out.finish_init()
    return out


@pytest.fixture
def two_resource_manifest(manifest) -> GraphManifest:
    config = manifest.to_dict(skip_defaults=False)
    resources = config["ingestion_model"]["resources"]
    second = {**resources[0], "name": "r2"}
    resources.append(second)
    out = GraphManifest.from_config(config)
    out.finish_init()
    return out


def _docs(n: int = N_DOCS) -> list[dict]:
    return [
        {"pid": f"p{i}", "name": f"n{i}", "oid": f"o{i % 7}", "rank": str(i)}
        for i in range(n)
    ]


def _cast(manifest: GraphManifest, docs: list[dict], **params):
    caster = DocumentCaster(manifest.require_ingestion_model())
    try:
        return asyncio.run(
            caster.cast_batch(docs, "r", params=IngestionParams(**params))
        )
    finally:
        caster.close()


def _shape(result) -> tuple[dict, dict]:
    return (
        {name: len(rows) for name, rows in result.graph.vertices.items()},
        {edge_id: len(rows) for edge_id, rows in result.graph.edges.items()},
    )


class TestModesAgree:
    @pytest.mark.parametrize("n_cores", [1, 2, 4])
    def test_chunking_does_not_change_the_graph(self, manifest, n_cores) -> None:
        baseline = _shape(_cast(manifest, _docs(), n_cores=1, cast_executor="inline"))
        assert (
            _shape(_cast(manifest, _docs(), n_cores=n_cores, cast_executor="thread"))
            == baseline
        )

    def test_worker_processes_produce_the_same_graph(self, manifest) -> None:
        baseline = _shape(_cast(manifest, _docs(), n_cores=1, cast_executor="inline"))
        assert (
            _shape(_cast(manifest, _docs(), n_cores=2, cast_executor="process"))
            == baseline
        )

    def test_document_order_is_preserved_across_chunks(self, manifest) -> None:
        result = _cast(manifest, _docs(), n_cores=4, cast_executor="thread")
        assert [row["pid"] for row in result.graph.vertices["person"]] == [
            f"p{i}" for i in range(N_DOCS)
        ]

    def test_an_empty_batch_is_handled(self, manifest) -> None:
        result = _cast(manifest, [], n_cores=4, cast_executor="thread")
        assert result.graph.vertices == {}


class TestPerDocumentErrorsStayPerDocument:
    """A chunk holds many documents; one bad document must not take the rest."""

    @staticmethod
    def _mixed() -> list[dict]:
        docs = _docs(200)
        docs[57] = {**docs[57], "rank": "not-an-int"}
        return docs

    @pytest.mark.parametrize("executor", ["inline", "thread", "process"])
    def test_skip_reports_one_failure_and_keeps_the_rest(
        self, manifest, executor
    ) -> None:
        result = _cast(
            manifest,
            self._mixed(),
            n_cores=4,
            cast_executor=executor,
            on_doc_error="skip",
        )
        assert len(result.failures) == 1
        assert result.failures[0].doc_index == 57
        assert len(result.graph.vertices["person"]) == 199

    @pytest.mark.parametrize("executor", ["inline", "thread", "process"])
    def test_fail_propagates(self, manifest, executor) -> None:
        with pytest.raises(Exception):
            _cast(
                manifest,
                self._mixed(),
                n_cores=4,
                cast_executor=executor,
                on_doc_error="fail",
            )

    def test_a_worker_failure_keeps_the_original_exception_type(self, manifest) -> None:
        """The worker's exception is rebuilt from data, not unpickled.

        Reporting it as the transport error would hide what actually broke.
        """
        inline = _cast(
            manifest, self._mixed(), cast_executor="inline", on_doc_error="skip"
        )
        process = _cast(
            manifest,
            self._mixed(),
            n_cores=2,
            cast_executor="process",
            on_doc_error="skip",
        )
        assert process.failures[0].exception_type == inline.failures[0].exception_type
        assert process.failures[0].traceback


class TestProcessPathFallsBackRatherThanFailing:
    def test_dynamic_edges_stay_in_process(self, manifest) -> None:
        """Dynamic edges register on shared config mid-cast, which a worker cannot do.

        The parent would never see those registrations, so the process path declines
        the work instead of silently losing them.
        """
        caster = DocumentCaster(manifest.require_ingestion_model())
        runtime = manifest.ingestion_model.fetch_resource("r")
        spec = caster._cast_spec(runtime, params=IngestionParams(dynamic_edges=True))
        assert spec is None

    def test_a_normal_resource_is_process_castable(self, manifest) -> None:
        caster = DocumentCaster(manifest.require_ingestion_model())
        runtime = manifest.ingestion_model.fetch_resource("r")
        assert caster._cast_spec(runtime, params=IngestionParams()) is not None


class TestAutoModeSelection:
    """`auto` picks processes only when the CPU work can win back the transfer."""

    def _caster(self, manifest) -> DocumentCaster:
        return DocumentCaster(manifest.require_ingestion_model())

    def test_auto_stays_inline_on_one_core(self, manifest) -> None:
        caster = self._caster(manifest)
        assert (
            caster._resolve_cast_mode(IngestionParams(n_cores=1), n_docs=10_000)
            == "inline"
        )

    def test_auto_picks_process_with_cores_and_volume(self, manifest) -> None:
        caster = self._caster(manifest)
        assert (
            caster._resolve_cast_mode(IngestionParams(n_cores=4), n_docs=10_000)
            == "process"
        )

    def test_auto_stays_inline_for_small_batches(self, manifest) -> None:
        caster = self._caster(manifest)
        assert (
            caster._resolve_cast_mode(IngestionParams(n_cores=4), n_docs=100)
            == "inline"
        )

    def test_auto_stays_inline_with_dynamic_edges(self, manifest) -> None:
        caster = self._caster(manifest)
        params = IngestionParams(n_cores=4, dynamic_edges=True)
        assert caster._resolve_cast_mode(params, n_docs=10_000) == "inline"

    def test_explicit_executor_is_respected(self, manifest) -> None:
        caster = self._caster(manifest)
        params = IngestionParams(n_cores=1, cast_executor="thread")
        assert caster._resolve_cast_mode(params, n_docs=10) == "thread"


class TestDynamicEdgesNeverMultiThread:
    def test_thread_mode_collapses_to_single_thread(self, manifest) -> None:
        """Dynamic edges register on shared config mid-cast; a thread fan-out
        would race those registrations, so the fallback is single-threaded."""
        caster = DocumentCaster(manifest.require_ingestion_model())
        captured: dict = {}
        original = caster._cast_documents

        async def _spy(runtime, docs, *, n_cores):
            captured["n_cores"] = n_cores
            return await original(runtime, docs, n_cores=n_cores)

        caster._cast_documents = _spy  # ty: ignore[invalid-assignment]
        params = IngestionParams(n_cores=8, cast_executor="thread", dynamic_edges=True)
        asyncio.run(caster.cast_batch(_docs(), "r", params=params))
        assert captured["n_cores"] == 1


class TestPoolsPerResource:
    def test_two_resources_keep_two_live_pools(self, two_resource_manifest) -> None:
        """Interleaving resources must not tear pools down (rebuild is the cost
        of importing graflo in every worker)."""
        caster = DocumentCaster(two_resource_manifest.require_ingestion_model())
        params = IngestionParams(n_cores=2, cast_executor="process")
        try:
            asyncio.run(caster.cast_batch(_docs(80), "r", params=params))
            asyncio.run(caster.cast_batch(_docs(80), "r2", params=params))
            asyncio.run(caster.cast_batch(_docs(80), "r", params=params))
            assert len(caster._pools) == 2
        finally:
            caster.close()
        assert not caster._pools
