"""A cast that yields no document is not written, and touches no database."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from types import SimpleNamespace
from typing import Any, Self
from unittest.mock import MagicMock

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.onto import ArangoConfig, Neo4jConfig
from graflo.filter.onto import FilterExpression
from graflo.hq.caster import Caster
from graflo.hq.db_writer import DBWriter
from graflo.hq.document_caster import CastBatchResult
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.onto import DBType

NEO4J = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")


def _records_casting_to_nothing(n: int) -> GraphContainer:
    """A container as cast from *n* records that produced nothing."""
    return GraphContainer.from_docs_list([defaultdict(list) for _ in range(n)])


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="test"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(name=name, properties=[Field(name="id")], identity=["id"])
                    for name in ("machine", "line")
                ]
            ),
            edge_config=EdgeConfig(edges=[Edge(source="machine", target="line")]),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )


def _writer() -> DBWriter:
    schema = _schema()
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    return DBWriter(schema=schema, ingestion_model=model)


class _Recorder:
    """A ConnectionManager stand-in that counts connections and writes."""

    opened = 0
    upserts: list[str] = []
    edges: list[str] = []

    def __init__(self, connection_config: Any) -> None:
        del connection_config

    def __enter__(self) -> Self:
        type(self).opened += 1
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs) -> None:
        type(self).upserts.append(class_name)

    def insert_edges_batch(self, docs_edges, source_class, target_class, **kw) -> None:
        type(self).edges.append(f"{source_class}->{target_class}")

    def reset_after_use(self) -> None:
        pass


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> type[_Recorder]:
    class Recorder(_Recorder):
        opened = 0
        upserts: list[str] = []
        edges: list[str] = []

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", Recorder)
    return Recorder


class TestIsEmpty:
    @pytest.mark.parametrize(
        "gc",
        [
            pytest.param(GraphContainer(), id="nothing"),
            pytest.param(
                GraphContainer(
                    vertices={"machine": []},
                    attached={"line": []},
                    edges={("machine", "line", None): []},
                ),
                id="empty-lists",
            ),
            pytest.param(_records_casting_to_nothing(3), id="empty-records"),
        ],
    )
    def test_no_document_is_empty(self, gc: GraphContainer) -> None:
        assert gc.is_empty()

    @pytest.mark.parametrize(
        "gc",
        [
            pytest.param(
                GraphContainer(vertices={"machine": [{"id": 1}]}), id="vertex"
            ),
            pytest.param(GraphContainer(attached={"line": [{"id": 1}]}), id="attached"),
            pytest.param(
                GraphContainer(edges={("machine", "line", None): [({}, {}, {})]}),
                id="edge",
            ),
        ],
    )
    def test_one_document_is_not_empty(self, gc: GraphContainer) -> None:
        assert not gc.is_empty()


class TestTheWriter:
    @pytest.mark.parametrize(
        "gc",
        [
            pytest.param(_records_casting_to_nothing(2), id="empty-records"),
            pytest.param(
                GraphContainer(
                    vertices={"machine": []}, edges={("machine", "line", None): []}
                ),
                id="empty-lists",
            ),
        ],
    )
    def test_an_empty_container_opens_no_connection(
        self, gc: GraphContainer, recorder: type[_Recorder]
    ) -> None:
        asyncio.run(_writer().write(gc, NEO4J, resource_name=None))

        assert recorder.opened == 0

    def test_a_class_without_documents_is_skipped(
        self, recorder: type[_Recorder]
    ) -> None:
        gc = GraphContainer(
            vertices={"machine": [{"id": 1}], "line": []},
            edges={("machine", "line", None): []},
        )

        asyncio.run(_writer().write(gc, NEO4J, resource_name=None))

        assert (recorder.upserts, recorder.edges) == (["machine"], [])

    def test_an_unsupported_guard_is_refused_even_when_empty(self) -> None:
        arango = ArangoConfig(uri="http://localhost:8529", username="u", password="p")
        guard = FilterExpression.from_dict(["==", "x", "id"])

        with pytest.raises(NotImplementedError):
            asyncio.run(_writer().write(GraphContainer(), arango, None, guard=guard))


def _caster(**params: Any) -> Caster:
    model = MagicMock()
    model.fetch_resource = MagicMock(return_value=SimpleNamespace(name="items"))
    return Caster(_schema(), model, ingestion_params=IngestionParams(**params))


class _Writes:
    def __init__(self) -> None:
        self.written: list[GraphContainer] = []

    async def write(self, gc: GraphContainer, **kwargs: Any) -> None:
        self.written.append(gc)


def _spy_on_writes(caster: Caster, monkeypatch: pytest.MonkeyPatch) -> dict:
    seen: dict[str, Any] = {"writer": 0, "writes": _Writes()}

    def _get_db_writer(conn_conf: Any) -> _Writes:
        seen["writer"] += 1
        return seen["writes"]

    monkeypatch.setattr(caster, "_get_db_writer", _get_db_writer)
    return seen


class TestTheCaster:
    def test_an_empty_batch_builds_no_writer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        caster = _caster()
        seen = _spy_on_writes(caster, monkeypatch)

        async def _cast(data, resource_name=None) -> CastBatchResult:
            return CastBatchResult(graph=_records_casting_to_nothing(2), failures=[])

        monkeypatch.setattr(caster, "cast_normal_resource", _cast)

        asyncio.run(caster.process_batch([{"id": 1}, {"id": 2}], "items", NEO4J))

        assert seen["writer"] == 0

    def test_per_record_writes_skip_the_empty_parts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        caster = _caster(stamp_for=lambda resource, record: {"t": record["t"]})
        seen = _spy_on_writes(caster, monkeypatch)
        kept = GraphContainer(vertices={"machine": [{"id": 1}]})

        async def _cast_by_key(records, keys, resource_name, **kwargs):
            return [(keys[0], GraphContainer()), (keys[1], kept)], []

        monkeypatch.setattr(caster._document_caster, "cast_batch_by_key", _cast_by_key)

        asyncio.run(caster.process_batch([{"t": "a"}, {"t": "b"}], "items", NEO4J))

        assert seen["writes"].written == [kept]

    def test_per_record_writes_with_only_empty_parts_build_no_writer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        caster = _caster(stamp_for=lambda resource, record: {"t": record["t"]})
        seen = _spy_on_writes(caster, monkeypatch)

        async def _cast_by_key(records, keys, resource_name, **kwargs):
            return [(key, GraphContainer()) for key in dict.fromkeys(keys)], []

        monkeypatch.setattr(caster._document_caster, "cast_batch_by_key", _cast_by_key)

        asyncio.run(caster.process_batch([{"t": "a"}, {"t": "b"}], "items", NEO4J))

        assert seen["writer"] == 0
