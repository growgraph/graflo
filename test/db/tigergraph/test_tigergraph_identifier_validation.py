"""TigerGraph identifier validation: reserved words and attribute names."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema.db_aware import EdgeConfigDBAware, VertexConfigDBAware
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db.tigergraph.conn import (
    TigerGraphConnection,
    _load_tigergraph_name_rules,
    _validate_tigergraph_schema_name,
)
import graflo.db.tigergraph.conn as tigergraph_conn_module
from graflo.onto import DBType


def test_name_rules_cached() -> None:
    _load_tigergraph_name_rules.cache_clear()
    _load_tigergraph_name_rules()
    info = _load_tigergraph_name_rules.cache_info()
    assert info.currsize == 1
    _load_tigergraph_name_rules()
    assert _load_tigergraph_name_rules.cache_info().hits >= 1


def test_reserved_word_rejected_for_vertex_property_label() -> None:
    with pytest.raises(ValueError, match="reserved"):
        _validate_tigergraph_schema_name("INT", "vertex property")


def test_validate_vertex_properties_helper() -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    vertex = Vertex(
        name="v",
        properties=[
            Field(name="id", type=FieldType.STRING),
            Field(name="FROM", type=FieldType.STRING),
        ],
        identity=["id"],
    )
    with pytest.raises(ValueError, match="reserved"):
        conn._validate_tigergraph_vertex_properties(vertex)


def test_validate_edge_property_names_helper() -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    vertex = Vertex(
        name="a",
        properties=[Field(name="id", type=FieldType.STRING)],
        identity=["id"],
    )
    vc = VertexConfig(vertices=[vertex])
    profile = DatabaseProfile(db_flavor=DBType.TIGERGRAPH)
    vc_db = VertexConfigDBAware(vc, profile)
    edge = Edge(
        source="a",
        target="a",
        relation="knows",
        properties=[Field(name="FLOAT", type=FieldType.STRING)],
    )
    ec = EdgeConfig(edges=[edge])
    edge.finish_init(vertex_config=vc)
    ec_db = EdgeConfigDBAware(ec, vc_db, profile)
    with pytest.raises(ValueError, match="reserved"):
        conn._validate_tigergraph_edge_property_names(edge, ec_db)


def test_configured_graph_name_prefers_database_then_schema_name() -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = SimpleNamespace(database="db_graph", schema_name="schema_graph")
    assert conn._configured_graph_name() == "db_graph"

    conn.config = SimpleNamespace(database=None, schema_name="schema_graph")
    assert conn._configured_graph_name() == "schema_graph"


def test_require_configured_graph_name_raises_when_unset() -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = SimpleNamespace(database=None, schema_name=None)
    with pytest.raises(ValueError, match="config.database or config.schema_name"):
        conn._require_configured_graph_name()


def test_clear_data_uses_bounded_parallel_deletes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = SimpleNamespace(database="cfg_graph", schema_name=None)

    submitted: list[tuple[str, str | None]] = []
    created_max_workers: list[int] = []

    class _FakeFuture:
        def __init__(self, fn, **kwargs):
            self._fn = fn
            self._kwargs = kwargs

        def result(self):
            return self._fn(**self._kwargs)

    class _FakeExecutor:
        def __init__(self, max_workers: int):
            created_max_workers.append(max_workers)
            self._futures: list[_FakeFuture] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, **kwargs):
            future = _FakeFuture(fn, **kwargs)
            self._futures.append(future)
            return future

    def _fake_as_completed(futures):
        return iter(list(futures))

    def _fake_delete_vertices(
        vertex_type: str, graph_name: str | None = None, **_kwargs
    ):
        submitted.append((vertex_type, graph_name))
        return {"deleted": vertex_type}

    monkeypatch.setattr(tigergraph_conn_module, "ThreadPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(tigergraph_conn_module, "as_completed", _fake_as_completed)
    monkeypatch.setattr(conn, "_delete_vertices", _fake_delete_vertices)

    class _FakeVertexConfig:
        vertex_set = ("a", "b", "c")

        @staticmethod
        def vertex_dbname(vertex_name: str) -> str:
            return f"V_{vertex_name}"

    class _FakeDbAwareSchema:
        vertex_config = _FakeVertexConfig()

    class _FakeSchema:
        metadata = SimpleNamespace(name="schema_graph")

        @staticmethod
        def resolve_db_aware(_db_type):
            return _FakeDbAwareSchema()

    conn.clear_data(_FakeSchema())

    assert created_max_workers == [3]
    assert set(submitted) == {
        ("V_a", "cfg_graph"),
        ("V_b", "cfg_graph"),
        ("V_c", "cfg_graph"),
    }
