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


def test_clear_data_uses_installed_query_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = SimpleNamespace(database="cfg_graph", schema_name=None)
    conn._installed_clear_data_queries = {}

    submitted_queries: list[tuple[str, tuple[str, ...]]] = []

    def _fake_clear_data_via_installed_query(
        graph_name: str, vertex_types: tuple[str, ...]
    ) -> None:
        submitted_queries.append((graph_name, vertex_types))

    monkeypatch.setattr(
        conn, "_clear_data_via_installed_query", _fake_clear_data_via_installed_query
    )
    monkeypatch.setattr(
        conn,
        "_delete_vertices",
        lambda *_args, **_kwargs: pytest.fail("fallback delete path should not run"),
    )

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

    assert submitted_queries == [("cfg_graph", ("V_a", "V_b", "V_c"))]


def test_clear_data_falls_back_to_vertex_deletes_when_query_path_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unit test: validates fallback control flow with mocked connection methods."""
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = SimpleNamespace(database="cfg_graph", schema_name=None)
    conn._installed_clear_data_queries = {}

    submitted_deletes: list[tuple[str, str | None]] = []

    monkeypatch.setattr(
        conn,
        "_clear_data_via_installed_query",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("query install failed")),
    )

    def _fake_delete_vertices(
        vertex_type: str, graph_name: str | None = None, **_kwargs
    ) -> dict[str, str]:
        submitted_deletes.append((vertex_type, graph_name))
        return {"deleted": vertex_type}

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

    assert set(submitted_deletes) == {
        ("V_a", "cfg_graph"),
        ("V_b", "cfg_graph"),
        ("V_c", "cfg_graph"),
    }
