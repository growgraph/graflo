from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

import pytest

from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.db.tigergraph.schema_ddl import SchemaDdlBuilder


def _bare_tg_conn() -> TigerGraphConnection:
    return TigerGraphConnection.__new__(TigerGraphConnection)


def _schema(*, relation_name: str | None = None, **spec: Any) -> Schema:
    edge_spec: dict[str, Any] = {
        "source": "user",
        "target": "post",
        "relation": "wrote",
        **spec,
    }
    if relation_name is not None:
        edge_spec["relation_name"] = relation_name
    return Schema.model_validate(
        {
            "metadata": {"name": "g"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": "user", "properties": ["id"], "identity": ["id"]},
                        {"name": "post", "properties": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {
                    "edges": [
                        {"source": "user", "target": "post", "relation": "wrote"}
                    ],
                    "inverses": [{"relation": "wrote", "inverse": "written_by"}],
                },
            },
            "db_profile": {"db_flavor": "tigergraph", "edge_specs": [edge_spec]},
        }
    )


@pytest.fixture
def captured_gsql(monkeypatch: pytest.MonkeyPatch):
    """Run the schema DDL builder against a connection that only records GSQL."""
    monkeypatch.setattr("graflo.db.tigergraph.schema_ddl.time.sleep", lambda _s: None)

    def run(schema: Schema) -> str:
        sent: list[str] = []
        conn = cast(Any, _bare_tg_conn())
        conn.config = SimpleNamespace(max_job_size=15000)
        conn._require_configured_graph_name = lambda: "g"
        conn._execute_gsql = sent.append
        conn._ensure_graph_context = lambda _name: nullcontext()
        conn._get_vertex_types = lambda: ["user", "post"]
        conn._get_edge_types = lambda: ["wrote"]
        SchemaDdlBuilder(conn)._define_schema_local(schema)
        return "\n".join(sent)

    return run


def test_undirected_edge_ddl() -> None:
    conn = _bare_tg_conn()
    edge = Edge(source="user", target="user", relation="friend_of", directed=False)
    stmt = conn._get_edge_add_statement(
        edge,
        relation_name="friend_of",
        source_vertex="user",
        target_vertex="user",
    )
    assert stmt.startswith("ADD UNDIRECTED EDGE friend_of")
    assert "WITH REVERSE_EDGE" not in stmt


def test_directed_edge_with_native_inverse_ddl() -> None:
    conn = _bare_tg_conn()
    edge = Edge(source="user", target="user", relation="is_following")
    stmt = conn._get_edge_add_statement(
        edge,
        relation_name="is_following",
        source_vertex="user",
        target_vertex="user",
        native_inverse="is_followed_by",
    )
    assert stmt.startswith("ADD DIRECTED EDGE is_following")
    assert 'WITH REVERSE_EDGE="is_followed_by"' in stmt


def test_native_inverse_forbidden_on_undirected() -> None:
    conn = _bare_tg_conn()
    edge = Edge(source="user", target="user", relation="friend_of", directed=False)
    with pytest.raises(ValueError, match="native inverse cannot be set for undirected"):
        conn._get_edge_add_statement(
            edge,
            relation_name="friend_of",
            source_vertex="user",
            target_vertex="user",
            native_inverse="friend_of_rev",
        )


def test_schema_ddl_names_the_native_inverse_after_the_declared_inverse(
    captured_gsql,
) -> None:
    gsql = captured_gsql(_schema(native_inverse=True))
    assert 'WITH REVERSE_EDGE="written_by"' in gsql


def test_schema_ddl_without_native_inverse_emits_no_pairing(captured_gsql) -> None:
    assert "WITH REVERSE_EDGE" not in captured_gsql(_schema())


def test_schema_ddl_refuses_a_native_inverse_shadowing_a_physical_type(
    captured_gsql,
) -> None:
    """Logical names differ, but the relation_name override takes the type name."""
    schema = _schema(native_inverse=True, relation_name="written_by")
    with pytest.raises(ValueError, match="collides with a TigerGraph"):
        captured_gsql(schema)
