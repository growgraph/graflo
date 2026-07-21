"""TigerGraph DDL: typed LIST attributes."""

from __future__ import annotations

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema.db_aware import VertexConfigDBAware
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db.field_type_support import tigergraph_type_for_field
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.onto import DBType


def _bare_tg_conn() -> TigerGraphConnection:
    return TigerGraphConnection.__new__(TigerGraphConnection)


def test_tigergraph_type_for_list_field() -> None:
    field = Field(name="tags", type=FieldType.LIST, item_type=FieldType.STRING)
    assert tigergraph_type_for_field(field) == "LIST<STRING>"


def test_vertex_add_statement_emits_list_string() -> None:
    conn = _bare_tg_conn()
    vertex = Vertex(
        name="Article",
        properties=[
            Field(name="id", type=FieldType.STRING),
            Field(name="tags", type=FieldType.LIST, item_type=FieldType.STRING),
        ],
        identity=["id"],
    )
    profile = DatabaseProfile(db_flavor=DBType.TIGERGRAPH)
    vc = VertexConfigDBAware(VertexConfig(vertices=[vertex]), profile)
    stmt = conn._get_vertex_add_statement(vertex, vc, db_profile=profile)
    assert "tags LIST<STRING>" in stmt.replace("\n", " ")
