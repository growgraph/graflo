"""TigerGraph DDL: GSQL DEFAULT from db_profile.default_property_values."""

from __future__ import annotations

from graflo.architecture.database_features import DatabaseProfile, DefaultPropertyValues
from graflo.architecture.schema.db_aware import VertexConfigDBAware
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.onto import DBType


def _bare_tg_conn() -> TigerGraphConnection:
    return TigerGraphConnection.__new__(TigerGraphConnection)


def test_vertex_add_statement_primary_key_float_default() -> None:
    """Mirrors: CREATE VERTEX Sensor (id STRING PRIMARY KEY, reading FLOAT DEFAULT -1.0)."""
    conn = _bare_tg_conn()
    vertex = Vertex(
        name="Sensor",
        properties=[
            Field(name="id", type=FieldType.STRING),
            Field(name="reading", type=FieldType.FLOAT),
        ],
        identity=["id"],
    )
    profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        default_property_values=DefaultPropertyValues(
            vertices={"Sensor": {"reading": -1.0}}
        ),
    )
    vc = VertexConfigDBAware(VertexConfig(vertices=[vertex]), profile)
    stmt = conn._get_vertex_add_statement(vertex, vc, db_profile=profile)
    assert "reading FLOAT DEFAULT -1.0" in stmt.replace("\n", " ")
    assert "PRIMARY_ID" in stmt or "PRIMARY KEY" in stmt


def test_vertex_field_def_helper() -> None:
    conn = _bare_tg_conn()
    profile = DatabaseProfile(
        default_property_values=DefaultPropertyValues(
            vertices={"Sensor": {"reading": -1.25}}
        ),
    )
    line = conn._gsql_vertex_field_def(
        logical_vertex_name="Sensor",
        field_name="reading",
        tg_type="FLOAT",
        db_profile=profile,
    )
    assert line == "reading FLOAT DEFAULT -1.25"


def test_vertex_add_statement_primary_id_string_default_uses_primary_key_syntax() -> (
    None
):
    """GSQL rejects PRIMARY_ID name STRING DEFAULT ...; use ... PRIMARY KEY instead."""
    conn = _bare_tg_conn()
    vertex = Vertex(
        name="company",
        properties=[
            Field(name="name", type=FieldType.STRING),
        ],
        identity=["name"],
    )
    profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        default_property_values=DefaultPropertyValues(
            vertices={"company": {"name": "ABC"}}
        ),
    )
    vc = VertexConfigDBAware(VertexConfig(vertices=[vertex]), profile)
    stmt = conn._get_vertex_add_statement(vertex, vc, db_profile=profile)
    normalized = stmt.replace("\n", " ")
    assert "PRIMARY_ID" not in normalized
    assert 'name STRING DEFAULT "ABC" PRIMARY KEY' in normalized
    assert "PRIMARY_ID_AS_ATTRIBUTE" not in normalized
