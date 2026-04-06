"""TigerGraph identifier validation: reserved words and attribute names."""

from __future__ import annotations

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
