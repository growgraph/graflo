from __future__ import annotations

import pytest

from graflo.architecture.database_features import DatabaseProfile, EdgePhysicalSpec
from graflo.architecture.schema.edge import Edge
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.onto import DBType


def _bare_tg_conn() -> TigerGraphConnection:
    return TigerGraphConnection.__new__(TigerGraphConnection)


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


def test_directed_edge_with_reverse_edge_ddl() -> None:
    conn = _bare_tg_conn()
    edge = Edge(source="user", target="user", relation="is_following")
    profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        edge_specs=[
            EdgePhysicalSpec(
                source="user",
                target="user",
                relation="is_following",
                relation_name="is_following",
                reverse_edge="is_followed_by",
            )
        ],
    )
    stmt = conn._get_edge_add_statement(
        edge,
        relation_name="is_following",
        source_vertex="user",
        target_vertex="user",
        db_profile=profile,
    )
    assert stmt.startswith("ADD DIRECTED EDGE is_following")
    assert 'WITH REVERSE_EDGE="is_followed_by"' in stmt


def test_reverse_edge_forbidden_on_undirected() -> None:
    conn = _bare_tg_conn()
    edge = Edge(source="user", target="user", relation="friend_of", directed=False)
    profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        edge_specs=[
            EdgePhysicalSpec(
                source="user",
                target="user",
                relation="friend_of",
                reverse_edge="friend_of_rev",
            )
        ],
    )
    with pytest.raises(ValueError, match="reverse_edge cannot be set for undirected"):
        conn._get_edge_add_statement(
            edge,
            relation_name="friend_of",
            source_vertex="user",
            target_vertex="user",
            db_profile=profile,
        )
