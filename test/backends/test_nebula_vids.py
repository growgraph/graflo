"""A NebulaGraph VID names its tag, so two types never share a vertex."""

from __future__ import annotations

from typing import Any, cast

from graflo.architecture.graph_types import EdgeDirection
from graflo.db.nebula.conn import NebulaConnection
from graflo.db.nebula.query import insert_vertices_ngql
from graflo.db.nebula.util import make_vid, vertex_key


def test_the_vid_carries_the_tag() -> None:
    assert make_vid("person", {"id": 1}, ["id"]) == "person::1"
    assert make_vid("org", {"id": 1}, ["id"]) == "org::1"
    assert make_vid("pair", {"a": "x", "b": "y"}, ["a", "b"]) == "pair::x::y"


def test_the_address_leaves_the_tag_out() -> None:
    assert vertex_key({"a": "x", "b": "y"}, ["a", "b"]) == "x::y"


def test_vertices_are_inserted_under_tagged_vids() -> None:
    statement = insert_vertices_ngql("person", [{"id": 1}], ["id"], ["id"])

    assert '"person::1":(1)' in statement


class _Rows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def rows_as_dicts(self) -> list[dict[str, Any]]:
        return self._rows


class _Nebula:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.statements: list[str] = []
        self._rows = rows or []

    def _execute(self, statement: str) -> _Rows:
        self.statements.append(statement)
        return _Rows(self._rows)


def test_edges_join_tagged_vids() -> None:
    fake = _Nebula()

    NebulaConnection.insert_edges_batch(
        cast(Any, fake),
        [[{"id": 1}, {"id": 1}, {}]],
        "person",
        "org",
        "works_at",
        ("id",),
        ("id",),
    )

    assert '"person::1"->"org::1"' in fake.statements[0]


def test_an_edge_query_is_anchored_on_the_tagged_vid() -> None:
    fake = _Nebula(
        rows=[{"props": {"since": 2020}, "src": "person::1", "dst": "org::9"}]
    )

    rows = NebulaConnection.fetch_edges(
        cast(Any, fake),
        "person",
        "1",
        edge_type="works_at",
        to_type="org",
        to_id="9",
        direction=EdgeDirection.OUT,
    )

    assert 'GO FROM "person::1"' in fake.statements[0]
    assert 'id($$) == "org::9"' in fake.statements[0]
    assert rows == [{"since": 2020, "_src": "1", "_dst": "9", "_type": ""}]


def test_vertices_are_removed_with_their_edges() -> None:
    fake = _Nebula()

    NebulaConnection.delete_vertices(
        cast(Any, fake),
        "person",
        [{"id": 1}, {"name": "no id"}, {"id": 2}, {"id": 3}],
        ("id",),
        chunk_size=2,
    )

    assert fake.statements == [
        'DELETE VERTEX "person::1", "person::2" WITH EDGE',
        'DELETE VERTEX "person::3" WITH EDGE',
    ]


def test_edges_are_removed_between_tagged_vids() -> None:
    fake = _Nebula()

    NebulaConnection.delete_edges(
        cast(Any, fake),
        "person",
        "org",
        "works_at",
        [({"id": 1}, {"id": 9}), ({"id": 2}, {})],
        ("id",),
        ("id",),
    )

    assert fake.statements == ['DELETE EDGE `works_at` "person::1"->"org::9"']
