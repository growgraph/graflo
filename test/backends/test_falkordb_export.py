"""FalkorDB export: every node and relationship, shaped as the other backends shape them."""

from __future__ import annotations

from typing import Any

from graflo.db.conn import ConnectionCapability
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.manager import ConnectionManager
from graflo.onto import DBType


class _Element:
    """A node or relationship as the FalkorDB client returns it."""

    def __init__(self, **properties: Any) -> None:
        self.properties = properties


class _Falkor(FalkordbConnection):
    def __init__(self, result_set: list[list[Any]]) -> None:
        self.result_set = result_set
        self.sent: list[str] = []

    def execute(self, query: str, **params: Any) -> Any:
        self.sent.append(query)
        return type("R", (), {"result_set": self.result_set})()


def test_falkordb_is_an_export_backend() -> None:
    assert DBType.FALKORDB in ConnectionManager.flavors_supporting(
        ConnectionCapability.GRAPH_EXPORT
    )


def test_all_nodes_of_a_label() -> None:
    conn = _Falkor([[_Element(id="a", labels=["x"])], [_Element(id="b")]])

    assert conn.fetch_all_docs("Person", limit=2) == [{"id": "a"}, {"id": "b"}]
    assert conn.sent == ["MATCH (n:Person) RETURN n LIMIT 2"]


def test_all_relationships_between_two_labels() -> None:
    conn = _Falkor([[_Element(id="a"), _Element(id="o"), _Element(since=2020)]])

    triples = conn.fetch_all_edges("Person", "Org", "works_at")

    assert triples == [[{"id": "a"}, {"id": "o"}, {"since": 2020}]]
    assert conn.sent == [
        "MATCH (source:Person)-[r:works_at]->(target:Org) RETURN source, target, r "
    ]
