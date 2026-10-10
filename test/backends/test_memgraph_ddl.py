"""The index statements the Memgraph connection sends."""

from __future__ import annotations

from graflo.db.memgraph.conn import edge_property_index_query


def test_an_edge_index_is_an_edge_type_index() -> None:
    assert (
        edge_property_index_query("REVIEWS", "rating")
        == "CREATE EDGE INDEX ON :REVIEWS(rating)"
    )


class _Cursor:
    def __init__(self, statements: list[str]) -> None:
        self._statements = statements

    def execute(self, query: str, params: object = None) -> None:
        self._statements.append(" ".join(query.split()))

    def close(self) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.autocommit = False

    def cursor(self) -> _Cursor:
        return _Cursor(self.statements)


def test_an_upsert_creates_its_merge_index_once_per_connection(monkeypatch) -> None:
    from graflo.connections.onto import MemgraphConfig
    from graflo.db.memgraph import conn as memgraph_conn

    fake = _Conn()
    monkeypatch.setattr(memgraph_conn.mgclient, "connect", lambda **kwargs: fake)
    db = memgraph_conn.MemgraphConnection(MemgraphConfig(uri="bolt://localhost:7687"))

    for batch in range(3):
        db.upsert_docs_batch([{"id": str(batch)}], "person", match_keys=["id"])

    assert [s for s in fake.statements if s.startswith("CREATE INDEX")] == [
        "CREATE INDEX ON :person(id)"
    ]
