"""The index statements the Memgraph connection sends."""

from __future__ import annotations

from typing import Any

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


def _memgraph(monkeypatch) -> tuple[Any, _Conn]:
    from graflo.connections.onto import MemgraphConfig
    from graflo.db.memgraph import conn as memgraph_conn

    fake = _Conn()
    monkeypatch.setattr(memgraph_conn.mgclient, "connect", lambda **kwargs: fake)
    return memgraph_conn.MemgraphConnection(
        MemgraphConfig(uri="bolt://localhost:7687")
    ), fake


def test_a_vertex_identity_is_indexed_and_constrained_unique(monkeypatch) -> None:
    from graflo.architecture.schema import Schema

    schema = Schema.model_validate(
        {
            "metadata": {"name": "people"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": ["name", "born"],
                            "identity": ["name", "born"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {"db_flavor": "memgraph"},
        }
    )
    db, fake = _memgraph(monkeypatch)

    db.define_vertex_indexes(schema.core_schema.vertex_config, schema)

    assert fake.statements == [
        "CREATE INDEX ON :person(name)",
        "CREATE INDEX ON :person(born)",
        "CREATE CONSTRAINT ON (n:person) ASSERT n.name, n.born IS UNIQUE",
    ]


class _CatalogCursor(_Cursor):
    """Answers ``SHOW CONSTRAINT INFO`` with a fixed catalogue."""

    description = [
        type("Column", (), {"name": name})()
        for name in ("constraint type", "label", "properties", "data_type")
    ]

    def fetchall(self) -> list[tuple]:
        return [
            ("unique", "person", ["name", "born"], ""),
            ("exists", "person", "name", ""),
            ("unique", "org", ["code"], ""),
        ]


class _CatalogConn(_Conn):
    def cursor(self) -> _Cursor:
        return _CatalogCursor(self.statements)


def _memgraph_with_catalog(monkeypatch) -> tuple[Any, _Conn]:
    from graflo.connections.onto import MemgraphConfig
    from graflo.db.memgraph import conn as memgraph_conn

    fake = _CatalogConn()
    monkeypatch.setattr(memgraph_conn.mgclient, "connect", lambda **kwargs: fake)
    return memgraph_conn.MemgraphConnection(
        MemgraphConfig(uri="bolt://localhost:7687")
    ), fake


def test_deleting_everything_drops_the_unique_constraints(monkeypatch) -> None:
    db, fake = _memgraph_with_catalog(monkeypatch)

    db.delete_graph_structure(delete_all=True)

    assert [s for s in fake.statements if s.startswith("DROP")] == [
        "DROP CONSTRAINT ON (n:person) ASSERT n.name, n.born IS UNIQUE",
        "DROP CONSTRAINT ON (n:org) ASSERT n.code IS UNIQUE",
    ]


def test_deleting_a_label_drops_only_its_unique_constraints(monkeypatch) -> None:
    db, fake = _memgraph_with_catalog(monkeypatch)

    db.delete_graph_structure(vertex_types=("org",))

    assert [s for s in fake.statements if s.startswith("DROP")] == [
        "DROP CONSTRAINT ON (n:org) ASSERT n.code IS UNIQUE",
    ]
