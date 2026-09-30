"""A filter value reaches each backend as a bound parameter or an escaped literal."""

from __future__ import annotations

from typing import Any, cast

import pytest

from graflo.architecture.graph_types import EdgeDirection
from graflo.db.arango.conn import ArangoConnection
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.nebula.conn import NebulaConnection
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.postgres.conn import PostgresConnection
from graflo.onto import AggregationType, ExpressionFlavor

HOSTILE = 'x" OR 1 == 1 OR "'
FILTER = ["==", HOSTILE, "name"]


class _Recorder:
    """Records ``(query, parameters)`` for each statement a connection sends."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []

    def assert_bound(self) -> None:
        assert self.sent
        for query, params in self.sent:
            assert HOSTILE not in query
        assert any(HOSTILE in params.values() for _, params in self.sent)


# -- ArangoDB -------------------------------------------------------------------


class _Arango(_Recorder):
    def __init__(self) -> None:
        super().__init__()
        outer = self

        class _Aql:
            def execute(self, query: str, bind_vars: dict | None = None) -> Any:
                outer.sent.append((query, dict(bind_vars or {})))
                return iter([])

        self.conn = type("C", (), {"aql": _Aql()})()

    def execute(self, query: str, bind_vars: dict | None = None) -> Any:
        return ArangoConnection.execute(cast(Any, self), query, bind_vars=bind_vars)


def test_arango_binds_filter_values() -> None:
    fake = _Arango()
    conn = cast(Any, fake)

    ArangoConnection.fetch_docs(conn, "people", filters=FILTER)
    ArangoConnection.aggregate(conn, "people", AggregationType.COUNT, filters=FILTER)
    ArangoConnection.fetch_edges(conn, "people", "1", edge_type="knows", filters=FILTER)
    ArangoConnection.fetch_present_documents(
        conn, [{"name": HOSTILE}], "people", ["name"]
    )

    fake.assert_bound()
    edge_query, edge_vars = fake.sent[2]
    assert "FILTER FILTER" not in edge_query
    assert edge_vars["anchor"] == "people/1"
    _, present_vars = fake.sent[3]
    assert present_vars["docs"] == [{"name": HOSTILE, "__i": 0}]


# -- Cypher ---------------------------------------------------------------------


class _Neo4j(_Recorder):
    def expression_flavor(self) -> ExpressionFlavor:
        return ExpressionFlavor.CYPHER

    def execute(self, query: str, **params: Any) -> Any:
        self.sent.append((query, params))
        return type("R", (), {"data": lambda _self: [], "result_set": []})()


def test_neo4j_binds_filter_values() -> None:
    fake = _Neo4j()
    conn = cast(Any, fake)

    Neo4jConnection.fetch_docs(conn, "Person", filters=FILTER)
    Neo4jConnection.fetch_edges(conn, "Person", "1", edge_type="KNOWS", filters=FILTER)

    fake.assert_bound()
    assert fake.sent[1][1]["from_id"] == "1"


def test_falkordb_binds_filter_values() -> None:
    fake = _Neo4j()
    conn = cast(Any, fake)

    FalkordbConnection.fetch_docs(conn, "Person", filters=FILTER)
    FalkordbConnection.fetch_edges(
        conn, "Person", "1", edge_type="KNOWS", filters=FILTER
    )
    FalkordbConnection.aggregate(conn, "Person", AggregationType.COUNT, filters=FILTER)

    fake.assert_bound()


class _Memgraph(_Recorder):
    def __init__(self) -> None:
        super().__init__()
        outer = self

        class _Cursor:
            description: list = []

            def execute(self, query: str, params: dict | None = None) -> None:
                outer.sent.append((query, dict(params or {})))

            def fetchall(self) -> list:
                return [(0,)]

            def close(self) -> None:
                return None

        self.conn = type("C", (), {"cursor": lambda _self: _Cursor()})()

    def expression_flavor(self) -> ExpressionFlavor:
        return ExpressionFlavor.CYPHER


def test_memgraph_binds_filter_values() -> None:
    fake = _Memgraph()
    conn = cast(Any, fake)

    MemgraphConnection.fetch_docs(conn, "Person", filters=FILTER)
    MemgraphConnection.fetch_edges(
        conn, "Person", "1", edge_type="KNOWS", filters=FILTER
    )
    MemgraphConnection.aggregate(conn, "Person", AggregationType.COUNT, filters=FILTER)

    fake.assert_bound()


# -- PostgreSQL -----------------------------------------------------------------


class _Pg(_Recorder):
    def __init__(self) -> None:
        super().__init__()
        self.config = type("C", (), {"schema_name": "g"})()

    def read(self, query: str, params: Any = None) -> list:
        self.sent.append((query, dict(params or {})))
        return []

    def get_table_columns(self, table: str, schema_name: str | None = None) -> list:
        return [{"name": c} for c in ("id", "source_id", "target_id")]


def test_postgres_binds_filter_values() -> None:
    fake = _Pg()
    conn = cast(Any, fake)

    PostgresConnection.fetch_docs(conn, "person", filters=FILTER)
    PostgresConnection.aggregate(conn, "person", AggregationType.COUNT, filters=FILTER)
    PostgresConnection.fetch_edges(
        conn,
        "person",
        "1",
        edge_type="person_person_knows_edges",
        filters=FILTER,
        direction=EdgeDirection.ANY,
    )

    fake.assert_bound()
    query, params = fake.sent[2]
    assert query.count("%(f0)s") == 2
    assert params == {"from_id": "1", "f0": HOSTILE}


# -- NebulaGraph: no parameters, so an escaped literal ---------------------------


class _Nebula:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.config = type("C", (), {"is_v3": True})()

    def _execute(self, statement: str) -> Any:
        self.statements.append(statement)
        return type("R", (), {"rows_as_dicts": lambda _self: []})()

    def _render_filter(self, filters: Any, doc_name: str) -> str:
        return NebulaConnection._render_filter(cast(Any, self), filters, doc_name)


def test_nebula_writes_an_escaped_literal() -> None:
    fake = _Nebula()

    NebulaConnection.fetch_docs(cast(Any, fake), "Person", filters=FILTER)

    [statement] = fake.statements
    assert '== "x\\" OR 1 == 1 OR \\""' in statement


@pytest.mark.parametrize("value", ["a\x00b", float("inf")])
def test_nebula_refuses_a_value_with_no_literal(value) -> None:
    with pytest.raises(ValueError, match="literal"):
        NebulaConnection.fetch_docs(
            cast(Any, _Nebula()), "Person", filters=["==", value, "name"]
        )


def test_nebula_escapes_the_vid_it_looks_up() -> None:
    fake = _Nebula()

    NebulaConnection.fetch_present_documents(
        cast(Any, fake), [{"name": HOSTILE}], "Person", ["name"]
    )

    [statement] = fake.statements
    assert '"Person::x\\" OR 1 == 1 OR \\""' in statement
