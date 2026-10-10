"""A PostgreSQL edge table stores every identity field of each endpoint."""

from __future__ import annotations

from typing import Any, Self, cast

import pytest

from graflo.architecture.graph_types import EdgeDirection
from graflo.db.postgres import target_write
from graflo.db.postgres.conn import PostgresConnection
from graflo.db.postgres.target_write import edge_endpoint_columns, edge_table_ddl

PAIR = ("a", "b")
TABLE = "pair_org_owns_edges"


def test_one_identity_field_keeps_the_id_column() -> None:
    assert edge_endpoint_columns("source", ["code"]) == ["source_id"]


def test_a_composite_identity_gets_a_column_per_field() -> None:
    assert edge_endpoint_columns("target", PAIR) == ["target__a", "target__b"]


def test_the_table_refers_to_the_whole_identity() -> None:
    create, create_without_keys = edge_table_ddl(
        "g",
        TABLE,
        source_table="pair",
        source_fields=PAIR,
        target_table="org",
        target_fields=["oid"],
        properties=[("since", "TEXT")],
    )

    assert create == (
        'CREATE TABLE IF NOT EXISTS "g"."pair_org_owns_edges" ('
        '"id" BIGSERIAL PRIMARY KEY, "source__a" TEXT NOT NULL, '
        '"source__b" TEXT NOT NULL, "target_id" TEXT NOT NULL, "since" TEXT, '
        'FOREIGN KEY ("source__a", "source__b") REFERENCES "g"."pair" ("a", "b"), '
        'FOREIGN KEY ("target_id") REFERENCES "g"."org" ("oid"))'
    )
    assert "FOREIGN KEY" not in create_without_keys


class _Cursor:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        return None

    def fetchone(self) -> tuple[str, None, list[str]]:
        """The catalogue holds the edge key over the endpoints and no stale index."""
        return (f"{TABLE}_edge_key", None, ["source__a", "source__b", "target_id"])


class _Pg:
    def __init__(self, columns: list[str] | None = None) -> None:
        self.config = type("C", (), {"schema_name": "g"})()
        self._columns = columns or []
        self.reads: list[str] = []

        class _Conn:
            def cursor(self) -> _Cursor:
                return _Cursor()

            def commit(self) -> None:
                return None

        self.conn = _Conn()

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        return [{"table_name": t} for t in ("pair", "org", TABLE)]

    def get_table_columns(
        self, table: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        return [{"name": c, "type": "text"} for c in self._columns]

    def read(self, query: str, params: Any = None) -> list[dict[str, Any]]:
        self.reads.append(query)
        return [
            {
                "id": 1,
                "source__a": "x",
                "source__b": "1",
                "target_id": "o1",
                "since": "2020",
            }
        ]


@pytest.fixture
def statements(monkeypatch) -> list[tuple[str, list[tuple]]]:
    recorded: list[tuple[str, list[tuple]]] = []
    monkeypatch.setattr(
        target_write,
        "execute_values",
        lambda cursor, query, values: recorded.append((str(query), values)),
    )
    return recorded


def test_two_endpoints_sharing_a_first_field_stay_two(statements) -> None:
    PostgresConnection.insert_edges_batch(
        cast(Any, _Pg()),
        [
            [{"a": "x", "b": "1"}, {"oid": "o1"}, {}],
            [{"a": "x", "b": "2"}, {"oid": "o1"}, {}],
            [{"a": "x"}, {"oid": "o1"}, {}],
        ],
        "pair",
        "org",
        "owns",
        PAIR,
        ("oid",),
    )

    [(_, values)] = statements
    assert values == [("x", "1", "o1"), ("x", "2", "o1")]


def test_edges_are_removed_by_every_endpoint_field(statements) -> None:
    PostgresConnection.delete_edges(
        cast(Any, _Pg()),
        "pair",
        "org",
        "owns",
        [({"a": "x", "b": 1}, {"oid": "o1"})],
        PAIR,
        ("oid",),
    )

    assert statements == [
        (
            (
                'DELETE FROM "g"."pair_org_owns_edges" '
                'WHERE ("source__a", "source__b", "target_id") IN (VALUES %s)'
            ),
            [("x", "1", "o1")],
        )
    ]


def test_a_vertex_takes_its_edge_rows_with_it(statements) -> None:
    PostgresConnection.delete_vertices(
        cast(Any, _Pg()), "pair", [{"a": "x", "b": "1"}], PAIR
    )

    assert [query for query, _ in statements] == [
        (
            'DELETE FROM "g"."pair_org_owns_edges" WHERE '
            '("source__a"::text, "source__b"::text) IN ('
            'SELECT v."a"::text, v."b"::text FROM "g"."pair" v '
            'WHERE (v."a", v."b") IN (VALUES %s))'
        ),
        'DELETE FROM "g"."pair" WHERE ("a", "b") IN (VALUES %s)',
    ]


def test_an_exported_edge_carries_the_whole_identity() -> None:
    rows = PostgresConnection.fetch_all_edges(
        cast(Any, _Pg()),
        "pair",
        "org",
        "owns",
        match_keys_source=PAIR,
        match_keys_target=("oid",),
    )

    assert rows == [
        [{"a": "x", "b": "1"}, {"oid": "o1"}, {"since": "2020", "relation": "owns"}]
    ]


def test_a_traversal_over_a_composite_endpoint_is_refused() -> None:
    fake = _Pg(columns=["id", "source__a", "source__b", "target_id"])

    with pytest.raises(NotImplementedError, match="composite identity"):
        PostgresConnection.fetch_edges(
            cast(Any, fake),
            "pair",
            "x",
            edge_type=TABLE,
            direction=EdgeDirection.OUT,
        )
    assert fake.reads == []


def test_endpoint_columns_are_not_read_as_edge_properties() -> None:
    fake = _Pg(columns=["id", "source__a", "source__b", "target_id", "since"])

    schema = PostgresConnection.introspect_graph_schema(cast(Any, fake))

    [edge] = schema.core_schema.edge_config.values()
    assert [field.name for field in edge.properties] == ["since"]
