"""PostgreSQL bulk writer, without a database: value text, CSV, statements, replay."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any, Self
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from graflo.architecture.schema import Schema
from graflo.connections.bulk import BulkConfig
from graflo.db.bulk import BulkEdgeChunk, BulkVertexChunk
from graflo.db.conn import SchemaExistsError
from graflo.db.postgres import bulk as pg_bulk
from graflo.db.postgres.bulk import (
    LOAD_LOG,
    SessionText,
    add_foreign_keys_sql,
    column_text,
    copy_sql,
    csv_text,
    edge_copy_rows,
    load_log_ddl,
    should_write,
    vertex_copy_rows,
)
from graflo.db.postgres.conn import PostgresConnection
from graflo.db.postgres.target_write import (
    edge_foreign_keys,
    vertex_key_ddl,
    vertex_table_ddl,
)

# -- value text ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (None, None),
        ("", ""),
        ("a,b", "a,b"),
        ('say "hi"\nbye', 'say "hi"\nbye'),
        (1, "1"),
        (-12, "-12"),
        (10**20, "100000000000000000000"),
        (True, "true"),
        (False, "false"),
        # Floats are sent as numeric literals, which print positionally.
        (1.5, "1.5"),
        (2.0, "2.0"),
        (1e-07, "0.0000001"),
        (1e20, "100000000000000000000"),
        (-0.0, "0.0"),
        (float("nan"), "NaN"),
        (float("inf"), "Infinity"),
        (float("-inf"), "-Infinity"),
        (Decimal("1E+2"), "100"),
        (Decimal("1.50"), "1.50"),
        (dt.date(2020, 1, 2), "2020-01-02"),
        (dt.datetime(2020, 1, 2, 3, 4, 5), "2020-01-02 03:04:05"),
        (dt.datetime(2020, 1, 2, 3, 4, 5, 500000), "2020-01-02 03:04:05.5"),
        (dt.time(1, 2, 3, 40), "01:02:03.00004"),
        (
            dt.time(1, 2, 3, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))),
            "01:02:03+05:30",
        ),
        (dt.time(1, 2, 3, tzinfo=dt.timezone(dt.timedelta(hours=-2))), "01:02:03-02"),
    ],
)
def test_a_scalar_is_written_as_the_text_its_column_holds(value, text) -> None:
    assert column_text(value) == text


@pytest.mark.parametrize(
    ("zone", "text"),
    [
        ("UTC", "2020-01-02 03:04:05+00"),
        ("Europe/Paris", "2020-01-02 04:04:05+01"),
        ("Asia/Kolkata", "2020-01-02 08:34:05+05:30"),
    ],
)
def test_a_datetime_with_a_zone_prints_in_the_session_zone(zone, text) -> None:
    value = dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.UTC)

    assert column_text(value, tz=ZoneInfo(zone)) == text


def test_a_datetime_with_a_zone_needs_the_session_zone() -> None:
    with pytest.raises(ValueError, match="TimeZone"):
        column_text(dt.datetime(2020, 1, 2, tzinfo=dt.UTC))


def test_dates_need_an_iso_datestyle() -> None:
    with pytest.raises(ValueError, match="DateStyle"):
        column_text(dt.date(2020, 1, 2), iso=False)


@pytest.mark.parametrize("value", [{"a": 1}, UUID(int=1), [{"a": 1}]])
def test_a_value_psycopg2_cannot_adapt_is_refused(value) -> None:
    with pytest.raises(TypeError, match="cannot store"):
        column_text(value, array=True)


@pytest.mark.parametrize(
    ("value", "array", "text"),
    [
        ([], True, "{}"),
        (["x", None, "a b", "", "NULL", 'q"\\'], True, '{x,NULL,"a b","","NULL","q\\"\\\\"}'),
        (["{a}", "c,d"], True, '{"{a}","c,d"}'),
        ([1, 2], True, "{1,2}"),
        ([1.5, 1e-07], True, "{1.5,0.0000001}"),
        ([[1, 2], [3, 4]], True, "{{1,2},{3,4}}"),
        ([dt.date(2020, 1, 2), dt.datetime(2020, 1, 2, 3)], True,
         '{2020-01-02,"2020-01-02 03:00:00"}'),
        # Booleans print as the element type does: text in a text array,
        # ``bool[]`` output in a list stored in a text column.
        ([True, False], True, "{true,false}"),
        ([True, False], False, "{t,f}"),
    ],
)  # fmt: skip
def test_a_list_is_written_as_an_array_literal(value, array, text) -> None:
    assert column_text(value, array=array) == text


def test_csv_quotes_every_value_and_leaves_null_empty() -> None:
    rows = [["a,b", 'say "hi"', None, ""], ["line\nbreak", "\\.", "x", None]]

    assert csv_text(rows) == ('"a,b","say ""hi""",,""\n"line\nbreak","\\.","x",\n')


# -- rows ----------------------------------------------------------------------


def test_vertex_rows_write_every_field_and_drop_rows_without_a_key(caplog) -> None:
    chunk = BulkVertexChunk(
        chunk_id="v:0",
        class_name="Person",
        match_keys=["pid"],
        rows=[
            {"pid": "p1", "name": "Ann", "tags": ["x", None], "_key": "k"},
            {"pid": 2, "nick": None},
            {"name": "keyless"},
        ],
    )

    with caplog.at_level("WARNING"):
        columns, rows = vertex_copy_rows(chunk, frozenset({"tags"}), SessionText())

    assert columns == ["name", "nick", "pid", "tags"]
    assert rows == [["Ann", None, "p1", "{x,NULL}"], [None, None, "2", None]]
    assert "1 row(s)" in caplog.text


def test_edge_rows_store_endpoint_identities_then_properties() -> None:
    chunk = BulkEdgeChunk(
        chunk_id="e:0",
        source_class="Person",
        target_class="City",
        relation_name="lives_in",
        match_keys_source=["pid"],
        match_keys_target=["country", "cid"],
        collection_name=None,
        rows=[
            ({"pid": "p1"}, {"country": "fr", "cid": 1}, {"role": "home", "_k": 1}),
            ({"pid": "p2"}, {"country": "fr", "cid": 2}, {"since": 2001}),
            ({"pid": None}, {"country": "fr", "cid": 2}, {}),
        ],
    )

    columns, rows = edge_copy_rows(chunk, frozenset(), SessionText())

    assert columns == ["source_id", "target__country", "target__cid", "role", "since"]
    assert rows == [["p1", "fr", "1", "home", None], ["p2", "fr", "2", None, "2001"]]


# -- statements ----------------------------------------------------------------


def _schema() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "hr"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": [
                                "id",
                                "name",
                                {"name": "tags", "type": "LIST", "item_type": "STRING"},
                            ],
                            "identity": ["id"],
                        },
                        {"name": "org", "properties": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "org",
                            "relation": "works",
                            "properties": ["since"],
                            "identities": [["source", "target", "since"]],
                        }
                    ]
                },
            },
            "db_profile": {"db_flavor": "postgres"},
        }
    )


EDGE = "person_org_works_edges"


def test_vertex_table_ddl_defers_only_the_primary_key() -> None:
    [person, _] = _schema().core_schema.vertex_config.vertices

    keyed, keyless = vertex_table_ddl("g", person)

    columns = '"id" TEXT, "name" TEXT, "tags" TEXT[]'
    assert keyed == (
        f'CREATE TABLE IF NOT EXISTS "g"."person" ({columns}, PRIMARY KEY ("id"))'
    )
    assert keyless == f'CREATE TABLE IF NOT EXISTS "g"."person" ({columns})'
    assert vertex_key_ddl("g", person) == (
        'ALTER TABLE "g"."person" ADD PRIMARY KEY ("id")'
    )


def test_bulk_statements() -> None:
    assert copy_sql("g", "person", ["id", 'na"me']) == (
        'COPY "g"."person" ("id", "na""me") FROM STDIN WITH (FORMAT csv)'
    )
    assert load_log_ddl("g") == (
        f'DROP TABLE IF EXISTS "g"."{LOAD_LOG}"',
        f'CREATE TABLE IF NOT EXISTS "g"."{LOAD_LOG}" (chunk_id TEXT PRIMARY KEY)',
    )
    keys = edge_foreign_keys(
        "g",
        source_table="person",
        source_fields=["id"],
        target_table="org",
        target_fields=["a", "b"],
    )
    assert keys == [
        'FOREIGN KEY ("source_id") REFERENCES "g"."person" ("id")',
        'FOREIGN KEY ("target__a", "target__b") REFERENCES "g"."org" ("a", "b")',
    ]
    assert add_foreign_keys_sql("g", EDGE, keys) == (
        f'ALTER TABLE "g"."{EDGE}" ADD {keys[0]} NOT VALID, ADD {keys[1]} NOT VALID'
    )


@pytest.mark.parametrize(
    ("replay", "logged", "write"),
    [(False, False, True), (True, False, True), (True, True, False)],
)
def test_a_chunk_is_skipped_only_when_a_replay_finds_it_logged(
    replay, logged, write
) -> None:
    assert should_write(replay, logged) is write


# -- the writer, against a scripted connection ---------------------------------


class _Cursor:
    def __init__(self, db: _Db) -> None:
        self.db = db
        self._result: list[Any] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        text = str(query)
        if self.db.fail_on is not None and self.db.fail_on in text:
            raise RuntimeError(f"failed: {self.db.fail_on}")
        self.db.executed.append(text)
        self._result = self.db.answer(text, params)

    def copy_expert(self, query: str, file: Any) -> None:
        if self.db.fail_on is not None and self.db.fail_on in query:
            raise RuntimeError(f"failed: {self.db.fail_on}")
        self.db.executed.append(query)
        self.db.copied.append(file.read())

    def fetchone(self) -> Any:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[Any]:
        return self._result


class _Db:
    def __init__(self, logged: set[str] | None = None) -> None:
        self.executed: list[str] = []
        self.copied: list[str] = []
        self.logged = set(logged or ())
        self.commits = 0
        self.rollbacks = 0
        self.fail_on: str | None = None

    def answer(self, text: str, params: Any) -> list[Any]:
        if text.startswith("SELECT current_setting('TimeZone')"):
            return [("UTC", "ISO, MDY")]
        if text.startswith(f'SELECT 1 FROM "g"."{LOAD_LOG}"'):
            return [(1,)] if params[0] in self.logged else []
        if text.startswith(f'INSERT INTO "g"."{LOAD_LOG}"'):
            self.logged.add(params[0])
        if text.startswith("SELECT a.attname"):
            return [("tags",)]
        if "contype = 'f' AND NOT convalidated" in text:
            return [("fk_a",), ("fk_b",)]
        if text.startswith("SELECT count(*)"):
            return [(3,)]
        return []

    def cursor(self, *args: Any, **kwargs: Any) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


def _pg(db: _Db) -> PostgresConnection:
    pg = PostgresConnection.__new__(PostgresConnection)
    pg.config = type("C", (), {"schema_name": "g"})()
    pg.conn = db
    return pg


def _vertices(replay: bool = False, chunk_id: str = "v:0") -> BulkVertexChunk:
    return BulkVertexChunk(
        chunk_id=chunk_id,
        class_name="person",
        match_keys=["id"],
        rows=[{"id": "p1", "tags": ["a b"]}],
        replay=replay,
    )


def _copies(db: _Db) -> list[str]:
    return [q for q in db.executed if q.startswith("COPY")]


def test_a_chunk_is_copied_and_logged_in_one_transaction() -> None:
    db = _Db()
    pg = _pg(db)

    pg.bulk_write_vertices(_schema(), _vertices())
    pg.bulk_write_vertices(_schema(), _vertices(chunk_id="v:1"))

    assert (
        _copies(db)
        == ['COPY "g"."person" ("id", "tags") FROM STDIN WITH (FORMAT csv)'] * 2
    )
    assert db.copied == ['"p1","{""a b""}"\n'] * 2
    assert db.logged == {"v:0", "v:1"}
    assert not any(q.startswith(f'SELECT 1 FROM "g"."{LOAD_LOG}"') for q in db.executed)
    # The session is set up once per connection, its array columns read once per table.
    assert db.executed.count("SET synchronous_commit = off") == 1
    assert sum(q.startswith("SELECT a.attname") for q in db.executed) == 1
    # One commit for the session, one per chunk.
    assert (db.commits, db.rollbacks) == (3, 0)


def test_a_replayed_chunk_found_in_the_log_is_not_written_again() -> None:
    db = _Db(logged={"v:0"})

    _pg(db).bulk_write_vertices(_schema(), _vertices(replay=True))

    assert _copies(db) == [] and db.copied == []
    assert db.rollbacks == 0


def test_a_replayed_chunk_missing_from_the_log_is_written() -> None:
    db = _Db()

    _pg(db).bulk_write_vertices(_schema(), _vertices(replay=True))

    assert len(_copies(db)) == 1
    assert db.logged == {"v:0"}


def test_a_failed_copy_is_rolled_back_with_its_log_row() -> None:
    db = _Db()
    db.fail_on = "COPY"

    with pytest.raises(RuntimeError, match="failed: COPY"):
        _pg(db).bulk_write_vertices(_schema(), _vertices())

    assert db.rollbacks == 1
    assert not any(q.startswith(f'INSERT INTO "g"."{LOAD_LOG}"') for q in db.executed)


def test_edges_are_copied_into_their_table() -> None:
    db = _Db()
    chunk = BulkEdgeChunk(
        chunk_id="e:0",
        source_class="person",
        target_class="org",
        relation_name="works",
        match_keys_source=["id"],
        match_keys_target=["id"],
        collection_name=None,
        rows=[({"id": "p1"}, {"id": "o1"}, {"since": 2001})],
    )

    _pg(db).bulk_write_edges(_schema(), chunk)

    copy = (
        f'COPY "g"."{EDGE}" ("source_id", "target_id", "since") '
        "FROM STDIN WITH (FORMAT csv)"
    )
    assert _copies(db) == [copy]
    assert db.copied == ['"p1","o1","2001"\n']


def test_begin_refuses_a_namespace_holding_the_schema() -> None:
    pg = _pg(_Db())
    pg.__dict__["_pg_schema_exists"] = lambda name: True
    pg.__dict__["get_tables"] = lambda schema_name=None: [
        {"table_name": "person", "description": None}
    ]

    with pytest.raises(SchemaExistsError):
        pg.bulk_begin(_schema(), BulkConfig())


def test_begin_creates_keyless_tables_and_an_empty_log() -> None:
    db = _Db()
    pg = _pg(db)
    pg.__dict__["_pg_schema_exists"] = lambda name: True
    pg.__dict__["get_tables"] = lambda schema_name=None: []

    pg.bulk_begin(_schema(), BulkConfig())

    creates = [q for q in db.executed if q.startswith("CREATE TABLE")]
    assert [q.split(" (")[0] for q in creates] == [
        'CREATE TABLE IF NOT EXISTS "g"."person"',
        'CREATE TABLE IF NOT EXISTS "g"."org"',
        f'CREATE TABLE IF NOT EXISTS "g"."{EDGE}"',
        f'CREATE TABLE IF NOT EXISTS "g"."{LOAD_LOG}"',
    ]
    assert not any("PRIMARY KEY (" in q or "FOREIGN KEY" in q for q in creates[:3])
    assert not any("UNIQUE INDEX" in q for q in db.executed)
    assert db.executed.index(f'DROP TABLE IF EXISTS "g"."{LOAD_LOG}"') < (
        db.executed.index(creates[-1])
    )


def test_a_resumed_begin_keeps_the_log_and_skips_the_guard() -> None:
    db = _Db()
    pg = _pg(db)
    pg.__dict__["_pg_schema_exists"] = lambda name: True
    pg.__dict__["get_tables"] = lambda schema_name=None: [
        {"table_name": "person", "description": None}
    ]

    pg.bulk_begin(_schema(), BulkConfig(), resume=True)

    assert f'DROP TABLE IF EXISTS "g"."{LOAD_LOG}"' not in db.executed


def test_prepare_adds_the_vertex_primary_keys() -> None:
    db = _Db()

    _pg(db).bulk_prepare_edges(_schema())

    assert [q for q in db.executed if q.startswith("ALTER TABLE")] == [
        'ALTER TABLE "g"."person" ADD PRIMARY KEY ("id")',
        'ALTER TABLE "g"."org" ADD PRIMARY KEY ("id")',
    ]


def test_finish_keys_edges_then_validates_foreign_keys_then_drops_the_log() -> None:
    db = _Db()

    counts = _pg(db).bulk_finish(_schema())

    key = next(i for i, q in enumerate(db.executed) if f'"{EDGE}_edge_key"' in q)
    add = next(i for i, q in enumerate(db.executed) if "NOT VALID" in q)
    validates = [i for i, q in enumerate(db.executed) if "VALIDATE CONSTRAINT" in q]
    assert db.executed[key].endswith(
        '("source_id", "target_id", "since") NULLS NOT DISTINCT'
    )
    assert key < add < validates[0]
    assert [db.executed[i] for i in validates] == [
        f'ALTER TABLE "g"."{EDGE}" VALIDATE CONSTRAINT "fk_a"',
        f'ALTER TABLE "g"."{EDGE}" VALIDATE CONSTRAINT "fk_b"',
    ]
    assert db.executed[-1] == f'DROP TABLE IF EXISTS "g"."{LOAD_LOG}"'
    assert counts.vertices == {"person": 3, "org": 3}
    assert counts.edges == {("person", "works", "org"): 3}


def test_a_failed_edge_key_is_raised_not_worked_around() -> None:
    db = _Db()
    db.fail_on = "_edge_key"

    with pytest.raises(RuntimeError, match="duplicate edge rows"):
        _pg(db).bulk_finish(_schema())

    assert db.rollbacks == 1
    assert not any("FOREIGN KEY" in q for q in db.executed)


def test_postgres_declares_a_bulk_writer() -> None:
    assert PostgresConnection.supports_bulk_load is True
    assert PostgresConnection.bulk_dense_ids is False
    assert pg_bulk.PostgresBulkMixin in PostgresConnection.__mro__
