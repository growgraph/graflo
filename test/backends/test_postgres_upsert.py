"""PostgreSQL batch writes leave absent columns alone and fuse duplicate keys."""

from __future__ import annotations

from typing import Any, Self, cast

import pytest
from psycopg2 import sql

from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.db.postgres import target_write
from graflo.db.postgres.conn import PostgresConnection
from graflo.db.postgres.target_write import edge_key_ddl

EDGE_TABLE = "person_org_works_edges"


def _render(query: Any) -> str:
    """*query* as SQL text, rendered without a live connection."""
    if isinstance(query, sql.Composed):
        return "".join(_render(part) for part in query.seq)
    if isinstance(query, sql.Identifier):
        return ".".join('"' + s.replace('"', '""') + '"' for s in query.strings)
    if isinstance(query, sql.SQL):
        return query.string
    return str(query)


class _Cursor:
    def __init__(
        self,
        executed: list[str],
        fail_on: str | None,
        catalog: tuple[str | None, str | None],
    ) -> None:
        self.executed = executed
        self._fail_on = fail_on
        self._catalog = catalog

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        text = _render(query)
        if self._fail_on is not None and self._fail_on in text:
            raise RuntimeError("could not create unique index")
        self.executed.append(text)

    def fetchone(self) -> tuple[str | None, str | None]:
        return self._catalog


class _Conn:
    def __init__(
        self,
        fail_on: str | None = None,
        catalog: tuple[str | None, str | None] = (None, None),
    ) -> None:
        self.executed: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self._fail_on = fail_on
        self._catalog = catalog

    def cursor(self) -> _Cursor:
        return _Cursor(self.executed, self._fail_on, self._catalog)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _Pg:
    def __init__(
        self,
        schema: Schema | None = None,
        fail_on: str | None = None,
        catalog: tuple[str | None, str | None] = (None, None),
    ) -> None:
        self.config = type("C", (), {"schema_name": "g"})()
        self.conn = _Conn(fail_on, catalog)
        if schema is not None:
            self._target_schema = schema


@pytest.fixture
def statements(monkeypatch) -> list[tuple[str, list[tuple]]]:
    recorded: list[tuple[str, list[tuple]]] = []
    monkeypatch.setattr(
        target_write,
        "execute_values",
        lambda cursor, query, values: recorded.append((_render(query), values)),
    )
    return recorded


@pytest.fixture
def logged_writes(monkeypatch) -> None:
    """Record each ``VALUES`` write among the statements its cursor executed."""
    monkeypatch.setattr(
        target_write,
        "execute_values",
        lambda cursor, query, values: cursor.executed.append(_render(query)),
    )


@pytest.fixture
def failing_write(monkeypatch) -> None:
    def _fail(cursor: Any, query: Any, values: Any) -> None:
        raise RuntimeError("write failed")

    monkeypatch.setattr(target_write, "execute_values", _fail)


def _upsert(pg: _Pg, docs: list[dict[str, Any]]) -> None:
    PostgresConnection.upsert_docs_batch(cast(Any, pg), docs, "person", ("id",))


def _insert_edges(pg: _Pg, edges: list[Any], **kwargs: Any) -> None:
    PostgresConnection.insert_edges_batch(
        cast(Any, pg), edges, "person", "org", "works", ("id",), ("id",), **kwargs
    )


# -- vertices -----------------------------------------------------------------


def test_each_row_shape_is_written_by_its_own_statement(statements) -> None:
    pg = _Pg()

    _upsert(
        pg,
        [
            {"id": "1", "name": "a"},
            {"id": "2", "age": 3},
            {"id": "3", "name": "c", "_key": "k3"},
        ],
    )

    assert statements == [
        (
            (
                'INSERT INTO "g"."person" ("id", "name") VALUES %s '
                'ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"'
            ),
            [("1", "a"), ("3", "c")],
        ),
        (
            (
                'INSERT INTO "g"."person" ("age", "id") VALUES %s '
                'ON CONFLICT ("id") DO UPDATE SET "age" = EXCLUDED."age"'
            ),
            [(3, "2")],
        ),
    ]
    assert pg.conn.commits == 1


def test_a_column_a_row_lacks_is_never_written_for_it(statements) -> None:
    _upsert(_Pg(), [{"id": "1", "name": "a"}, {"id": "2", "age": 3}])

    for query, _ in statements:
        assert ("name" in query) != ("age" in query)


def test_duplicate_keys_fuse_into_one_row_the_later_winning(statements) -> None:
    _upsert(
        _Pg(),
        [
            {"id": "1", "name": "a", "age": 1},
            {"id": "2", "name": "z", "age": 9},
            {"id": "1", "name": "b"},
        ],
    )

    [(_, values)] = statements
    assert values == [(1, "1", "b"), (9, "2", "z")]


def test_keys_equal_as_text_fuse(statements) -> None:
    _upsert(_Pg(), [{"id": 1, "name": "a"}, {"id": "1", "age": 2}])

    [(_, values)] = statements
    assert values == [(2, "1", "a")]


def test_a_row_with_an_incomplete_key_is_dropped_with_a_warning(
    statements, caplog
) -> None:
    with caplog.at_level("WARNING", logger=target_write.logger.name):
        _upsert(
            _Pg(),
            [{"name": "a"}, {"id": None, "name": "n"}, {"id": "1", "name": "b"}],
        )

    [(_, values)] = statements
    assert values == [("1", "b")]
    [record] = caplog.records
    assert "2 row(s)" in record.getMessage()
    assert "person" in record.getMessage()


def test_a_row_of_key_columns_only_does_nothing_on_conflict(statements) -> None:
    _upsert(_Pg(), [{"id": "1"}])

    assert statements == [
        (
            'INSERT INTO "g"."person" ("id") VALUES %s ON CONFLICT ("id") DO NOTHING',
            [("1",)],
        )
    ]


def test_a_failed_vertex_write_is_rolled_back_and_raised(failing_write) -> None:
    pg = _Pg()

    with pytest.raises(RuntimeError, match="write failed"):
        _upsert(pg, [{"id": "1", "name": "a"}])

    assert (pg.conn.rollbacks, pg.conn.commits) == (1, 0)


# -- edges --------------------------------------------------------------------


def test_edges_are_ignored_on_their_key_by_default(statements) -> None:
    _insert_edges(
        _Pg(),
        [[{"id": "p1"}, {"id": "o1"}, {"since": "2020", "w": 1}]],
        relationship_merge_properties=("since",),
    )

    assert statements == [
        (
            (
                f'INSERT INTO "g"."{EDGE_TABLE}" '
                '("source_id", "target_id", "since", "w") VALUES %s '
                'ON CONFLICT ("source_id", "target_id", "since") DO NOTHING'
            ),
            [("p1", "o1", "2020", 1)],
        )
    ]


def test_upserted_edges_update_their_other_weights(statements) -> None:
    _insert_edges(
        _Pg(),
        [[{"id": "p1"}, {"id": "o1"}, {"since": "2020", "w": 1}]],
        relationship_merge_properties=("since",),
        on_duplicate="upsert",
    )

    [(query, _)] = statements
    assert query.endswith(
        'ON CONFLICT ("source_id", "target_id", "since") '
        'DO UPDATE SET "w" = EXCLUDED."w"'
    )


def test_an_upserted_edge_with_only_key_weights_does_nothing(statements) -> None:
    _insert_edges(
        _Pg(),
        [[{"id": "p1"}, {"id": "o1"}, {"since": "2020"}]],
        relationship_merge_properties=("since",),
        on_duplicate="upsert",
    )

    [(query, _)] = statements
    assert query.endswith('ON CONFLICT ("source_id", "target_id", "since") DO NOTHING')


def test_edges_without_merge_properties_conflict_on_their_endpoints(
    statements,
) -> None:
    _insert_edges(_Pg(), [[{"id": "p1"}, {"id": "o1"}, {"w": 1}]])

    [(query, _)] = statements
    assert query.endswith('ON CONFLICT ("source_id", "target_id") DO NOTHING')


def test_duplicate_edges_fuse_the_later_weights_winning(statements) -> None:
    _insert_edges(
        _Pg(),
        [
            [{"id": "p1"}, {"id": "o1"}, {"since": "2020", "w": 1, "x": "keep"}],
            [{"id": "p1"}, {"id": "o1"}, {"since": "2021", "w": 5, "x": "other"}],
            [{"id": 1}, {"id": "o1"}, {"since": "2020", "w": 1, "x": "a"}],
            [{"id": "1"}, {"id": "o1"}, {"since": 2020, "w": 2}],
            [{"id": "p1"}, {"id": "o1"}, {"since": "2020", "w": 2}],
        ],
        relationship_merge_properties=("since",),
        on_duplicate="upsert",
    )

    [(_, values)] = statements
    assert values == [
        ("p1", "o1", "2020", 2, "keep"),
        ("p1", "o1", "2021", 5, "other"),
        ("1", "o1", 2020, 2, "a"),
    ]


def test_edge_weight_shapes_are_written_apart_without_private_keys(
    statements,
) -> None:
    _insert_edges(
        _Pg(),
        [
            [{"id": "p1"}, {"id": "o1"}, {"w": 1, "_key": "k"}],
            [{"id": "p2"}, {"id": "o1"}, {"note": "n"}],
            [{"id": None}, {"id": "o1"}, {"w": 3}],
        ],
        on_duplicate="upsert",
    )

    assert statements == [
        (
            (
                f'INSERT INTO "g"."{EDGE_TABLE}" ("source_id", "target_id", "w") '
                'VALUES %s ON CONFLICT ("source_id", "target_id") '
                'DO UPDATE SET "w" = EXCLUDED."w"'
            ),
            [("p1", "o1", 1)],
        ),
        (
            (
                f'INSERT INTO "g"."{EDGE_TABLE}" ("source_id", "target_id", "note") '
                'VALUES %s ON CONFLICT ("source_id", "target_id") '
                'DO UPDATE SET "note" = EXCLUDED."note"'
            ),
            [("p2", "o1", "n")],
        ),
    ]


def test_a_failed_edge_write_is_rolled_back_and_raised(failing_write) -> None:
    pg = _Pg()

    with pytest.raises(RuntimeError, match="write failed"):
        _insert_edges(pg, [[{"id": "p1"}, {"id": "o1"}, {}]])

    assert (pg.conn.rollbacks, pg.conn.commits) == (1, 0)


# -- edge table key -----------------------------------------------------------


def test_the_edge_key_ddl_drops_the_stale_index_and_creates_the_key() -> None:
    assert edge_key_ddl("g", EDGE_TABLE, ["source_id", "target_id"]) == (
        f'DROP INDEX IF EXISTS "g"."{EDGE_TABLE}_edge_uniq"',
        (
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{EDGE_TABLE}_edge_key" ON '
            f'"g"."{EDGE_TABLE}" ("source_id", "target_id") NULLS NOT DISTINCT'
        ),
    )


def test_an_edge_table_without_properties_still_has_a_key() -> None:
    schema = _schema([])
    edge = _edge(schema).model_copy(update={"properties": []})
    pg = _Pg(schema)

    PostgresConnection._create_edge_table(cast(Any, pg), edge)

    assert pg.conn.executed[-1].endswith(
        '("source_id", "target_id") NULLS NOT DISTINCT'
    )


def _schema(identities: list[list[str]]) -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "hr"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "properties": ["id"], "identity": ["id"]},
                        {"name": "org", "properties": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "org",
                            "relation": "works",
                            "properties": ["since", "note"],
                            "identities": identities,
                        }
                    ]
                },
            },
            "db_profile": {"db_flavor": "postgres"},
        }
    )


def _edge(schema: Schema) -> Edge:
    [edge] = schema.core_schema.edge_config.values()
    return edge


def test_the_edge_table_is_keyed_on_the_schema_merge_properties() -> None:
    schema = _schema([["source", "target", "since"]])
    pg = _Pg(schema)

    PostgresConnection._create_edge_table(cast(Any, pg), _edge(schema))

    assert pg.conn.executed[1:] == [
        f'DROP INDEX IF EXISTS "g"."{EDGE_TABLE}_edge_uniq"',
        (
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{EDGE_TABLE}_edge_key" ON '
            f'"g"."{EDGE_TABLE}" ("source_id", "target_id", "since") NULLS NOT DISTINCT'
        ),
    ]
    assert pg.conn.commits == 1


@pytest.mark.parametrize(
    ("identity", "key"),
    [
        (["source", "target", "relation"], '("source_id", "target_id")'),
        (
            ["source", "target", "relation", "since"],
            '("source_id", "target_id", "since")',
        ),
    ],
)
def test_the_relation_token_adds_no_key_column(identity, key) -> None:
    schema = _schema([identity])
    pg = _Pg(schema)

    PostgresConnection._create_edge_table(cast(Any, pg), _edge(schema))

    assert pg.conn.executed[-1].endswith(f"{key} NULLS NOT DISTINCT")


def test_without_a_schema_the_edge_table_is_keyed_on_all_properties() -> None:
    pg = _Pg()

    PostgresConnection._create_edge_table(cast(Any, pg), _edge(_schema([])))

    assert pg.conn.executed[-1].endswith(
        '("source_id", "target_id", "since", "note") NULLS NOT DISTINCT'
    )


def test_an_edge_table_holding_duplicates_is_refused_with_a_clear_error() -> None:
    schema = _schema([])
    pg = _Pg(schema, fail_on="_edge_key")

    with pytest.raises(RuntimeError, match=f"{EDGE_TABLE}.*duplicate edge rows"):
        PostgresConnection._create_edge_table(cast(Any, pg), _edge(schema))

    assert (pg.conn.rollbacks, pg.conn.commits) == (1, 0)


# -- edge key on write -----------------------------------------------------------

LOCK = f'LOCK TABLE "g"."{EDGE_TABLE}" IN SHARE ROW EXCLUSIVE MODE'
DROP_STALE = f'DROP INDEX IF EXISTS "g"."{EDGE_TABLE}_edge_uniq"'


def _write_since(pg: _Pg) -> None:
    _insert_edges(
        pg,
        [[{"id": "p1"}, {"id": "o1"}, {"since": "2020", "w": 1}]],
        relationship_merge_properties=("since",),
    )


def _columns(statement: str, after: str) -> list[str]:
    """The parenthesised column list following *after* in *statement*."""
    inner = statement.split(after, 1)[1].split("(", 1)[1].split(")", 1)[0]
    return [column.strip() for column in inner.split(",")]


def test_the_first_edge_write_gives_an_unkeyed_table_its_key_first(
    logged_writes,
) -> None:
    pg = _Pg()

    _write_since(pg)

    probe, lock, drop, create, insert = pg.conn.executed
    assert probe.startswith("SELECT to_regclass(")
    assert (lock, drop) == (LOCK, DROP_STALE)
    assert create == (
        f'CREATE UNIQUE INDEX IF NOT EXISTS "{EDGE_TABLE}_edge_key" ON '
        f'"g"."{EDGE_TABLE}" ("source_id", "target_id", "since") NULLS NOT DISTINCT'
    )
    assert insert.startswith(f'INSERT INTO "g"."{EDGE_TABLE}"')
    assert (pg.conn.commits, pg.conn.rollbacks) == (1, 0)


def test_the_edge_key_columns_are_the_conflict_target(logged_writes) -> None:
    pg = _Pg()

    _write_since(pg)

    create, insert = pg.conn.executed[3], pg.conn.executed[4]
    assert _columns(create, " ON ") == _columns(insert, "ON CONFLICT")


def test_a_second_edge_write_on_the_connection_issues_no_ddl(logged_writes) -> None:
    pg = _Pg()
    _write_since(pg)
    first = len(pg.conn.executed)

    _write_since(pg)

    [insert] = pg.conn.executed[first:]
    assert insert.startswith("INSERT INTO")


def test_a_table_already_keyed_is_written_without_ddl(logged_writes) -> None:
    pg = _Pg(catalog=(f"{EDGE_TABLE}_edge_key", None))

    _write_since(pg)

    probe, insert = pg.conn.executed
    assert probe.startswith("SELECT to_regclass(")
    assert insert.startswith("INSERT INTO")


def test_a_stale_index_beside_the_key_is_still_dropped(logged_writes) -> None:
    pg = _Pg(catalog=(f"{EDGE_TABLE}_edge_key", f"{EDGE_TABLE}_edge_uniq"))

    _write_since(pg)

    assert DROP_STALE in pg.conn.executed


def test_an_edge_table_with_duplicates_is_refused_on_write(logged_writes) -> None:
    pg = _Pg(fail_on="_edge_key")

    with pytest.raises(RuntimeError, match=f"{EDGE_TABLE}.*duplicate edge rows"):
        _write_since(pg)

    assert (pg.conn.rollbacks, pg.conn.commits) == (1, 0)
    assert not any(s.startswith("INSERT") for s in pg.conn.executed)


def test_a_refused_edge_key_is_tried_again_on_the_next_write(logged_writes) -> None:
    pg = _Pg(fail_on="_edge_key")
    with pytest.raises(RuntimeError):
        _write_since(pg)
    first = len(pg.conn.executed)

    with pytest.raises(RuntimeError, match="duplicate edge rows"):
        _write_since(pg)

    assert pg.conn.executed[first:] == [pg.conn.executed[0], LOCK, DROP_STALE]


def test_a_dry_edge_write_issues_no_ddl(logged_writes) -> None:
    pg = _Pg()

    _insert_edges(pg, [[{"id": "p1"}, {"id": "o1"}, {}]], dry=True)

    assert pg.conn.executed == []


# -- identifier length -----------------------------------------------------------


def test_an_index_name_within_63_bytes_is_unchanged() -> None:
    name = "ix_" + "a" * 60
    assert target_write._pg_index_name(name) == name


def test_a_long_index_name_is_shortened_by_a_digest_stably() -> None:
    name = "ix_person_" + "a" * 70

    short = target_write._pg_index_name(name)

    assert len(short.encode("utf-8")) <= 63
    assert short == target_write._pg_index_name(name)
    assert short.startswith("ix_person_aaa")
    assert len(short.rsplit("_", 1)[1]) == 8


def test_long_index_names_sharing_63_bytes_stay_apart() -> None:
    prefix = "ix_person_" + "a" * 60
    first = target_write._pg_index_name(prefix + "__x_id")
    second = target_write._pg_index_name(prefix + "__y_id")

    assert first != second


def test_a_long_multibyte_index_name_is_cut_on_a_character_boundary() -> None:
    short = target_write._pg_index_name("ix_" + "é" * 40)

    assert len(short.encode("utf-8")) <= 63
    assert short.startswith("ix_é")


def _long_secondaries_schema() -> Schema:
    """``person`` with two secondaries over fields sharing a 60-byte prefix."""
    prefix = "a" * 60
    return Schema.model_validate(
        {
            "metadata": {"name": "hr"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": ["id", f"{prefix}__x_id", f"{prefix}__y_id"],
                            "identity": ["id"],
                            "secondary_identities": [
                                {"name": "x", "fields": [f"{prefix}__x_id"]},
                                {"name": "y", "fields": [f"{prefix}__y_id"]},
                            ],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {"db_flavor": "postgres"},
        }
    )


def test_secondary_indexes_over_long_fields_get_distinct_short_names() -> None:
    schema = _long_secondaries_schema()
    pg = _Pg(schema)

    PostgresConnection.define_vertex_indexes(
        cast(Any, pg), schema.core_schema.vertex_config, schema
    )

    names = [statement.split('"')[1] for statement in pg.conn.executed]
    assert len(names) == 2
    assert len(set(names)) == 2
    assert all(len(name.encode("utf-8")) <= 63 for name in names)


def test_long_edge_index_names_are_shortened() -> None:
    table = "p" * 60
    key = target_write._edge_key_index_name(table)

    assert len(key.encode("utf-8")) <= 63
    assert key == target_write._pg_index_name(f"{table}_edge_key")


def test_the_stale_unique_index_is_dropped_by_the_name_postgres_stored() -> None:
    """Created under its whole name, it is stored under that name's first 63 bytes."""
    table = "p" * 60

    assert target_write._edge_unique_index_name(table) == f"{table}_ed"
