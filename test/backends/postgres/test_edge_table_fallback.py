"""An edge table whose keyed CREATE fails is created without foreign keys, in the same transaction."""

from __future__ import annotations

from typing import Any, Self, cast

import pytest

from graflo.architecture.schema import Schema
from graflo.db.postgres.conn import PostgresConnection

TABLE = "person_org_works_edges"


class _AbortingCursor:
    """Fails the first FOREIGN KEY statement and, like PostgreSQL, every later one
    until the transaction is rolled back to a savepoint."""

    def __init__(self, log: list[str]) -> None:
        self.log = log
        self._aborted = False
        self._last = ""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        text = str(query)
        if self._aborted and not text.startswith("ROLLBACK TO SAVEPOINT"):
            raise RuntimeError("current transaction is aborted")
        self.log.append(text)
        self._last = text
        if text.startswith("ROLLBACK TO SAVEPOINT"):
            self._aborted = False
        elif "FOREIGN KEY" in text:
            self._aborted = True
            raise RuntimeError("there is no unique constraint matching given keys")

    def fetchone(self) -> Any:
        if self._last.startswith("SELECT obj_description("):
            return (None,)
        return (None, None, None)


class _Conn:
    def __init__(self) -> None:
        self.log: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _AbortingCursor:
        return _AbortingCursor(self.log)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _Pg:
    def __init__(self, schema: Schema) -> None:
        self.config = type("C", (), {"schema_name": "g"})()
        self.conn = _Conn()
        self._target_schema = schema


def _schema() -> Schema:
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
                        {"source": "person", "target": "org", "relation": "works"}
                    ]
                },
            },
            "db_profile": {"db_flavor": "postgres"},
        }
    )


def test_a_failed_keyed_create_is_rolled_back_to_a_savepoint_before_the_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = _schema()
    [edge] = schema.core_schema.edge_config.values()
    pg = _Pg(schema)

    with caplog.at_level("WARNING"):
        PostgresConnection._create_edge_table(cast(Any, pg), edge)

    log = pg.conn.log
    assert log[0] == "SAVEPOINT graflo_edge_table"
    assert "FOREIGN KEY" in log[1]
    assert log[2] == "ROLLBACK TO SAVEPOINT graflo_edge_table"
    assert log[3].startswith(f'CREATE TABLE IF NOT EXISTS "g"."{TABLE}"')
    assert "FOREIGN KEY" not in log[3]
    assert "RELEASE SAVEPOINT graflo_edge_table" not in log
    # The key is still built in the same transaction, which then commits.
    assert log[-1].startswith(f'CREATE UNIQUE INDEX IF NOT EXISTS "{TABLE}_edge_key"')
    assert (pg.conn.commits, pg.conn.rollbacks) == (1, 0)
    assert "creating without FK" in caplog.text


def test_an_unkeyed_table_is_created_without_keys_or_savepoint() -> None:
    schema = _schema()
    [edge] = schema.core_schema.edge_config.values()
    pg = _Pg(schema)

    PostgresConnection._create_edge_table(cast(Any, pg), edge, keyed=False)

    log = pg.conn.log
    assert log[0].startswith(f'CREATE TABLE IF NOT EXISTS "g"."{TABLE}"')
    assert "FOREIGN KEY" not in log[0]
    assert not any("graflo_edge_table" in q or "UNIQUE INDEX" in q for q in log)
    assert pg.conn.commits == 1
