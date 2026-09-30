"""SQLDataSource against SQLite: the row limit reaches the database, errors surface."""

from __future__ import annotations

import pathlib

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import SQLAlchemyError

from graflo.data_source.sql import SQLConfig, SQLDataSource


@pytest.fixture()
def connection_string(tmp_path: pathlib.Path) -> str:
    conn_str = f"sqlite:///{tmp_path / 'events.db'}"
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT)"))
        conn.execute(
            text(
                "INSERT INTO events (id, kind) VALUES "
                "(1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'a')"
            )
        )
        conn.commit()
    engine.dispose()
    return conn_str


def _statements(source: SQLDataSource) -> list[str]:
    """Record every statement *source* sends to the database."""
    sent: list[str] = []

    @event.listens_for(source._get_engine(), "before_cursor_execute")
    def _record(conn, cursor, statement, parameters, context, executemany):
        sent.append(statement)

    return sent


def _rows(
    source: SQLDataSource, batch_size: int = 1000, limit: int | None = None
) -> list[dict]:
    batches = source.iter_batches(batch_size=batch_size, limit=limit)
    return [row for batch in batches for row in batch]


def test_a_limit_is_part_of_the_statement(connection_string: str) -> None:
    source = SQLDataSource(
        config=SQLConfig(
            connection_string=connection_string,
            query="SELECT * FROM events ORDER BY id",
        )
    )
    sent = _statements(source)

    rows = _rows(source, batch_size=10, limit=2)

    assert [row["id"] for row in rows] == [1, 2]
    assert len(sent) == 1
    assert "LIMIT" in sent[0].upper()


def test_no_limit_sends_the_query_unchanged(connection_string: str) -> None:
    query = "SELECT * FROM events ORDER BY id"
    source = SQLDataSource(
        config=SQLConfig(connection_string=connection_string, query=query)
    )
    sent = _statements(source)

    assert len(_rows(source, batch_size=2)) == 5
    assert sent == [query]


def test_a_limited_query_keeps_its_parameters(connection_string: str) -> None:
    source = SQLDataSource(
        config=SQLConfig(
            connection_string=connection_string,
            query="SELECT * FROM events WHERE kind = :kind ORDER BY id;",
            params={"kind": "a"},
        )
    )

    assert [row["id"] for row in _rows(source, limit=2)] == [1, 3]


def test_a_failing_query_raises(connection_string: str) -> None:
    source = SQLDataSource(
        config=SQLConfig(
            connection_string=connection_string,
            query="SELECT * FROM events WHERE base.\"kind\" = 'a'",
        )
    )

    with pytest.raises(SQLAlchemyError):
        _rows(source)
