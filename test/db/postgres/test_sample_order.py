"""Sampling returns the same rows for the same data, whatever the storage order.

PostgreSQL returns an unordered ``SELECT`` in storage order, and an ``UPDATE``
moves a row to the end of the heap. A sample that feeds an inference prompt
must not change when nothing but storage order did.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def tables(postgres_conn):
    statements = [
        "DROP TABLE IF EXISTS gf_sample_keyed, gf_sample_unkeyed",
        "CREATE TABLE gf_sample_keyed (id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE TABLE gf_sample_unkeyed (v TEXT, n INTEGER)",
        "INSERT INTO gf_sample_keyed VALUES (3, 'c'), (1, 'a'), (2, 'b')",
        "INSERT INTO gf_sample_unkeyed VALUES ('c', 3), ('a', 1), ('b', 2)",
    ]
    try:
        with postgres_conn.conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        postgres_conn.conn.commit()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"postgres unavailable: {error}")
    yield postgres_conn
    with postgres_conn.conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS gf_sample_keyed, gf_sample_unkeyed")
    postgres_conn.conn.commit()


def _touch(conn, table: str) -> None:
    """Rewrite one row in place, which moves it in storage order."""
    with conn.conn.cursor() as cursor:
        cursor.execute(f"UPDATE {table} SET v = v WHERE v = 'a'")
    conn.conn.commit()


def test_a_keyed_table_samples_in_key_order(tables) -> None:
    before = tables.get_table_sample_rows("gf_sample_keyed", limit=2)
    _touch(tables, "gf_sample_keyed")
    after = tables.get_table_sample_rows("gf_sample_keyed", limit=2)
    assert [row["id"] for row in before] == [1, 2]
    assert after == before


def test_an_unkeyed_table_samples_the_same_rows_after_an_update(tables) -> None:
    before = tables.get_table_sample_rows("gf_sample_unkeyed", limit=2)
    _touch(tables, "gf_sample_unkeyed")
    after = tables.get_table_sample_rows("gf_sample_unkeyed", limit=2)
    assert after == before
    assert len(before) == 2
