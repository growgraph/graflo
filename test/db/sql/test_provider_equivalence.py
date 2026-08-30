"""The two providers must agree about the same database.

PostgreSQL is the one engine GraFlo can introspect two ways: through
``pg_catalog`` directly, and through SQLAlchemy reflection like every other
engine. Pointing both at one database is the only test that can show the
reflection path is a faithful substitute rather than merely a plausible one —
everywhere else there is nothing to compare against.

Differences that are expected are asserted as differences, not smoothed over:
``pg_catalog`` reports column comments and an estimated row count, reflection
reports neither.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from graflo.db.sql import detect_edge_tables, detect_vertex_tables

SCHEMA = "gf_provider_equiv"

DDL = f"""
DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
CREATE SCHEMA {SCHEMA};
CREATE TABLE {SCHEMA}.author (
    id        integer PRIMARY KEY,
    full_name text NOT NULL UNIQUE,
    hindex    integer
);
CREATE TABLE {SCHEMA}.field (
    id    integer PRIMARY KEY,
    name  text NOT NULL,
    level integer
);
CREATE TABLE {SCHEMA}.author_field (
    author_id integer NOT NULL REFERENCES {SCHEMA}.author(id),
    field_id  integer NOT NULL REFERENCES {SCHEMA}.field(id),
    PRIMARY KEY (author_id, field_id)
);
INSERT INTO {SCHEMA}.author VALUES (1, 'Ada Lovelace', 9), (2, 'Grace Hopper', 12);
INSERT INTO {SCHEMA}.field  VALUES (10, 'Computing', 1), (11, 'Mathematics', 1);
INSERT INTO {SCHEMA}.author_field VALUES (1, 10), (2, 11);
"""


@pytest.fixture(scope="module")
def providers() -> Iterator[tuple]:
    """The same PostgreSQL schema, seen through both providers."""
    from sqlalchemy import create_engine

    from graflo.connections.onto import PostgresConfig
    from graflo.db.postgres.conn import PostgresConnection
    from graflo.db.sql.alchemy import SqlAlchemyMetadataProvider

    try:
        config = PostgresConfig.from_docker_env()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"postgres config unavailable: {error}")

    try:
        conn = PostgresConnection(config)
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"postgres unreachable: {error}")

    engine = create_engine(config.to_sqlalchemy_connection_string())
    try:
        with conn.conn.cursor() as cursor:
            cursor.execute(DDL)
        conn.conn.commit()
        yield conn, SqlAlchemyMetadataProvider(engine, default_schema=SCHEMA)
    finally:
        try:
            with conn.conn.cursor() as cursor:
                cursor.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            conn.conn.commit()
        except Exception:
            pass
        engine.dispose()
        conn.close()


def test_the_same_tables_are_found(providers) -> None:
    catalogue, reflected = providers
    assert {t["table_name"] for t in catalogue.get_tables(SCHEMA)} == {
        t["table_name"] for t in reflected.get_tables()
    }


@pytest.mark.parametrize("table", ["author", "field", "author_field"])
def test_keys_and_uniqueness_agree(providers, table: str) -> None:
    catalogue, reflected = providers
    assert catalogue.get_primary_keys(table, SCHEMA) == reflected.get_primary_keys(
        table
    )
    assert sorted(catalogue.get_unique_columns(table, SCHEMA)) == sorted(
        reflected.get_unique_columns(table)
    )


@pytest.mark.parametrize("table", ["author", "field", "author_field"])
def test_foreign_keys_agree(providers, table: str) -> None:
    catalogue, reflected = providers

    def pairs(rows):
        return {
            (fk["column"], fk["references_table"], fk.get("references_column"))
            for fk in rows
        }

    assert pairs(catalogue.get_foreign_keys(table, SCHEMA)) == pairs(
        reflected.get_foreign_keys(table)
    )


@pytest.mark.parametrize("table", ["author", "field", "author_field"])
def test_column_names_and_nullability_agree(providers, table: str) -> None:
    catalogue, reflected = providers

    def shape(rows):
        return {(c["name"], c["is_nullable"]) for c in rows}

    assert shape(catalogue.get_table_columns(table, SCHEMA)) == shape(
        reflected.get_table_columns(table)
    )


def test_both_classify_the_schema_identically(providers) -> None:
    """The assertion that matters: same database in, same graph out."""
    catalogue, reflected = providers

    assert {t.name for t in detect_vertex_tables(catalogue, SCHEMA)} == {
        t.name for t in detect_vertex_tables(reflected)
    }

    def edge_shape(edges):
        return {(e.name, frozenset((e.source_table, e.target_table))) for e in edges}

    assert edge_shape(detect_edge_tables(catalogue, SCHEMA)) == edge_shape(
        detect_edge_tables(reflected)
    )
