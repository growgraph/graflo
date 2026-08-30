"""A small 3NF SQLite database, built in-process.

SQLite needs no container and no credentials, which is the point: dialect
neutrality is worth little if it can only be demonstrated against the one
engine that already worked. These run in the default suite.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine

#: Two entity tables and a junction table between them — the shape schema
#: inference is meant to recognise.
DDL = """
CREATE TABLE author (
    id        INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL UNIQUE,
    hindex    INTEGER
);
CREATE TABLE field (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    level INTEGER
);
CREATE TABLE author_field (
    author_id INTEGER NOT NULL REFERENCES author(id),
    field_id  INTEGER NOT NULL REFERENCES field(id),
    PRIMARY KEY (author_id, field_id)
);
INSERT INTO author VALUES (1, 'Ada Lovelace', 9), (2, 'Grace Hopper', 12);
INSERT INTO field  VALUES (10, 'Computing', 1), (11, 'Mathematics', 1);
INSERT INTO author_field VALUES (1, 10), (2, 11);
"""


@pytest.fixture(scope="module")
def sqlite_engine(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Engine]:
    path: Path = tmp_path_factory.mktemp("sqlite_source") / "source.db"
    connection = sqlite3.connect(path)
    connection.executescript(DDL)
    connection.commit()
    connection.close()
    engine = create_engine(f"sqlite:///{path}")
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def sqlite_provider(sqlite_engine: Engine):
    from graflo.db.sql.alchemy import SqlAlchemyMetadataProvider

    return SqlAlchemyMetadataProvider(sqlite_engine)
