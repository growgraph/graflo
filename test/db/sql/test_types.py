"""One type table, many dialects.

Every engine spells the same handful of concepts differently. These pin the
spellings that matter, and — more importantly — that adding a dialect never
changes how an existing name resolves.
"""

from __future__ import annotations

import pytest

from graflo.db.postgres.types import PostgresTypeMapper
from graflo.db.sql.types import SqlTypeMapper


@pytest.mark.parametrize(
    ("sql_type", "expected"),
    [
        # ANSI / PostgreSQL
        ("integer", "INT"),
        ("bigint", "INT"),
        ("double precision", "FLOAT"),
        ("numeric", "FLOAT"),
        ("character varying", "STRING"),
        ("text", "STRING"),
        ("boolean", "BOOL"),
        ("timestamp with time zone", "DATETIME"),
        ("jsonb", "STRING"),
        # SQLite reflects uppercase
        ("INTEGER", "INT"),
        ("TEXT", "STRING"),
        ("VARCHAR(64)", "STRING"),
        # BigQuery / Snowflake
        ("INT64", "INT"),
        ("FLOAT64", "FLOAT"),
        ("BIGNUMERIC", "FLOAT"),
        ("BOOL", "BOOL"),
        ("BYTES", "STRING"),
        ("GEOGRAPHY", "STRING"),
        # MySQL / SQL Server
        ("tinyint", "INT"),
        ("nvarchar", "STRING"),
        ("longtext", "STRING"),
        ("datetime2", "DATETIME"),
    ],
)
def test_type_names_map_across_dialects(sql_type: str, expected: str) -> None:
    assert SqlTypeMapper.map_type(sql_type) == expected


@pytest.mark.parametrize(
    ("sql_type", "expected"),
    [("integer[]", ("LIST", "INT")), ("_text", ("LIST", "STRING"))],
)
def test_arrays_carry_their_item_type(sql_type: str, expected) -> None:
    assert SqlTypeMapper.map_field(sql_type) == expected


def test_unknown_types_fall_back_rather_than_raise() -> None:
    """Inference drafts a schema for a modeller; one exotic column must not stop it."""
    assert SqlTypeMapper.map_type("some_extension_type") == "STRING"


def test_postgres_mapper_still_resolves_its_own_spellings() -> None:
    """The PostgreSQL path must be unchanged by the table having grown."""
    for name in ("int4", "int8", "float8", "varchar", "timestamptz", "bytea", "uuid"):
        assert PostgresTypeMapper.map_type(name) == SqlTypeMapper.map_type(name)
    assert PostgresTypeMapper.map_type("int4") == "INT"
    assert PostgresTypeMapper.map_type("timestamptz") == "DATETIME"
