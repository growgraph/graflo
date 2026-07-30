"""``PostgresConfig.from_dsn`` — DSN parsing into connectable config.

``uri`` alone derives only host and port; ``username`` / ``password`` /
``database`` are independent fields, so a config built from a bare URI cannot
produce a connection string. These tests pin the parsing that closes that gap.
"""

import pytest

from graflo.connections.onto import PostgresConfig


def test_from_dsn_populates_credentials_and_database():
    config = PostgresConfig.from_dsn("postgresql://alice:secret@db.example:5433/shop")
    assert config.hostname == "db.example"
    assert str(config.port) == "5433"
    assert config.username == "alice"
    assert config.password == "secret"
    assert config.database == "shop"


def test_from_dsn_yields_usable_sqlalchemy_connection_string():
    """The regression this guards: a bare uri= config raised here."""
    config = PostgresConfig.from_dsn("postgresql://alice:secret@db.example:5433/shop")
    assert (
        config.to_sqlalchemy_connection_string()
        == "postgresql://alice:secret@db.example:5433/shop"
    )


def test_from_dsn_percent_decodes_credentials():
    config = PostgresConfig.from_dsn("postgresql://user%40corp:p%40ss@h/db")
    assert config.username == "user@corp"
    assert config.password == "p@ss"


def test_from_dsn_applies_default_port():
    config = PostgresConfig.from_dsn("postgresql://user@localhost/db")
    assert str(config.port) == "5432"


@pytest.mark.parametrize(
    "dsn,expected",
    [
        ("postgresql://u@h/db?schema=sales", "sales"),
        ("postgresql://u@h/db?options=-csearch_path=sales", "sales"),
        ("postgresql://u@h/db?options=-c%20search_path=sales", "sales"),
        ("postgresql://u@h/db", None),
    ],
)
def test_from_dsn_reads_schema_from_query_or_libpq_options(dsn, expected):
    assert PostgresConfig.from_dsn(dsn).schema_name == expected


def test_from_dsn_overrides_win_over_parsed_values():
    config = PostgresConfig.from_dsn(
        "postgresql://u@h/db", database="other", schema_name="analytics"
    )
    assert config.database == "other"
    assert config.schema_name == "analytics"


def test_from_dsn_rejects_dsn_without_host():
    with pytest.raises(ValueError, match="no host"):
        PostgresConfig.from_dsn("not-a-dsn")


def test_from_dsn_tolerates_missing_database():
    """A DSN may legitimately omit the database; the error should surface later,
    at connection-string construction, not at parse time."""
    config = PostgresConfig.from_dsn("postgresql://u@h")
    assert config.database is None
    with pytest.raises(ValueError, match="database name is required"):
        config.to_sqlalchemy_connection_string()
