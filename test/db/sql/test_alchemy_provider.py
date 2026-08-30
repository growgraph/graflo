"""The reflection provider answers the seven questions introspection asks.

Asserted against SQLite rather than PostgreSQL on purpose: PostgreSQL has its
own catalogue path, so proving the protocol only there would prove nothing
about the reflection one.
"""

from __future__ import annotations

from graflo.db.sql.provider import SqlMetadataProvider


def test_provider_satisfies_the_protocol(sqlite_provider) -> None:
    assert isinstance(sqlite_provider, SqlMetadataProvider)


def test_tables_are_listed(sqlite_provider) -> None:
    names = {t["table_name"] for t in sqlite_provider.get_tables()}
    assert names == {"author", "field", "author_field"}


def test_columns_carry_name_and_type(sqlite_provider) -> None:
    columns = {c["name"]: c for c in sqlite_provider.get_table_columns("author")}
    assert set(columns) == {"id", "full_name", "hindex"}
    assert columns["full_name"]["type"] == "TEXT"
    assert columns["full_name"]["is_nullable"] == "NO"
    assert columns["hindex"]["is_nullable"] == "YES"
    assert columns["id"]["ordinal_position"] == 1


def test_primary_keys_are_reported_in_key_order(sqlite_provider) -> None:
    assert sqlite_provider.get_primary_keys("author") == ["id"]
    assert sqlite_provider.get_primary_keys("author_field") == [
        "author_id",
        "field_id",
    ]


def test_single_column_unique_constraints_only(sqlite_provider) -> None:
    """A composite unique constraint says nothing about its columns alone."""
    assert sqlite_provider.get_unique_columns("author") == ["full_name"]
    assert sqlite_provider.get_unique_columns("field") == []


def test_foreign_keys_are_one_row_per_column(sqlite_provider) -> None:
    fks = sqlite_provider.get_foreign_keys("author_field")
    assert {(fk["column"], fk["references_table"]) for fk in fks} == {
        ("author_id", "author"),
        ("field_id", "field"),
    }
    assert all(fk["references_column"] == "id" for fk in fks)


def test_row_counts_and_samples(sqlite_provider) -> None:
    assert sqlite_provider.get_table_row_count_estimate("author") == 2
    rows = sqlite_provider.get_table_sample_rows("author", limit=5)
    assert {row["full_name"] for row in rows} == {"Ada Lovelace", "Grace Hopper"}


def test_unknown_table_degrades_rather_than_raises(sqlite_provider) -> None:
    """Sampling is best-effort; introspection must not die on one bad table."""
    assert sqlite_provider.get_table_sample_rows("does_not_exist") == []
    assert sqlite_provider.get_table_row_count_estimate("does_not_exist") is None


class _PartialInspector:
    """An inspector for a dialect that reflects tables but not constraints.

    Stands in for the warehouse case: BigQuery does not enforce keys, so its
    dialect may not implement constraint reflection at all.
    """

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_unique_constraints(self, table_name, schema=None):
        raise NotImplementedError("dialect does not reflect unique constraints")

    def get_foreign_keys(self, table_name, schema=None):
        raise NotImplementedError("dialect does not reflect foreign keys")


def test_absent_constraint_reflection_is_not_fatal(sqlite_engine) -> None:
    """ "None declared" is the honest answer; raising would lose the whole source."""
    from typing import cast

    from sqlalchemy.engine import Inspector

    from graflo.db.sql import detect_vertex_tables
    from graflo.db.sql.alchemy import SqlAlchemyMetadataProvider

    provider = SqlAlchemyMetadataProvider(sqlite_engine)
    provider._inspector = cast(Inspector, _PartialInspector(provider.inspector))

    assert provider.get_unique_columns("author") == []
    assert provider.get_foreign_keys("author_field") == []

    # Tables and keys still reflect, so entities are still recovered.
    assert {t.name for t in detect_vertex_tables(provider)} == {"author", "field"}
