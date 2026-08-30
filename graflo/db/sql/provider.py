"""What a relational source must answer to be introspected.

Introspection needs seven facts about a schema. Everything downstream —
classifying a table as an entity or a relationship, recovering edge endpoints,
mapping columns onto typed fields — is derived from those and is the same for
every dialect. Naming them as a protocol is what lets PostgreSQL keep its
``pg_catalog`` fast path while any other engine arrives through SQLAlchemy
reflection, with one implementation of the logic between them.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SqlMetadataProvider(Protocol):
    """Schema metadata for one relational database.

    ``schema_name`` is the namespace to read: a PostgreSQL schema, a MySQL
    database, a BigQuery dataset. Implementations accept ``None`` and fall back
    to their own default.
    """

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        """Tables in *schema_name*, each as a dict with at least ``table_name``."""
        ...

    def get_table_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Columns of *table_name*.

        Each dict carries ``name`` and ``type``, and may carry ``description``,
        ``is_nullable``, ``column_default`` and ``ordinal_position``.
        """
        ...

    def get_primary_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        """Primary-key column names, in key order."""
        ...

    def get_unique_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        """Columns carrying a single-column uniqueness constraint."""
        ...

    def get_foreign_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Foreign keys, each with ``column`` and ``references_table``.

        ``references_column`` and ``constraint_name`` are optional. An engine
        that does not enforce foreign keys may still declare them (BigQuery), and
        one that enforces them may have none declared — in which case edge
        detection falls back to name-based inference.
        """
        ...

    def get_table_row_count_estimate(
        self, table_name: str, schema_name: str | None = None
    ) -> int | None:
        """Approximate row count, or ``None`` when the engine cannot cheaply say."""
        ...

    def get_table_sample_rows(
        self, table_name: str, schema_name: str | None = None, limit: int = 5
    ) -> list[dict[str, Any]]:
        """A few rows, for column sampling. Empty list when unavailable."""
        ...
