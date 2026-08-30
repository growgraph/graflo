"""A :class:`SqlMetadataProvider` backed by SQLAlchemy reflection.

PostgreSQL reads its own catalogue directly, which is faster and exposes column
comments. Every other engine arrives here instead: SQLAlchemy's ``Inspector``
answers the same seven questions against SQLite, MySQL, DuckDB, Snowflake,
BigQuery or anything else with a dialect installed, so schema inference is a
property of the engine's reflection support rather than of GraFlo.

What varies between engines is not the questions but how much of the answer is
there. A warehouse typically declares no foreign keys — BigQuery's are
unenforced and often absent entirely — so edge detection falls back to the
name-based inference in :mod:`graflo.db.sql.inference_utils`. That is a real
degradation on denormalised schemas, not a bug: a star schema is not 3NF, and
nothing in the catalogue says which columns were meant to be joins.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import Engine, inspect, text
from sqlalchemy.engine import Inspector

from graflo.db.sql.provider import SqlMetadataProvider

logger = logging.getLogger(__name__)


class SqlAlchemyMetadataProvider(SqlMetadataProvider):
    """Reflect schema metadata from any engine SQLAlchemy has a dialect for.

    Args:
        engine: A live SQLAlchemy ``Engine``.
        default_schema: Namespace used when a call passes ``schema_name=None``.
            ``None`` means the engine's own default, which is what SQLite and
            most single-namespace engines want.
    """

    def __init__(self, engine: Engine, *, default_schema: str | None = None):
        self.engine = engine
        self.default_schema = default_schema
        self._inspector: Inspector | None = None

    @property
    def inspector(self) -> Inspector:
        if self._inspector is None:
            self._inspector = inspect(self.engine)
        return self._inspector

    def _schema(self, schema_name: str | None) -> str | None:
        return schema_name if schema_name is not None else self.default_schema

    def _reflect(
        self, method: str, table_name: str, schema: str | None, *, default: Any
    ) -> Any:
        """Call an optional ``Inspector`` method, degrading when unsupported.

        Reflection is not uniform across dialects. Constraint reflection in
        particular is absent on several warehouse dialects — BigQuery does not
        enforce keys, so its dialect may not implement the call at all — and
        SQLAlchemy signals that by raising ``NotImplementedError``. Treating it
        as fatal would make an entire class of source un-introspectable over a
        question whose honest answer is simply "none declared".

        The consequence is real and worth stating: with no foreign keys, edge
        detection falls back to name-based inference, which is weaker.
        """
        try:
            return getattr(self.inspector, method)(table_name, schema=schema)
        except NotImplementedError:
            logger.debug(
                "Dialect %s does not implement %s; treating as empty.",
                self.engine.dialect.name,
                method,
            )
            return default
        except Exception as error:
            logger.warning(
                "Reflection call %s failed for '%s': %s. Continuing without it, "
                "which may weaken edge detection.",
                method,
                table_name,
                error,
            )
            return default

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        schema = self._schema(schema_name)
        return [
            {"table_name": name, "table_schema": schema}
            for name in self.inspector.get_table_names(schema=schema)
        ]

    def get_table_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        schema = self._schema(schema_name)
        columns = self.inspector.get_columns(table_name, schema=schema)
        out: list[dict[str, Any]] = []
        for position, col in enumerate(columns, start=1):
            out.append(
                {
                    "name": col["name"],
                    # str() of the reflected type, so the type mapper sees the
                    # dialect's own spelling ("VARCHAR(64)", "INT64", …) exactly
                    # as the pg_catalog path would report it.
                    "type": str(col.get("type", "")),
                    "description": col.get("comment") or "",
                    "is_nullable": "YES" if col.get("nullable", True) else "NO",
                    "column_default": col.get("default"),
                    "ordinal_position": position,
                }
            )
        return out

    def get_primary_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        schema = self._schema(schema_name)
        constraint = self.inspector.get_pk_constraint(table_name, schema=schema)
        return list(constraint.get("constrained_columns") or [])

    def get_unique_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        """Single-column uniqueness only, matching the PostgreSQL path.

        A multi-column unique constraint says nothing about any one of its
        columns being unique, so folding it in here would mark columns unique
        that are not.
        """
        schema = self._schema(schema_name)
        constraints = self._reflect(
            "get_unique_constraints", table_name, schema, default=[]
        )
        unique: list[str] = []
        for constraint in constraints:
            columns = constraint.get("column_names") or []
            if len(columns) == 1:
                unique.append(columns[0])
        return unique

    def get_foreign_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        schema = self._schema(schema_name)
        out: list[dict[str, Any]] = []
        for fk in self._reflect("get_foreign_keys", table_name, schema, default=[]):
            constrained = fk.get("constrained_columns") or []
            referred = fk.get("referred_columns") or []
            referred_table = fk.get("referred_table")
            if not constrained or not referred_table:
                continue
            # One entry per constrained column, matching the pg_catalog path,
            # which reports a composite key as several single-column rows.
            for position, column in enumerate(constrained):
                out.append(
                    {
                        "column": column,
                        "references_table": referred_table,
                        "references_column": (
                            referred[position] if position < len(referred) else None
                        ),
                        "constraint_name": fk.get("name"),
                    }
                )
        return out

    def get_table_row_count_estimate(
        self, table_name: str, schema_name: str | None = None
    ) -> int | None:
        """Exact ``COUNT(*)``; SQLAlchemy exposes no portable estimate.

        PostgreSQL answers this from ``pg_class.reltuples`` without scanning.
        There is no dialect-neutral equivalent, and the count is only used to
        rank tables, so a failure returns ``None`` rather than aborting
        introspection over a table that is large or unreadable.
        """
        qualified = self._qualified(table_name, schema_name)
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {qualified}"))
                return int(result.scalar() or 0)
        except Exception as error:
            logger.debug("Row count unavailable for '%s': %s", table_name, error)
            return None

    def _qualified(self, table_name: str, schema_name: str | None) -> str:
        """``schema.table`` when the engine has a namespace, else ``table``."""
        schema = self._schema(schema_name)
        return f"{schema}.{table_name}" if schema else table_name

    def get_table_sample_rows(
        self, table_name: str, schema_name: str | None = None, limit: int = 5
    ) -> list[dict[str, Any]]:
        qualified = self._qualified(table_name, schema_name)
        try:
            with self.engine.connect() as connection:
                rows = connection.execute(
                    text(f"SELECT * FROM {qualified} LIMIT {int(limit)}")
                )
                return [dict(row._mapping) for row in rows]
        except Exception as error:
            logger.debug("Sample rows unavailable for '%s': %s", table_name, error)
            return []
