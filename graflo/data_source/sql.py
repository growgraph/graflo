"""SQL database data source implementation.

This module provides a data source for SQL databases using SQLAlchemy-style
configuration. It supports parameterized queries and streams rows in bounded
memory using server-side cursor semantics where the driver supports them.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable, Iterator
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import Field, PrivateAttr

from graflo.architecture.base import ConfigBaseModel
from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.util.extras import lazy_module

if TYPE_CHECKING:
    import sqlalchemy as _sqlalchemy
    import sqlalchemy.engine as _sqlalchemy_engine
    import sqlalchemy.sql.expression as _sqlalchemy_sql_expression
else:
    _sqlalchemy = lazy_module("sqlalchemy", "sql")
    _sqlalchemy_engine = lazy_module("sqlalchemy.engine", "sql")
    _sqlalchemy_sql_expression = lazy_module("sqlalchemy.sql.expression", "sql")

logger = logging.getLogger(__name__)


class SQLConfig(ConfigBaseModel):
    """Configuration for SQL data source.

    Uses SQLAlchemy connection string format.

    Attributes:
        connection_string: SQLAlchemy connection string
            (e.g., 'postgresql://user:pass@localhost/dbname')
        query: SQL query string (supports parameterized queries)
        params: Query parameters as dictionary (for parameterized queries)
        pagination: Deprecated. Ignored; retained for config compatibility.
        page_size: Deprecated. Ignored; use ``iter_batches(batch_size=...)``.
    """

    connection_string: str
    query: str
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: bool | None = None
    page_size: int | None = None


def _check_distinct_columns(columns: Iterable[str]) -> None:
    """Raise when a result names two columns alike: its rows have no single reading."""
    repeated = sorted(name for name, n in Counter(columns).items() if n > 1)
    if repeated:
        raise ValueError(
            "the query returns more than one column named "
            + ", ".join(repr(name) for name in repeated)
            + "; give each a distinct name (in a view, with 'as')"
        )


class SQLDataSource(AbstractDataSource):
    """Data source for SQL databases.

    This class provides a data source for SQL databases using SQLAlchemy.
    Results are streamed with ``stream_results`` and ``fetchmany`` so large
    queries avoid OFFSET-based re-scans and bounded memory per chunk.
    Rows are returned as dictionaries with column names as keys.

    Attributes:
        config: SQL configuration
        engine: SQLAlchemy engine (created on first use)
    """

    config: SQLConfig
    source_type: DataSourceType = DataSourceType.SQL
    # ``Any``: pydantic evaluates this annotation, which must not import the driver.
    _engine: Any = PrivateAttr(default=None)

    def _get_engine(self) -> _sqlalchemy_engine.Engine:
        """Get or create SQLAlchemy engine.

        Returns:
            SQLAlchemy engine instance
        """
        if self._engine is None:
            url = _sqlalchemy_engine.make_url(self.config.connection_string)
            # SQLAlchemy 2.1 maps a bare ``postgresql://`` to psycopg (v3);
            # graflo ships psycopg2, so pin it unless a driver is named.
            if url.drivername == "postgresql":
                url = url.set(drivername="postgresql+psycopg2")
            self._engine = _sqlalchemy.create_engine(url)
        return self._engine

    @staticmethod
    def _row_to_json_dict(row: Any) -> dict[str, Any]:
        """Map one result row to a plain dict with JSON-friendly values."""
        row_dict: dict[str, Any] = dict(row._mapping)
        for key, value in row_dict.items():
            if isinstance(value, Decimal):
                row_dict[key] = float(value)
        return row_dict

    def _statement(self, limit: int | None) -> _sqlalchemy_sql_expression.Executable:
        """The configured query, bounded to *limit* rows when one is given.

        The bound is part of the statement, so the database plans for it and
        stops there. The query becomes a derived table and the row limit is
        compiled for the engine's dialect, which leaves the query text itself
        untouched. A derived table needs distinct column names on some engines,
        so a query that returns two columns of one name cannot be limited there.
        """
        if limit is None:
            return _sqlalchemy.text(self.config.query)
        inner = _sqlalchemy.text(self.config.query.strip().rstrip(";").rstrip())
        source = inner.columns().subquery("graflo_source")
        return (
            _sqlalchemy.select(_sqlalchemy.literal_column("*"))
            .select_from(source)
            .limit(limit)
        )

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Iterate over SQL query results in batches.

        Executes the configured query once per call and reads via
        ``fetchmany`` on a streaming result. With ``limit``, the statement
        itself is bounded to that many rows (see :meth:`_statement`).

        Args:
            batch_size: Target size of each yielded batch of row dicts
                (last batch may be smaller).
            limit: Maximum total rows to read, or ``None`` for full result.

        Yields:
            list[dict]: Batches of rows as dictionaries

        Raises:
            sqlalchemy.exc.SQLAlchemyError: When the query fails. A source that
                cannot be read must not look like an empty one.
            ValueError: The result names two columns alike.
        """
        effective_batch = max(1, batch_size)
        engine = self._get_engine()
        total_items = 0

        try:
            with engine.connect() as conn:
                stream = conn.execution_options(stream_results=True)
                result = stream.execute(self._statement(limit), self.config.params)
                try:
                    _check_distinct_columns(result.keys())
                    while True:
                        if limit is not None and total_items >= limit:
                            break

                        remaining = None if limit is None else limit - total_items
                        fetch_n = (
                            effective_batch
                            if remaining is None
                            else min(effective_batch, remaining)
                        )

                        rows = result.fetchmany(fetch_n)
                        if not rows:
                            break

                        batch: list[dict] = []
                        for row in rows:
                            batch.append(self._row_to_json_dict(row))
                            total_items += 1
                            if limit is not None and total_items >= limit:
                                break

                        if batch:
                            yield batch

                        if limit is not None and total_items >= limit:
                            break
                finally:
                    result.close()

        except Exception as e:
            logger.error("SQL query execution failed: %s", e)
            raise
