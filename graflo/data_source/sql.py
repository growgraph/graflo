"""SQL database data source implementation.

This module provides a data source for SQL databases using SQLAlchemy-style
configuration. It supports parameterized queries and streams rows in bounded
memory using server-side cursor semantics where the driver supports them.
"""

import logging
from decimal import Decimal
from typing import Any, Iterator

from pydantic import Field, PrivateAttr
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from graflo.architecture.base import ConfigBaseModel
from graflo.data_source.base import AbstractDataSource, DataSourceType

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
    _engine: Engine | None = PrivateAttr(default=None)

    def _get_engine(self) -> Engine:
        """Get or create SQLAlchemy engine.

        Returns:
            SQLAlchemy engine instance
        """
        if self._engine is None:
            self._engine = create_engine(self.config.connection_string)
        return self._engine

    @staticmethod
    def _row_to_json_dict(row: Any) -> dict[str, Any]:
        """Map one result row to a plain dict with JSON-friendly values."""
        row_dict: dict[str, Any] = dict(row._mapping)
        for key, value in row_dict.items():
            if isinstance(value, Decimal):
                row_dict[key] = float(value)
        return row_dict

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Iterate over SQL query results in batches.

        Executes the configured query once per call and reads via
        ``fetchmany`` on a streaming result. Optional ``limit`` stops after
        that many rows without adding LIMIT/OFFSET to the SQL text.

        Args:
            batch_size: Target size of each yielded batch of row dicts
                (last batch may be smaller).
            limit: Maximum total rows to read, or ``None`` for full result.

        Yields:
            list[dict]: Batches of rows as dictionaries
        """
        effective_batch = max(1, batch_size)
        engine = self._get_engine()
        total_items = 0

        try:
            with engine.connect() as conn:
                stream = conn.execution_options(stream_results=True)
                result = stream.execute(text(self.config.query), self.config.params)
                try:
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
