"""SQL database data source implementation.

This module provides a data source for SQL databases using SQLAlchemy-style
configuration. It supports parameterized queries and pagination.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SQLConfig(BaseDataclass):
    """Configuration for SQL data source.

    Uses SQLAlchemy connection string format.

    Attributes:
        connection_string: SQLAlchemy connection string
            (e.g., 'postgresql://user:pass@localhost/dbname')
        query: SQL query string (supports parameterized queries)
        params: Query parameters as dictionary (for parameterized queries)
        pagination: Whether to use pagination (default: True)
        page_size: Number of rows per page (default: 1000)
    """

    connection_string: str
    query: str
    params: dict[str, Any] = dataclasses.field(default_factory=dict)
    pagination: bool = True
    page_size: int = 1000


@dataclasses.dataclass
class SQLDataSource(AbstractDataSource):
    """Data source for SQL databases.

    This class provides a data source for SQL databases using SQLAlchemy.
    It supports parameterized queries and pagination. Returns rows as
    dictionaries with column names as keys.

    Attributes:
        config: SQL configuration
        engine: SQLAlchemy engine (created on first use)
    """

    config: SQLConfig
    engine: Engine | None = dataclasses.field(default=None, init=False)

    def __post_init__(self):
        """Initialize the SQL data source."""
        super().__post_init__()
        self.source_type = DataSourceType.SQL

    def _get_engine(self) -> Engine:
        """Get or create SQLAlchemy engine.

        Returns:
            SQLAlchemy engine instance
        """
        if self.engine is None:
            self.engine = create_engine(self.config.connection_string)
        return self.engine

    def _add_pagination(self, query: str, offset: int, limit: int) -> str:
        """Add pagination to SQL query.

        Args:
            query: Original SQL query
            offset: Offset value
            limit: Limit value

        Returns:
            Query with pagination added
        """
        # Check if query already has LIMIT/OFFSET
        query_upper = query.upper().strip()
        if "LIMIT" in query_upper or "OFFSET" in query_upper:
            # Query already has pagination, return as-is
            return query

        # Add pagination based on database type
        # For most SQL databases, use LIMIT/OFFSET
        # For SQL Server, use TOP and OFFSET/FETCH
        connection_string_lower = self.config.connection_string.lower()

        if "sqlserver" in connection_string_lower or "mssql" in connection_string_lower:
            # SQL Server syntax
            return f"{query} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        elif "oracle" in connection_string_lower:
            # Oracle syntax (using ROWNUM or FETCH)
            return f"{query} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        else:
            # Standard SQL (PostgreSQL, MySQL, SQLite, etc.)
            return f"{query} LIMIT {limit} OFFSET {offset}"

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Iterate over SQL query results in batches.

        Args:
            batch_size: Number of items per batch
            limit: Maximum number of items to retrieve

        Yields:
            list[dict]: Batches of rows as dictionaries
        """
        engine = self._get_engine()
        total_items = 0
        offset = 0

        # Use configured page size or batch size, whichever is smaller
        page_size = min(self.config.page_size, batch_size)

        while True:
            # Build query
            if self.config.pagination:
                query_str = self._add_pagination(
                    self.config.query, offset=offset, limit=page_size
                )
            else:
                query_str = self.config.query

            # Execute query
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(query_str), self.config.params)
                    rows = result.fetchall()

                    # Convert rows to dictionaries
                    batch = []
                    from decimal import Decimal

                    for row in rows:
                        if limit and total_items >= limit:
                            break

                        # Convert row to dictionary
                        row_dict = dict(row._mapping)
                        # Convert Decimal to float for JSON compatibility
                        for key, value in row_dict.items():
                            if isinstance(value, Decimal):
                                row_dict[key] = float(value)
                        batch.append(row_dict)
                        total_items += 1

                        # Yield when batch is full
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []

                    # Yield remaining items
                    if batch:
                        yield batch

                    # Check if we should continue
                    if limit and total_items >= limit:
                        break

                    # Check if there are more rows
                    if len(rows) < page_size:
                        # No more rows
                        break

                    # Update offset for next iteration
                    if self.config.pagination:
                        offset += page_size
                    else:
                        # No pagination, single query
                        break

            except Exception as e:
                logger.error(f"SQL query execution failed: {e}")
                break
