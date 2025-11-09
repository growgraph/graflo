"""In-memory data source implementations.

This module provides data source implementations for in-memory data structures,
including lists of dictionaries, lists of lists, and Pandas DataFrames.
"""

from __future__ import annotations

import dataclasses
from typing import Iterator

import pandas as pd

from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.util.chunker import ChunkerFactory


@dataclasses.dataclass
class InMemoryDataSource(AbstractDataSource):
    """Data source for in-memory data structures.

    This class provides a data source for Python objects that are already
    in memory, including lists of dictionaries, lists of lists, and Pandas DataFrames.

    Attributes:
        data: Data to process (list[dict], list[list], or pd.DataFrame)
        columns: Optional column names for list[list] data
    """

    data: list[dict] | list[list] | pd.DataFrame
    columns: list[str] | None = None

    def __post_init__(self):
        """Initialize the in-memory data source."""
        self.source_type = DataSourceType.IN_MEMORY

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Iterate over in-memory data in batches.

        Args:
            batch_size: Number of items per batch
            limit: Maximum number of items to retrieve

        Yields:
            list[dict]: Batches of documents as dictionaries
        """
        # Normalize data: convert list[list] to list[dict] if needed
        data = self.data
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
            # list[list] - convert to list[dict] using columns
            if self.columns is None:
                raise ValueError(
                    "columns parameter is required when data is list[list]"
                )
            data = [{k: v for k, v in zip(self.columns, item)} for item in data]

        # Create chunker using factory (only pass columns if it's a DataFrame)
        chunker_kwargs = {
            "resource": data,
            "batch_size": batch_size,
            "limit": limit,
        }
        # Note: columns is not passed to chunker - we handle list[list] conversion above
        # DataFrame chunker doesn't need columns either

        chunker = ChunkerFactory.create_chunker(**chunker_kwargs)

        # Yield batches
        for batch in chunker:
            yield batch
