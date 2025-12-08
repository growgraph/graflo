"""Base classes for data source abstraction.

This module defines the abstract base class and types for all data sources.
Data sources handle data retrieval from various sources (files, APIs, databases)
and provide a unified interface for batch iteration.
"""

from __future__ import annotations

import abc
from typing import Iterator

from graflo.onto import BaseDataclass, BaseEnum


class DataSourceType(BaseEnum):
    """Types of data sources supported by the system.

    FILE: File-based data sources (JSON, JSONL, CSV/TSV)
    API: REST API data sources
    SQL: SQL database data sources
    IN_MEMORY: In-memory data sources (lists, DataFrames)
    """

    FILE = "file"
    API = "api"
    SQL = "sql"
    IN_MEMORY = "in_memory"


class AbstractDataSource(BaseDataclass, abc.ABC):
    """Abstract base class for all data sources.

    Data sources handle data retrieval from various sources and provide
    a unified interface for batch iteration. They are separate from Resources,
    which handle data transformation. Many DataSources can map to the same Resource.

    Attributes:
        source_type: Type of the data source
        resource_name: Name of the resource this data source maps to
            (set externally via DataSourceRegistry)
    """

    source_type: DataSourceType

    def __post_init__(self):
        """Initialize the data source after dataclass initialization."""
        self._resource_name: str | None = None

    @property
    def resource_name(self) -> str | None:
        """Get the resource name this data source maps to.

        Returns:
            Resource name or None if not set
        """
        return self._resource_name

    @resource_name.setter
    def resource_name(self, value: str | None):
        """Set the resource name this data source maps to.

        Args:
            value: Resource name to set
        """
        self._resource_name = value

    @abc.abstractmethod
    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Iterate over data in batches.

        This method yields batches of documents (dictionaries) from the data source.
        Each batch is a list of dictionaries representing the data items.

        Args:
            batch_size: Number of items per batch
            limit: Maximum number of items to retrieve (None for no limit)

        Yields:
            list[dict]: Batches of documents as dictionaries

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement iter_batches")

    def __iter__(self):
        """Make data source iterable, yielding individual items.

        Yields:
            dict: Individual documents
        """
        for batch in self.iter_batches(batch_size=1, limit=None):
            for item in batch:
                yield item
