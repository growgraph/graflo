"""File-based data source implementations.

This module provides data source implementations for file-based data sources,
including JSON, JSONL, and CSV/TSV files. It integrates with the existing
chunker logic for efficient batch processing.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Iterator

from graflo.architecture.onto import EncodingType
from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.util.chunker import ChunkerFactory, ChunkerType


@dataclasses.dataclass
class FileDataSource(AbstractDataSource):
    """Base class for file-based data sources.

    This class provides a common interface for file-based data sources,
    integrating with the existing chunker system for batch processing.

    Attributes:
        path: Path to the file
        file_type: Type of file (json, jsonl, table)
        encoding: File encoding (default: UTF_8)
    """

    path: Path | str
    file_type: str | None = None
    encoding: EncodingType = EncodingType.UTF_8

    def __post_init__(self):
        """Initialize the file data source."""
        self.source_type = DataSourceType.FILE
        if isinstance(self.path, str):
            self.path = Path(self.path)

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Iterate over file data in batches.

        Args:
            batch_size: Number of items per batch
            limit: Maximum number of items to retrieve

        Yields:
            list[dict]: Batches of documents as dictionaries
        """
        # Determine chunker type
        chunker_type = None
        if self.file_type:
            chunker_type = ChunkerType(self.file_type.lower())

        # Create chunker using factory
        chunker_kwargs = {
            "resource": self.path,
            "type": chunker_type,
            "batch_size": batch_size,
            "limit": limit,
            "encoding": self.encoding,
        }
        # Only add sep for table files
        if chunker_type == ChunkerType.TABLE and hasattr(self, "sep"):
            chunker_kwargs["sep"] = self.sep

        chunker = ChunkerFactory.create_chunker(**chunker_kwargs)

        # Yield batches
        for batch in chunker:
            yield batch


@dataclasses.dataclass
class JsonFileDataSource(FileDataSource):
    """Data source for JSON files.

    JSON files are expected to contain hierarchical data structures,
    similar to REST API responses. The chunker handles nested structures
    and converts them to dictionaries.

    Attributes:
        path: Path to the JSON file
        encoding: File encoding (default: UTF_8)
    """

    def __post_init__(self):
        """Initialize the JSON file data source."""
        super().__post_init__()
        self.file_type = ChunkerType.JSON.value


@dataclasses.dataclass
class JsonlFileDataSource(FileDataSource):
    """Data source for JSONL (JSON Lines) files.

    JSONL files contain one JSON object per line, making them suitable
    for streaming and batch processing.

    Attributes:
        path: Path to the JSONL file
        encoding: File encoding (default: UTF_8)
    """

    def __post_init__(self):
        """Initialize the JSONL file data source."""
        super().__post_init__()
        self.file_type = ChunkerType.JSONL.value


@dataclasses.dataclass
class TableFileDataSource(FileDataSource):
    """Data source for CSV/TSV files.

    Table files are converted to dictionaries with column headers as keys.
    Each row becomes a dictionary.

    Attributes:
        path: Path to the CSV/TSV file
        encoding: File encoding (default: UTF_8)
        sep: Field separator (default: ',')
    """

    sep: str = ","

    def __post_init__(self):
        """Initialize the table file data source."""
        super().__post_init__()
        self.file_type = ChunkerType.TABLE.value
