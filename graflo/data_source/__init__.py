"""Data source abstraction layer for graph database ingestion.

This package provides a unified interface for different data source types,
separating "where data comes from" (DataSource) from "how it's transformed" (Resource).

Key Components:
    - AbstractDataSource: Base class for all data sources
    - FileDataSource: File-based data sources (JSON, JSONL, CSV/TSV)
    - APIDataSource: REST API data source
    - SQLDataSource: SQL database data source
    - DataSourceRegistry: Maps DataSources to Resource names

Example:
    >>> from graflo.data_source import FileDataSource, DataSourceRegistry
    >>> source = FileDataSource(path="data.json", file_type="json")
    >>> registry = DataSourceRegistry()
    >>> registry.register(source, resource_name="users")
"""

from .api import APIConfig, APIDataSource, PaginationConfig
from .base import AbstractDataSource, DataSourceType
from .factory import DataSourceFactory
from .file import (
    FileDataSource,
    JsonFileDataSource,
    JsonlFileDataSource,
    TableFileDataSource,
)
from .memory import InMemoryDataSource
from .registry import DataSourceRegistry
from .sql import SQLConfig, SQLDataSource

__all__ = [
    "AbstractDataSource",
    "APIConfig",
    "APIDataSource",
    "DataSourceFactory",
    "DataSourceRegistry",
    "DataSourceType",
    "FileDataSource",
    "InMemoryDataSource",
    "JsonFileDataSource",
    "JsonlFileDataSource",
    "PaginationConfig",
    "SQLConfig",
    "SQLDataSource",
    "TableFileDataSource",
]
