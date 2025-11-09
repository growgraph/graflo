"""graflo: A flexible graph database abstraction layer.

graflo provides a unified interface for working with different graph databases
(ArangoDB, Neo4j) through a common API. It handles graph operations, data
transformations, and query generation while abstracting away database-specific
details.

Key Features:
    - Database-agnostic graph operations
    - Flexible schema management
    - Query generation and execution
    - Data transformation utilities
    - Filter expression system

Example:
    >>> from graflo.backend.manager import ConnectionManager
    >>> with ConnectionManager(config) as conn:
    ...     conn.init_db(schema, clean_start=True)
    ...     conn.upsert_docs_batch(docs, "users")
"""

from .architecture import Index, Schema
from .caster import Caster
from .data_source import (
    APIConfig,
    APIDataSource,
    AbstractDataSource,
    DataSourceFactory,
    DataSourceRegistry,
    DataSourceType,
    FileDataSource,
    JsonFileDataSource,
    JsonlFileDataSource,
    PaginationConfig,
    SQLConfig,
    SQLDataSource,
    TableFileDataSource,
)
from .backend import ConnectionManager, ConnectionType
from .filter.onto import ComparisonOperator, LogicalOperator
from .onto import AggregationType
from .util.onto import Patterns

__all__ = [
    "AbstractDataSource",
    "APIConfig",
    "APIDataSource",
    "AggregationType",
    "ComparisonOperator",
    "ConnectionManager",
    "ConnectionType",
    "Caster",
    "DataSourceFactory",
    "DataSourceRegistry",
    "DataSourceType",
    "FileDataSource",
    "Index",
    "JsonFileDataSource",
    "JsonlFileDataSource",
    "LogicalOperator",
    "PaginationConfig",
    "Patterns",
    "Schema",
    "SQLConfig",
    "SQLDataSource",
    "TableFileDataSource",
]
