"""graflo: A flexible graph database abstraction layer.

graflo provides a unified interface for working with different graph databases
(ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph) through a common API.
It handles graph operations, data transformations, and query generation while
abstracting away database-specific details.

Key Features:
    - Database-agnostic graph operations
    - Flexible schema management with typed fields
    - Automatic schema inference from PostgreSQL databases
    - Query generation and execution
    - Data transformation utilities
    - Filter expression system

Example:
    >>> from graflo import GraphEngine, Schema, IngestionParams
    >>> engine = GraphEngine()
    >>> schema = engine.infer_schema(postgres_config)
    >>> engine.define_and_ingest(schema, target_db_config)
"""

# --- Core orchestration ---------------------------------------------------
from .hq import Caster, GraphEngine, IngestionParams

# --- Architecture ----------------------------------------------------------
from .architecture import (
    Edge,
    EdgeConfig,
    FieldType,
    Index,
    Resource,
    Schema,
    Vertex,
    VertexConfig,
)

# --- Data sources ----------------------------------------------------------
from .data_source import (
    APIConfig,
    APIDataSource,
    AbstractDataSource,
    DataSourceFactory,
    DataSourceRegistry,
    DataSourceType,
    FileDataSource,
    InMemoryDataSource,
    JsonFileDataSource,
    JsonlFileDataSource,
    PaginationConfig,
    SQLConfig,
    SQLDataSource,
    TableFileDataSource,
)

# --- Database --------------------------------------------------------------
from .db import ConnectionManager, ConnectionType

# --- Filters ---------------------------------------------------------------
from .filter import ComparisonOperator, FilterExpression, LogicalOperator

# --- Enums & utilities -----------------------------------------------------
from .onto import AggregationType, DBType
from .util.onto import FilePattern, Patterns, ResourcePattern, TablePattern

__all__ = [
    # Orchestration
    "GraphEngine",
    "Caster",
    "IngestionParams",
    # Architecture
    "Schema",
    "Resource",
    "Vertex",
    "VertexConfig",
    "Edge",
    "EdgeConfig",
    "FieldType",
    "Index",
    # Data sources
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
    # Database
    "ConnectionManager",
    "ConnectionType",
    # Filters
    "ComparisonOperator",
    "FilterExpression",
    "LogicalOperator",
    # Enums & utilities
    "AggregationType",
    "DBType",
    "FilePattern",
    "Patterns",
    "ResourcePattern",
    "TablePattern",
]
