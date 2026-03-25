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
    >>> from graflo import GraphEngine, IngestionParams
    >>> engine = GraphEngine()
    >>> manifest = engine.infer_manifest(postgres_config)
    >>> engine.define_and_ingest(manifest, target_db_config)

For targeted imports (smaller dependency graph), see ``docs/importing.md`` in the package repo.
"""

# --- Core orchestration ---------------------------------------------------
from .hq import (
    CastBatchResult,
    Caster,
    GraphEngine,
    IngestionParams,
    RowCastFailure,
    RowErrorBudgetExceeded,
)

# --- Architecture ----------------------------------------------------------
from .architecture import (
    Bindings,
    FileConnector,
    GraphMetadata,
    GraphManifest,
    JoinClause,
    DatabaseProfile,
    Edge,
    EdgeConfig,
    FieldType,
    CoreSchema,
    GraphModel,
    Index,
    IngestionModel,
    ResourceConnector,
    ResourceType,
    Resource,
    SparqlConnector,
    Schema,
    TableConnector,
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

__all__ = [
    # Orchestration
    "GraphEngine",
    "Caster",
    "CastBatchResult",
    "IngestionParams",
    "RowCastFailure",
    "RowErrorBudgetExceeded",
    # Architecture
    "Schema",
    "GraphMetadata",
    "CoreSchema",
    "GraphModel",
    "DatabaseProfile",
    "IngestionModel",
    "GraphManifest",
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
    "FileConnector",
    "Bindings",
    "JoinClause",
    "ResourceConnector",
    "ResourceType",
    "SparqlConnector",
    "TableConnector",
]
