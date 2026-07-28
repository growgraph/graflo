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

from __future__ import annotations

# Orchestration (graflo.hq) is loaded lazily via __getattr__ so importing
# ``graflo.architecture.*`` does not eagerly pull GraphEngine, Sanitizer, etc.

_HQ_EXPORTS = frozenset(
    {
        "CastBatchResult",
        "Caster",
        "GraphEngine",
        "IngestionParams",
        "DocCastFailure",
        "DocErrorBudgetExceeded",
    }
)

# ``graflo.db`` pulls ``graflo.hq`` transitively (e.g. TigerGraph connection ↔ ConnectionProvider).
# Load these lazily so ``import graflo`` during ``graflo.hq`` initialization does not recurse.
_DB_EXPORTS = frozenset({"ConnectionManager", "ConnectionType"})


def __getattr__(name: str):
    if name in _HQ_EXPORTS:
        import graflo.hq as _hq

        return getattr(_hq, name)
    if name in _DB_EXPORTS:
        import graflo.db as _db

        return getattr(_db, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)


# --- Architecture ----------------------------------------------------------
from .architecture import (
    APIConnector,
    Bindings,
    BoundSourceKind,
    CoreSchema,
    DatabaseProfile,
    Edge,
    EdgeConfig,
    FieldType,
    FileConnector,
    GraphManifest,
    GraphMetadata,
    GraphModel,
    Index,
    IngestionModel,
    JoinClause,
    KafkaConnector,
    Resource,
    ResourceConnector,
    Schema,
    SparqlConnector,
    TableConnector,
    Vertex,
    VertexConfig,
)

# --- Data sources ----------------------------------------------------------
from .data_source import (
    AbstractDataSource,
    APIConfig,
    APIDataSource,
    DataSourceFactory,
    DataSourceRegistry,
    DataSourceType,
    FileDataSource,
    InMemoryDataSource,
    JsonFileDataSource,
    JsonlFileDataSource,
    KafkaConfig,
    KafkaDataSource,
    PaginationConfig,
    SQLConfig,
    SQLDataSource,
    TableFileDataSource,
)

# --- Database (lazy via __getattr__; see _DB_EXPORTS) -------------------------
# --- Filters ---------------------------------------------------------------
from .filter import ComparisonOperator, FilterExpression, LogicalOperator

# --- Enums & utilities -----------------------------------------------------
from .onto import AggregationType, DBType

__all__ = [
    "APIConfig",
    "APIConnector",
    "APIDataSource",
    # Data sources
    "AbstractDataSource",
    # Enums & utilities
    "AggregationType",
    "Bindings",
    "BoundSourceKind",
    "CastBatchResult",
    "Caster",
    # Filters
    "ComparisonOperator",
    # Database
    "ConnectionManager",
    "ConnectionType",
    "CoreSchema",
    "DBType",
    "DataSourceFactory",
    "DataSourceRegistry",
    "DataSourceType",
    "DatabaseProfile",
    "DocCastFailure",
    "DocErrorBudgetExceeded",
    "Edge",
    "EdgeConfig",
    "FieldType",
    "FileConnector",
    "FileDataSource",
    "FilterExpression",
    # Orchestration
    "GraphEngine",
    "GraphManifest",
    "GraphMetadata",
    "GraphModel",
    "InMemoryDataSource",
    "Index",
    "IngestionModel",
    "IngestionParams",
    "JoinClause",
    "JsonFileDataSource",
    "JsonlFileDataSource",
    "KafkaConfig",
    "KafkaConnector",
    "KafkaDataSource",
    "LogicalOperator",
    "PaginationConfig",
    "Resource",
    "ResourceConnector",
    "SQLConfig",
    "SQLDataSource",
    # Architecture
    "Schema",
    "SparqlConnector",
    "TableConnector",
    "TableFileDataSource",
    "Vertex",
    "VertexConfig",
]
