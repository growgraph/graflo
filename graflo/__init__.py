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
from .architecture import (  # noqa: E402
    APIConnector,
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
    BoundSourceKind,
    ResourceConnector,
    Resource,
    SparqlConnector,
    Schema,
    TableConnector,
    Vertex,
    VertexConfig,
)

# --- Data sources ----------------------------------------------------------
from .data_source import (  # noqa: E402
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

# --- Database (lazy via __getattr__; see _DB_EXPORTS) -------------------------

# --- Filters ---------------------------------------------------------------
from .filter import ComparisonOperator, FilterExpression, LogicalOperator  # noqa: E402

# --- Enums & utilities -----------------------------------------------------
from .onto import AggregationType, DBType  # noqa: E402

__all__ = [
    # Orchestration
    "GraphEngine",
    "Caster",
    "CastBatchResult",
    "IngestionParams",
    "DocCastFailure",
    "DocErrorBudgetExceeded",
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
    "APIConnector",
    "FileConnector",
    "Bindings",
    "JoinClause",
    "BoundSourceKind",
    "ResourceConnector",
    "SparqlConnector",
    "TableConnector",
]
