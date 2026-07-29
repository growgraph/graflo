"""graflo: A flexible graph database abstraction layer.

graflo provides a unified interface for working with different graph databases
(ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, Nebula, PostgreSQL) through a
common API. It handles graph operations, data transformations, and query
generation while abstracting away database-specific details.

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

This façade is fully lazy (PEP 562): ``import graflo`` costs almost nothing,
and every ``graflo.X`` package import stays as light as its own dependencies.
For targeted imports (smaller dependency graph), see ``docs/guides/importing.md``
in the package repo. Layering (low to high):

    onto / util / architecture.base
      -> filter, architecture.graph_types
      -> architecture.schema
      -> connections (configs), architecture.contract
      -> architecture.pipeline, data_source, architecture.backend, architecture.evolution
      -> db (live connections), object_storage
      -> hq (orchestration), migrate, rdf, plot, cli
"""

from __future__ import annotations

from typing import Any

_EXPORTS: dict[str, str] = {
    # --- Enums & core vocabulary -----------------------------------------
    "AggregationType": "graflo.onto",
    "DBType": "graflo.onto",
    # --- Architecture (schema + contract) --------------------------------
    "APIConnector": "graflo.architecture",
    "Bindings": "graflo.architecture",
    "BoundSourceKind": "graflo.architecture",
    "CoreSchema": "graflo.architecture",
    "DatabaseProfile": "graflo.architecture",
    "Edge": "graflo.architecture",
    "EdgeConfig": "graflo.architecture",
    "FieldType": "graflo.architecture",
    "FileConnector": "graflo.architecture",
    "GraphManifest": "graflo.architecture",
    "GraphMetadata": "graflo.architecture",
    "GraphModel": "graflo.architecture",
    "Index": "graflo.architecture",
    "IngestionModel": "graflo.architecture",
    "KafkaConnector": "graflo.architecture",
    "Resource": "graflo.architecture",
    "ResourceConnector": "graflo.architecture",
    "Schema": "graflo.architecture",
    "SparqlConnector": "graflo.architecture",
    "TableConnector": "graflo.architecture",
    "Vertex": "graflo.architecture",
    "VertexConfig": "graflo.architecture",
    "PaginationConfig": "graflo.architecture.contract.bindings",
    # --- Filters ----------------------------------------------------------
    "ComparisonOperator": "graflo.filter",
    "FilterExpression": "graflo.filter",
    "JoinClause": "graflo.filter",
    "LogicalOperator": "graflo.filter",
    # --- Data sources ------------------------------------------------------
    "APIConfig": "graflo.data_source",
    "APIDataSource": "graflo.data_source",
    "AbstractDataSource": "graflo.data_source",
    "DataSourceFactory": "graflo.data_source",
    "DataSourceRegistry": "graflo.data_source",
    "DataSourceType": "graflo.data_source",
    "FileDataSource": "graflo.data_source",
    "InMemoryDataSource": "graflo.data_source",
    "JsonFileDataSource": "graflo.data_source",
    "JsonlFileDataSource": "graflo.data_source",
    "KafkaConfig": "graflo.data_source",
    "KafkaDataSource": "graflo.data_source",
    "SQLConfig": "graflo.data_source",
    "SQLDataSource": "graflo.data_source",
    "TableFileDataSource": "graflo.data_source",
    # --- Database (live connections) --------------------------------------
    "ConnectionManager": "graflo.db",
    "ConnectionType": "graflo.db",
    # --- Orchestration -----------------------------------------------------
    "CastBatchResult": "graflo.hq",
    "Caster": "graflo.hq",
    "DocCastFailure": "graflo.hq",
    "DocErrorBudgetExceeded": "graflo.hq",
    "GraphEngine": "graflo.hq",
    "IngestionParams": "graflo.hq",
}

__all__ = [
    "APIConfig",
    "APIConnector",
    "APIDataSource",
    "AbstractDataSource",
    "AggregationType",
    "Bindings",
    "BoundSourceKind",
    "CastBatchResult",
    "Caster",
    "ComparisonOperator",
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
    "Schema",
    "SparqlConnector",
    "TableConnector",
    "TableFileDataSource",
    "Vertex",
    "VertexConfig",
]


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        import importlib

        value = getattr(importlib.import_module(_EXPORTS[name]), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
