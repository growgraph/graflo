"""Declarative contracts: manifest, bindings, ingestion models, resources, transforms."""

from __future__ import annotations

from typing import Any

from .bindings import (
    APIConnector,
    ApiResponseStructure,
    Bindings,
    BoundSourceKind,
    ColumnTimeFilter,
    FileConnector,
    JoinClause,
    KafkaConnector,
    PaginationConfig,
    PaginationRequestConfig,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)
from .manifest import GraphManifest

__all__ = [
    "APIConnector",
    "ApiResponseStructure",
    "Bindings",
    "BoundSourceKind",
    "ColumnTimeFilter",
    "FileConnector",
    "GraphManifest",
    "IngestionModel",
    "JoinClause",
    "KafkaConnector",
    "PaginationConfig",
    "PaginationRequestConfig",
    "ProtoTransform",
    "Resource",
    "ResourceConfig",
    "ResourceConnector",
    "ResourceRuntime",
    "SparqlConnector",
    "TableConnector",
    "Transform",
    "build_resource_runtime",
]

_INGESTION_EXPORTS = frozenset(
    {
        "IngestionModel",
        "ProtoTransform",
        "Resource",
        "ResourceConfig",
        "ResourceRuntime",
        "Transform",
        "build_resource_runtime",
    }
)


def __getattr__(name: str) -> Any:
    # Lazy: ingestion → runtime → graph_types; keep bindings importable without that cycle.
    if name in _INGESTION_EXPORTS:
        from . import ingestion as _ingestion

        value = getattr(_ingestion, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
