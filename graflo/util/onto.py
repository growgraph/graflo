"""Backward import shim for bindings models.

Internal code should import these classes from ``graflo.architecture.contract.bindings``.
"""

from graflo.architecture.contract.bindings import (
    Bindings,
    BoundSourceKind,
    APIConnector,
    ApiResponseStructure,
    FileConnector,
    JoinClause,
    PaginationConfig,
    PaginationRequestConfig,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)

__all__ = [
    "Bindings",
    "BoundSourceKind",
    "APIConnector",
    "ApiResponseStructure",
    "FileConnector",
    "JoinClause",
    "PaginationConfig",
    "PaginationRequestConfig",
    "ResourceConnector",
    "SparqlConnector",
    "TableConnector",
]
