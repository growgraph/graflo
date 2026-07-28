"""Backward import shim for bindings models.

Internal code should import these classes from ``graflo.architecture.contract.bindings``.
"""

from graflo.architecture.contract.bindings import (
    APIConnector,
    ApiResponseStructure,
    Bindings,
    BoundSourceKind,
    FileConnector,
    JoinClause,
    PaginationConfig,
    PaginationRequestConfig,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)

__all__ = [
    "APIConnector",
    "ApiResponseStructure",
    "Bindings",
    "BoundSourceKind",
    "FileConnector",
    "JoinClause",
    "PaginationConfig",
    "PaginationRequestConfig",
    "ResourceConnector",
    "SparqlConnector",
    "TableConnector",
]
