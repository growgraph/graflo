"""Backward import shim for bindings models.

Internal code should import these classes from ``graflo.architecture.contract.bindings``.
"""

from graflo.architecture.contract.bindings import (
    Bindings,
    BoundSourceKind,
    FileConnector,
    JoinClause,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)

__all__ = [
    "Bindings",
    "BoundSourceKind",
    "FileConnector",
    "JoinClause",
    "ResourceConnector",
    "SparqlConnector",
    "TableConnector",
]
