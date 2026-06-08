"""Data source abstraction layer for graph database ingestion.

This package provides a unified interface for different data source types,
separating "where data comes from" (DataSource) from "how it's transformed" (Resource).

Key Components:
    - AbstractDataSource: Base class for all data sources
    - FileDataSource: File-based data sources (JSON, JSONL, CSV/TSV)
    - APIDataSource: REST API runtime executor (built from APIConnector + conn_proxy)
    - SQLDataSource: SQL database data source
    - DataSourceRegistry: Maps DataSources to Resource names
"""

from .api import APIConfig, APIDataSource
from .base import AbstractDataSource, DataSourceType
from .factory import DataSourceFactory
from .file import (
    FileDataSource,
    JsonFileDataSource,
    JsonlFileDataSource,
    TableFileDataSource,
)
from .memory import InMemoryDataSource
from .registry import DataSourceRegistry
from .sql import SQLConfig, SQLDataSource
from graflo.architecture.contract.bindings import APIConnector, PaginationConfig

__all__ = [
    "AbstractDataSource",
    "APIConfig",
    "APIConnector",
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
]

# RDF / SPARQL data sources (rdflib, SPARQLWrapper — core deps; optional import if missing)
try:
    from .rdf import (  # noqa: F401
        RdfDataSource,
        RdfFileDataSource,
        SparqlDataSource,
        SparqlEndpointDataSource,
        SparqlSourceConfig,
    )

    __all__ += [
        "RdfDataSource",
        "RdfFileDataSource",
        "SparqlDataSource",
        "SparqlEndpointDataSource",
        "SparqlSourceConfig",
    ]
except ImportError:
    pass
