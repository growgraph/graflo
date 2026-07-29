"""Data source abstraction layer for graph database ingestion.

This package provides a unified interface for different data source types,
separating "where data comes from" (DataSource) from "how it's transformed" (Resource).

Key Components:
    - AbstractDataSource: Base class for all data sources
    - FileDataSource: File-based data sources (JSON, JSONL, CSV/TSV)
    - APIDataSource: REST API runtime executor (built from APIConnector + conn_proxy)
    - SQLDataSource: SQL database data source
    - DataSourceRegistry: Maps DataSources to Resource names

Connector *models* (``APIConnector``, pagination configs, …) live in
:mod:`graflo.architecture.contract.bindings`. This façade is lazy (PEP 562):
importing ``graflo.data_source`` does not load source drivers (Kafka, SPARQL,
…); they load on first attribute access.
"""

from __future__ import annotations

from typing import Any

_EXPORTS: dict[str, str] = {
    "APIConfig": "graflo.data_source.api",
    "APIDataSource": "graflo.data_source.api",
    "AbstractDataSource": "graflo.data_source.base",
    "DataSourceType": "graflo.data_source.base",
    "DataSourceFactory": "graflo.data_source.factory",
    "FileDataSource": "graflo.data_source.file",
    "JsonFileDataSource": "graflo.data_source.file",
    "JsonlFileDataSource": "graflo.data_source.file",
    "TableFileDataSource": "graflo.data_source.file",
    "KafkaConfig": "graflo.data_source.kafka",
    "KafkaDataSource": "graflo.data_source.kafka",
    "InMemoryDataSource": "graflo.data_source.memory",
    "DataSourceRegistry": "graflo.data_source.registry",
    "SQLConfig": "graflo.data_source.sql",
    "SQLDataSource": "graflo.data_source.sql",
    "RdfDataSource": "graflo.data_source.rdf",
    "RdfFileDataSource": "graflo.data_source.rdf",
    "SparqlDataSource": "graflo.data_source.rdf",
    "SparqlEndpointDataSource": "graflo.data_source.rdf",
    "SparqlSourceConfig": "graflo.data_source.rdf",
}

__all__ = [
    "APIConfig",
    "APIDataSource",
    "AbstractDataSource",
    "DataSourceFactory",
    "DataSourceRegistry",
    "DataSourceType",
    "FileDataSource",
    "InMemoryDataSource",
    "JsonFileDataSource",
    "JsonlFileDataSource",
    "KafkaConfig",
    "KafkaDataSource",
    "RdfDataSource",
    "RdfFileDataSource",
    "SQLConfig",
    "SQLDataSource",
    "SparqlDataSource",
    "SparqlEndpointDataSource",
    "SparqlSourceConfig",
    "TableFileDataSource",
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
