"""Connection configuration and runtime credential resolution.

This package is the single home for *how to reach things*:

- :mod:`graflo.connections.onto` — target-DB config models (``DBConfig`` and
  per-backend subclasses). Pure pydantic; importable without any DB driver.
- :mod:`graflo.connections.graflo_backend` — config for the on-disk GraFlo backend.
- :mod:`graflo.connections.mapping` — ``DBType`` → config-class mapping.
- :mod:`graflo.connections.sources` — runtime *source* connection models
  (REST API, Kafka, SPARQL, S3 credentials).
- :mod:`graflo.connections.provider` — ``ConnectionProvider`` protocol resolving
  ``conn_proxy`` labels to runtime configs (keeps YAML manifests secret-free).

Layering: ``onto`` / ``graflo_backend`` / ``mapping`` / ``sources`` sit below
``graflo.architecture.contract`` (bindings reference ``PostgresConfig``);
``provider`` sits above it (it resolves contract connectors). The façade is
lazy so importing one side never drags in the other.
"""

from __future__ import annotations

from typing import Any

_EXPORTS: dict[str, str] = {
    # onto — target DB configs
    "TARGET_DATABASES": "graflo.connections.onto",
    "ArangoConfig": "graflo.connections.onto",
    "DBConfig": "graflo.connections.onto",
    "FalkordbConfig": "graflo.connections.onto",
    "MemgraphConfig": "graflo.connections.onto",
    "NebulaConfig": "graflo.connections.onto",
    "Neo4jConfig": "graflo.connections.onto",
    "PostgresConfig": "graflo.connections.onto",
    "GraphDBConfig": "graflo.connections.onto",
    "SparqlEndpointConfig": "graflo.connections.onto",
    "TigergraphBulkLoadConfig": "graflo.connections.onto",
    "TigergraphBulkLoadJobOptions": "graflo.connections.onto",
    "TigergraphConfig": "graflo.connections.onto",
    # graflo_backend
    "GraFloBackendConfig": "graflo.connections.graflo_backend",
    # mapping
    "DB_TYPE_MAPPING": "graflo.connections.mapping",
    "get_config_class": "graflo.connections.mapping",
    # sources — runtime source connection models
    "ApiAuth": "graflo.connections.sources",
    "ApiGeneralizedConnConfig": "graflo.connections.sources",
    "GeneralizedConnConfig": "graflo.connections.sources",
    "KafkaConnConfig": "graflo.connections.sources",
    "KafkaGeneralizedConnConfig": "graflo.connections.sources",
    "KafkaSecurityProtocol": "graflo.connections.sources",
    "PostgresGeneralizedConnConfig": "graflo.connections.sources",
    "RestApiConnConfig": "graflo.connections.sources",
    "S3GeneralizedConnConfig": "graflo.connections.sources",
    "SparqlAuth": "graflo.connections.sources",
    "SparqlGeneralizedConnConfig": "graflo.connections.sources",
    # provider — runtime resolution (imports the contract; lazy on purpose)
    "ConnectionProvider": "graflo.connections.provider",
    "EmptyConnectionProvider": "graflo.connections.provider",
    "InMemoryConnectionProvider": "graflo.connections.provider",
}

__all__ = [
    "DB_TYPE_MAPPING",
    "TARGET_DATABASES",
    "ApiAuth",
    "ApiGeneralizedConnConfig",
    "ArangoConfig",
    "ConnectionProvider",
    "DBConfig",
    "EmptyConnectionProvider",
    "FalkordbConfig",
    "GeneralizedConnConfig",
    "GraFloBackendConfig",
    "GraphDBConfig",
    "InMemoryConnectionProvider",
    "KafkaConnConfig",
    "KafkaGeneralizedConnConfig",
    "KafkaSecurityProtocol",
    "MemgraphConfig",
    "NebulaConfig",
    "Neo4jConfig",
    "PostgresConfig",
    "PostgresGeneralizedConnConfig",
    "RestApiConnConfig",
    "S3GeneralizedConnConfig",
    "SparqlAuth",
    "SparqlEndpointConfig",
    "SparqlGeneralizedConnConfig",
    "TigergraphBulkLoadConfig",
    "TigergraphBulkLoadJobOptions",
    "TigergraphConfig",
    "get_config_class",
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
