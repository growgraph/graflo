"""Connection configuration and runtime credential resolution.

This package is the single home for *how to reach things*:

- :mod:`graflo.connections.onto` — target-DB config models (``DBConfig`` and
  per-backend subclasses). Pure pydantic; importable without any DB driver.
- :mod:`graflo.connections.graflo_backend` — config for the on-disk GraFlo backend.
- :mod:`graflo.connections.mapping` — ``DBType`` → config-class mapping.
- :mod:`graflo.connections.sources` — runtime *source* connection models
  (REST API, Kafka, files on a local disk or S3) and ``ConnConfig``, the union
  a ``conn_proxy`` label resolves to.
- :mod:`graflo.connections.kinds` — every connection as one union discriminated
  by ``kind``, ``parse_connection`` and the roles each kind plays.
- :mod:`graflo.connections.env` — the ``conn_proxy`` → ``{PREFIX}<FIELD>``
  environment-variable rule.
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
    "TigergraphConfig": "graflo.connections.onto",
    # graflo_backend
    "GraFloBackendConfig": "graflo.connections.graflo_backend",
    # mapping
    "DB_TYPE_MAPPING": "graflo.connections.mapping",
    "get_config_class": "graflo.connections.mapping",
    # kinds — every connection as one union, and its roles
    "CONNECTION_ADAPTER": "graflo.connections.kinds",
    "AnyConnection": "graflo.connections.kinds",
    "Connection": "graflo.connections.kinds",
    "ConnectionKind": "graflo.connections.kinds",
    "Role": "graflo.connections.kinds",
    "connection_kind": "graflo.connections.kinds",
    "parse_connection": "graflo.connections.kinds",
    "roles": "graflo.connections.kinds",
    # sources — runtime source connection models
    "ApiAuth": "graflo.connections.sources",
    "ConnConfig": "graflo.connections.sources",
    "FilesConnConfig": "graflo.connections.sources",
    "KafkaConnConfig": "graflo.connections.sources",
    "KafkaSecurityProtocol": "graflo.connections.sources",
    "RestApiConnConfig": "graflo.connections.sources",
    "S3ConnConfig": "graflo.connections.sources",
    "SparqlAuth": "graflo.connections.sources",
    # env — the conn_proxy → environment-variable naming rule
    "proxy_env_prefix": "graflo.connections.env",
    # provider — runtime resolution (imports the contract; lazy on purpose)
    "ConnectionProvider": "graflo.connections.provider",
    "EmptyConnectionProvider": "graflo.connections.provider",
    "InMemoryConnectionProvider": "graflo.connections.provider",
    "connector_config": "graflo.connections.provider",
    "resolve_secrets_from_env": "graflo.connections.provider",
}

__all__ = [
    "CONNECTION_ADAPTER",
    "DB_TYPE_MAPPING",
    "TARGET_DATABASES",
    "AnyConnection",
    "ApiAuth",
    "ArangoConfig",
    "ConnConfig",
    "Connection",
    "ConnectionKind",
    "ConnectionProvider",
    "DBConfig",
    "EmptyConnectionProvider",
    "FalkordbConfig",
    "FilesConnConfig",
    "GraFloBackendConfig",
    "GraphDBConfig",
    "InMemoryConnectionProvider",
    "KafkaConnConfig",
    "KafkaSecurityProtocol",
    "MemgraphConfig",
    "NebulaConfig",
    "Neo4jConfig",
    "PostgresConfig",
    "RestApiConnConfig",
    "Role",
    "S3ConnConfig",
    "SparqlAuth",
    "SparqlEndpointConfig",
    "TigergraphConfig",
    "connection_kind",
    "connector_config",
    "get_config_class",
    "parse_connection",
    "proxy_env_prefix",
    "resolve_secrets_from_env",
    "roles",
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
