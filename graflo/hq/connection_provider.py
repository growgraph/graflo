"""Runtime connection/config resolution for source connectors.

This module defines a connector-centric runtime indirection:
``Bindings (contract) -> conn_proxy name -> GeneralizedConnConfig (runtime)``.
"""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, Field

from graflo.architecture.contract.bindings import (
    Bindings,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.db import PostgresConfig, SparqlEndpointConfig


class SparqlAuth(BaseModel):
    """Authentication payload for SPARQL endpoint access."""

    username: str | None = None
    password: str | None = None


class PostgresGeneralizedConnConfig(BaseModel):
    """Generalized runtime config variant for SQL/Postgres connections."""

    kind: Literal["postgres"] = "postgres"
    config: PostgresConfig


class SparqlGeneralizedConnConfig(BaseModel):
    """Generalized runtime config variant for SPARQL endpoint connections."""

    kind: Literal["sparql"] = "sparql"
    config: SparqlEndpointConfig


GeneralizedConnConfig = PostgresGeneralizedConnConfig | SparqlGeneralizedConnConfig


class ConnectionProvider(Protocol):
    """Resolve runtime source connection/auth configuration.

    New connector-centric resolution (preferred):
    - :meth:`get_generalized_conn_config` takes a connector and returns the
      generalized runtime config.

    Legacy helpers (kept for backwards compatibility):
    - :meth:`get_postgres_config`
    - :meth:`get_sparql_auth`
    """

    def get_generalized_conn_config(
        self, connector: ResourceConnector
    ) -> GeneralizedConnConfig | None:
        """Return generalized runtime config for a connector."""

    def get_postgres_config(
        self, resource_name: str, connector: TableConnector
    ) -> PostgresConfig | None:
        """Return source DB config for a SQL table resource (legacy)."""

    def get_sparql_auth(
        self, resource_name: str, connector: SparqlConnector
    ) -> SparqlAuth | None:
        """Return source auth payload for a SPARQL resource (legacy)."""


class EmptyConnectionProvider:
    """No-op provider when no source credentials/config are configured."""

    def get_generalized_conn_config(
        self, connector: ResourceConnector
    ) -> GeneralizedConnConfig | None:
        return None

    def get_postgres_config(
        self, resource_name: str, connector: TableConnector
    ) -> PostgresConfig | None:
        return None

    def get_sparql_auth(
        self, resource_name: str, connector: SparqlConnector
    ) -> SparqlAuth | None:
        return None


class InMemoryConnectionProvider(BaseModel):
    """Simple in-memory provider for proxy-based generalized configs.

    Supports two wiring modes:
    - New: ``proxy_by_connector_hash`` + ``configs_by_proxy``
    - Legacy: per-resource maps (``postgres_by_resource`` / ``sparql_by_resource``)
    """

    # New wiring.
    configs_by_proxy: dict[str, GeneralizedConnConfig] = Field(default_factory=dict)
    proxy_by_connector_hash: dict[str, str] = Field(default_factory=dict)

    # Legacy wiring (kept to avoid breaking existing providers).
    postgres_by_resource: dict[str, PostgresConfig] = Field(default_factory=dict)
    sparql_by_resource: dict[str, SparqlEndpointConfig] = Field(default_factory=dict)
    sparql_by_endpoint: dict[str, SparqlEndpointConfig] = Field(default_factory=dict)
    default_sparql: SparqlEndpointConfig | None = None

    # ------------------------------------------------------------------
    # New API
    # ------------------------------------------------------------------
    def register_generalized_config(
        self, *, conn_proxy: str, config: GeneralizedConnConfig
    ) -> None:
        self.configs_by_proxy[conn_proxy] = config

    def bind_connector_to_conn_proxy(
        self, *, connector: ResourceConnector, conn_proxy: str
    ) -> None:
        self.proxy_by_connector_hash[connector.hash] = conn_proxy

    def bind_from_bindings(self, *, bindings: Bindings) -> None:
        """Populate ``proxy_by_connector_hash`` from the contract bindings."""
        for entry in bindings.connector_connection_bindings:
            for connector in bindings.connectors:
                if (
                    entry.connector == connector.hash
                    or entry.connector == connector.name
                ):
                    self.proxy_by_connector_hash[connector.hash] = entry.conn_proxy

    def get_generalized_conn_config(
        self, connector: ResourceConnector
    ) -> GeneralizedConnConfig | None:
        proxy = self.proxy_by_connector_hash.get(connector.hash)
        if proxy is None:
            return None
        return self.configs_by_proxy.get(proxy)

    # ------------------------------------------------------------------
    # Legacy API
    # ------------------------------------------------------------------
    def get_postgres_config(
        self, resource_name: str, connector: TableConnector
    ) -> PostgresConfig | None:
        generalized = self.get_generalized_conn_config(connector)
        if isinstance(generalized, PostgresGeneralizedConnConfig):
            return generalized.config
        return self.postgres_by_resource.get(resource_name)

    def get_sparql_auth(
        self, resource_name: str, connector: SparqlConnector
    ) -> SparqlAuth | None:
        generalized = self.get_generalized_conn_config(connector)
        if isinstance(generalized, SparqlGeneralizedConnConfig):
            cfg = generalized.config
            return SparqlAuth(username=cfg.username, password=cfg.password)

        cfg = self.sparql_by_resource.get(resource_name)
        if cfg is None and connector.endpoint_url:
            cfg = self.sparql_by_endpoint.get(connector.endpoint_url)
        if cfg is None:
            cfg = self.default_sparql
        if cfg is None:
            return None
        return SparqlAuth(username=cfg.username, password=cfg.password)
