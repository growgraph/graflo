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


class S3GeneralizedConnConfig(BaseModel):
    """Runtime credentials and defaults for S3 staging (TigerGraph bulk ingest)."""

    kind: Literal["s3"] = "s3"
    bucket: str | None = Field(
        default=None,
        description="Default bucket when TigergraphBulkLoadConfig.s3_bucket is unset.",
    )
    region: str | None = Field(default=None)
    aws_access_key_id: str | None = Field(default=None)
    aws_secret_access_key: str | None = Field(default=None)
    endpoint_url: str | None = Field(
        default=None, description="For S3-compatible endpoints (MinIO, etc.)."
    )


GeneralizedConnConfig = (
    PostgresGeneralizedConnConfig
    | SparqlGeneralizedConnConfig
    | S3GeneralizedConnConfig
)


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

    def get_generalized_config_by_proxy(
        self, conn_proxy: str
    ) -> GeneralizedConnConfig | None:
        """Resolve a non-secret proxy name to runtime config (S3, etc.)."""


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

    def get_generalized_config_by_proxy(
        self, conn_proxy: str
    ) -> GeneralizedConnConfig | None:
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
        for connector in bindings.connectors:
            proxy = bindings.get_conn_proxy_for_connector(connector)
            if proxy is not None:
                self.proxy_by_connector_hash[connector.hash] = proxy

    def bind_single_config_for_bindings(
        self,
        *,
        bindings: Bindings,
        conn_proxy: str,
        config: GeneralizedConnConfig,
    ) -> None:
        """Bind one generalized config to all connectors in bindings.

        This is intended for the common case where a single source DB
        (or single generalized API endpoint) supplies all SQL/SPARQL connectors
        in the manifest.

        Raises:
            ValueError: if bindings use multiple different ``conn_proxy`` labels.
        """
        used_proxies: set[str] = set()
        for connector in bindings.connectors:
            proxy = bindings.get_conn_proxy_for_connector(connector)
            if proxy is not None:
                used_proxies.add(proxy)

        if not used_proxies:
            raise ValueError(
                "No connector_connection mappings found in bindings; "
                "expected connector -> conn_proxy rows."
            )

        if used_proxies != {conn_proxy}:
            used = ", ".join(sorted(used_proxies))
            raise ValueError(
                f"Expected all connector_connection mappings to use conn_proxy='{conn_proxy}', "
                f"but found proxies: {used}. For multi-proxy setups, bind explicitly "
                "with register_generalized_config(...) and bind_from_bindings(...)."
            )

        self.register_generalized_config(conn_proxy=conn_proxy, config=config)
        self.bind_from_bindings(bindings=bindings)

    def get_generalized_conn_config(
        self, connector: ResourceConnector
    ) -> GeneralizedConnConfig | None:
        proxy = self.proxy_by_connector_hash.get(connector.hash)
        if proxy is None:
            return None
        return self.configs_by_proxy.get(proxy)

    def get_generalized_config_by_proxy(
        self, conn_proxy: str
    ) -> GeneralizedConnConfig | None:
        return self.configs_by_proxy.get(conn_proxy)

    def register_s3_config(
        self, *, conn_proxy: str, config: S3GeneralizedConnConfig
    ) -> None:
        """Store S3 staging credentials/config under *conn_proxy*."""
        self.configs_by_proxy[conn_proxy] = config

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
