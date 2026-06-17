"""Runtime connection/config resolution for source connectors.

This module defines a connector-centric runtime indirection:
``Bindings (contract) -> conn_proxy name -> GeneralizedConnConfig (runtime)``.
"""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, Field

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.db.connection import PostgresConfig, SparqlEndpointConfig
from graflo.connection_models import (
    ApiAuth,
    ApiGeneralizedConnConfig,
    GeneralizedConnConfig,
    PostgresGeneralizedConnConfig,
    RestApiConnConfig,
    S3GeneralizedConnConfig,
    SparqlAuth,
    SparqlGeneralizedConnConfig,
)

__all__ = [
    "ApiAuth",
    "ApiGeneralizedConnConfig",
    "ConnectionProvider",
    "EmptyConnectionProvider",
    "GeneralizedConnConfig",
    "InMemoryConnectionProvider",
    "PostgresGeneralizedConnConfig",
    "RestApiConnConfig",
    "S3GeneralizedConnConfig",
    "SparqlAuth",
    "SparqlGeneralizedConnConfig",
]


def _proxy_to_env_prefix(conn_proxy: str) -> str:
    """Map a ``conn_proxy`` label to an environment-variable prefix."""
    return conn_proxy.upper().replace("-", "_") + "_"


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

    configs_by_proxy: dict[str, GeneralizedConnConfig] = Field(default_factory=dict)
    proxy_by_connector_hash: dict[str, str] = Field(default_factory=dict)

    postgres_by_resource: dict[str, PostgresConfig] = Field(default_factory=dict)
    sparql_by_resource: dict[str, SparqlEndpointConfig] = Field(default_factory=dict)
    sparql_by_endpoint: dict[str, SparqlEndpointConfig] = Field(default_factory=dict)
    default_sparql: SparqlEndpointConfig | None = None

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

    def register_api_config_from_env(
        self,
        *,
        conn_proxy: str,
        env_prefix: str | None = None,
    ) -> None:
        """Register REST API runtime config for *conn_proxy* from environment variables.

        *env_prefix* defaults to a proxy-derived prefix (e.g. ``user_service`` →
        ``USER_SERVICE_``), reading ``{prefix}BASE_URL`` and optional auth vars.
        """
        prefix = (
            env_prefix if env_prefix is not None else _proxy_to_env_prefix(conn_proxy)
        )
        runtime = RestApiConnConfig.from_env(env_prefix=prefix)
        self.register_generalized_config(
            conn_proxy=conn_proxy,
            config=ApiGeneralizedConnConfig(config=runtime),
        )

    def register_all_api_configs_from_env(
        self,
        *,
        bindings: Bindings,
        env_prefix_map: dict[str, str] | None = None,
    ) -> None:
        """Register env-backed API configs for all API ``conn_proxy`` labels in *bindings*.

        Discovers unique ``conn_proxy`` values attached to :class:`APIConnector`
        instances, loads each via :meth:`register_api_config_from_env`, then binds
        all connectors from *bindings*.

        Args:
            bindings: Manifest bindings with ``connector_connection`` rows.
            env_prefix_map: Optional per-proxy env prefix overrides.
        """
        prefix_map = env_prefix_map or {}
        api_proxies: set[str] = set()
        for connector in bindings.connectors:
            if not isinstance(connector, APIConnector):
                continue
            proxy = bindings.get_conn_proxy_for_connector(connector)
            if proxy is not None:
                api_proxies.add(proxy)

        if not api_proxies:
            raise ValueError(
                "No API connector_connection mappings found in bindings; "
                "expected at least one APIConnector with a conn_proxy."
            )

        for conn_proxy in sorted(api_proxies):
            self.register_api_config_from_env(
                conn_proxy=conn_proxy,
                env_prefix=prefix_map.get(conn_proxy),
            )
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
