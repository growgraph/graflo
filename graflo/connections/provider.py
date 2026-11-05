"""Runtime resolution of ``conn_proxy`` labels to connection configs.

Manifests stay secret-free: a connector names a ``conn_proxy`` label in its
bindings, and a :class:`ConnectionProvider` maps each label to a
:data:`~graflo.connections.sources.ConnConfig` at run time. Which connector
uses which label is read from the bindings at lookup, never copied into the
provider.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Protocol, TypeVar, cast

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    FileConnector,
    KafkaConnector,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.architecture.contract.bindings.core import AnyConnector
from graflo.connections.env import proxy_env_prefix
from graflo.connections.kinds import AnyConnection
from graflo.connections.onto import DBConfig, PostgresConfig, SparqlEndpointConfig
from graflo.connections.sources import (
    ApiAuth,
    ConnConfig,
    FilesConnConfig,
    KafkaConnConfig,
    RestApiConnConfig,
    S3ConnConfig,
    SparqlAuth,
)

__all__ = [
    "ApiAuth",
    "ConnConfig",
    "ConnectionProvider",
    "EmptyConnectionProvider",
    "FilesConnConfig",
    "InMemoryConnectionProvider",
    "KafkaConnConfig",
    "RestApiConnConfig",
    "S3ConnConfig",
    "SparqlAuth",
    "connector_config",
    "proxy_env_prefix",
    "resolve_db_secrets_from_env",
    "resolve_secrets_from_env",
]

_D = TypeVar("_D", bound=DBConfig)
_F = TypeVar("_F", KafkaConnConfig, FilesConnConfig)
_C = TypeVar("_C", bound=AnyConnection)
_T = TypeVar(
    "_T",
    PostgresConfig,
    SparqlEndpointConfig,
    RestApiConnConfig,
    KafkaConnConfig,
    FilesConnConfig,
)

#: The config type each kind of proxied connector needs.
_CONFIG_TYPE_BY_CONNECTOR: tuple[tuple[type[ResourceConnector], type], ...] = (
    (TableConnector, PostgresConfig),
    (SparqlConnector, SparqlEndpointConfig),
    (APIConnector, RestApiConnConfig),
    (KafkaConnector, KafkaConnConfig),
    (FileConnector, FilesConnConfig),
)


def _config_type_for(connector: ResourceConnector) -> type | None:
    for connector_type, config_type in _CONFIG_TYPE_BY_CONNECTOR:
        if isinstance(connector, connector_type):
            return config_type
    return None


def resolve_db_secrets_from_env(
    config: _D,
    conn_proxy: str,
    env_prefix: str | None = None,
) -> _D:
    """Fill a stored database config's missing username and password from the environment.

    The same rule as :func:`resolve_secrets_from_env`, for any database config,
    a graph target included: ``{prefix}USERNAME`` / ``{prefix}PASSWORD`` fill
    what *config* lacks and never override a stored value.

    Returns:
        A copy of *config* with missing credentials filled, or *config* itself
        when nothing was filled.
    """
    prefix = env_prefix if env_prefix is not None else proxy_env_prefix(conn_proxy)
    update = {
        field: value
        for field in ("username", "password")
        if (value := os.environ.get(f"{prefix}{field.upper()}") or None) is not None
        and getattr(config, field) is None
    }
    return config.model_copy(update=update) if update else config


def resolve_secrets_from_env(
    config: _C,
    conn_proxy: str,
    env_prefix: str | None = None,
) -> _C:
    """Fill a stored config's missing credentials from the environment.

    A registry stores connections without secrets; the secret behind a
    ``conn_proxy`` label arrives at run time, read from ``{prefix}<NAME>``
    where *prefix* defaults to :func:`proxy_env_prefix`:

    - database and SPARQL: ``USERNAME``, ``PASSWORD``;
    - REST API: ``TOKEN``, ``USERNAME``, ``PASSWORD``;
    - Kafka: ``SASL_USERNAME``, ``SASL_PASSWORD``;
    - files on S3: ``ACCESS_KEY_ID``, ``SECRET_ACCESS_KEY``.

    A value already present on *config* is kept: the environment fills gaps
    and never overrides a stored setting.

    Returns:
        A copy of *config* with missing credentials filled, or *config* itself
        when nothing was filled.
    """
    prefix = env_prefix if env_prefix is not None else proxy_env_prefix(conn_proxy)

    if isinstance(config, DBConfig):
        return resolve_db_secrets_from_env(config, conn_proxy, prefix)

    if isinstance(config, KafkaConnConfig):
        return cast(
            _C, _fill_from_env(config, ("sasl_username", "sasl_password"), prefix)
        )

    if isinstance(config, FilesConnConfig):
        if not config.is_s3:
            return config
        return cast(
            _C,
            _fill_from_env(config, ("access_key_id", "secret_access_key"), prefix),
        )

    if isinstance(config, RestApiConnConfig):
        auth = config.auth or ApiAuth()
        update = {
            field: value
            for field in ("token", "username", "password")
            if (value := os.environ.get(f"{prefix}{field.upper()}") or None) is not None
            and getattr(auth, field) is None
        }
        if not update:
            return config
        return config.model_copy(update={"auth": auth.model_copy(update=update)})

    return config


def _fill_from_env(config: _F, fields: tuple[str, ...], prefix: str) -> _F:
    """*config* with each of *fields* it lacks taken from ``{prefix}<FIELD>``."""
    update = {
        field: value
        for field in fields
        if (value := os.environ.get(f"{prefix}{field.upper()}") or None) is not None
        and getattr(config, field) is None
    }
    return config.model_copy(update=update) if update else config


class ConnectionProvider(Protocol):
    """Resolve a ``conn_proxy`` label to its runtime connection config."""

    def get_config(self, conn_proxy: str) -> ConnConfig | None:
        """The config registered under *conn_proxy*, or ``None``."""
        ...


class EmptyConnectionProvider:
    """A provider with no configs: every label resolves to ``None``."""

    def get_config(self, conn_proxy: str) -> ConnConfig | None:
        return None


class InMemoryConnectionProvider:
    """``conn_proxy`` label → config, held in memory.

    Build it from a mapping, ``InMemoryConnectionProvider({"shop_db": cfg})``,
    or from the environment with :meth:`from_env`.
    """

    def __init__(self, configs: Mapping[str, ConnConfig] | None = None) -> None:
        self.configs: dict[str, ConnConfig] = dict(configs or {})

    def register(self, conn_proxy: str, config: ConnConfig) -> None:
        """Register (or replace) the config of *conn_proxy*."""
        self.configs[conn_proxy] = config

    def get_config(self, conn_proxy: str) -> ConnConfig | None:
        return self.configs.get(conn_proxy)

    @classmethod
    def from_env(
        cls,
        bindings: Bindings,
        *,
        env_prefix_map: dict[str, str] | None = None,
    ) -> InMemoryConnectionProvider:
        """One config per ``conn_proxy`` the bindings use, read from the environment.

        The config type follows the connectors that name the label: table →
        :class:`PostgresConfig`, SPARQL → :class:`SparqlEndpointConfig`, REST
        API → :class:`RestApiConnConfig`, Kafka → :class:`KafkaConnConfig`,
        file → :class:`FilesConnConfig`. Each reads ``{PREFIX}<FIELD>``,
        *PREFIX* from :func:`proxy_env_prefix` unless *env_prefix_map* names
        one for the label. Labels named only by push connectors are skipped.

        Raises:
            ValueError: A label is named by connectors needing different config
                types, or a label's required variables are missing — every
                such label is listed in one error.
        """
        prefix_map = env_prefix_map or {}
        configs: dict[str, ConnConfig] = {}
        problems: list[str] = []
        for proxy, connectors in bindings.conn_proxies().items():
            needed = {t for c in connectors if (t := _config_type_for(c)) is not None}
            if not needed:
                continue
            if len(needed) > 1:
                kinds = ", ".join(sorted(t.__name__ for t in needed))
                problems.append(f"{proxy!r} is used by connectors needing {kinds}")
                continue
            config_type = needed.pop()
            prefix = prefix_map.get(proxy, proxy_env_prefix(proxy))
            try:
                config = _load_from_env(config_type, proxy, prefix)
            except ValueError as exc:
                problems.append(f"{proxy!r}: {exc}")
                continue
            configs[proxy] = config
        if problems:
            raise ValueError(
                "Cannot load connection configs from the environment: "
                + "; ".join(problems)
            )
        return cls(configs)


def _load_from_env(config_type: type, proxy: str, prefix: str) -> ConnConfig:
    if config_type is RestApiConnConfig:
        return RestApiConnConfig.from_env(env_prefix=prefix)
    if config_type is KafkaConnConfig:
        return KafkaConnConfig.from_env(env_prefix=prefix)
    if config_type is FilesConnConfig:
        return FilesConnConfig.from_env(env_prefix=prefix)
    if config_type is PostgresConfig:
        config = PostgresConfig.from_proxy_env(proxy, env_prefix=prefix)
        if config.uri is None:
            raise ValueError(f"environment variable {prefix}URI is required")
        return config
    return SparqlEndpointConfig.from_proxy_env(proxy, env_prefix=prefix)


def connector_config(
    connector: AnyConnector,
    bindings: Bindings,
    provider: ConnectionProvider,
    expected: type[_T],
) -> _T:
    """The config of *connector*'s ``conn_proxy``, checked to be an *expected*.

    Raises:
        ValueError: The connector has no ``conn_proxy``, nothing is registered
            under it, or what is registered is another kind of config.
    """
    label = connector.name or connector.hash
    proxy = bindings.get_conn_proxy_for_connector(connector)
    if proxy is None:
        raise ValueError(
            f"connector {label!r} has no conn_proxy; "
            f"a {expected.__name__} is needed to read it"
        )
    config = provider.get_config(proxy)
    if config is None:
        raise ValueError(
            f"no connection configuration is registered for conn_proxy "
            f"{proxy!r} (connector {label!r})"
        )
    if not isinstance(config, expected):
        raise ValueError(
            f"conn_proxy {proxy!r} (connector {label!r}) holds a "
            f"{type(config).__name__}; a {expected.__name__} is needed"
        )
    return config
