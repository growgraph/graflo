from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import APIConnector, Bindings, TableConnector
from graflo.db import PostgresConfig
from graflo.hq.connection_provider import (
    ApiGeneralizedConnConfig,
    InMemoryConnectionProvider,
    PostgresGeneralizedConnConfig,
    RestApiConnConfig,
)


def test_bindings_connector_connection_resolves_by_name_and_hash() -> None:
    connector = TableConnector(table_name="t1", schema_name="public", name="c1")

    bindings_by_name = Bindings(
        connectors=[connector],
        resource_connector=[],
        connector_connection=[{"connector": "c1", "conn_proxy": "pg"}],
    )
    assert bindings_by_name.get_conn_proxy_for_connector(connector) == "pg"

    bindings_by_hash = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": connector.hash, "conn_proxy": "pg2"}],
    )
    assert bindings_by_hash.get_conn_proxy_for_connector(connector) == "pg2"


def test_bindings_connector_connection_resolves_by_connector_hash_without_name() -> (
    None
):
    """connector_connection must reference connector hash or name, not resource_name."""
    connector = TableConnector(
        table_name="t1",
        schema_name="public",
        resource_name="people",
    )
    bindings = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": connector.hash, "conn_proxy": "pg"}],
    )
    assert bindings.get_conn_proxy_for_connector(connector) == "pg"


def test_bindings_connector_connection_conflicting_proxies_raises() -> None:
    connector = TableConnector(table_name="t1", schema_name="public", name="c1")
    with pytest.raises(ValueError, match="Conflicting conn_proxy mapping"):
        Bindings(
            connectors=[connector],
            connector_connection=[
                {"connector": "c1", "conn_proxy": "pg1"},
                {"connector": "c1", "conn_proxy": "pg2"},
            ],
        )


def test_provider_resolves_connector_based_config_for_multiple_resources() -> None:
    connector = TableConnector(table_name="t1", schema_name="public", name="c1")
    pg_cfg = PostgresConfig(
        uri="postgresql://localhost:5432/db",
        username="u",
        password="p",
        database="db",
        schema_name="public",
    )

    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="postgres_source",
        config=PostgresGeneralizedConnConfig(config=pg_cfg),
    )
    provider.bind_connector_to_conn_proxy(
        connector=connector, conn_proxy="postgres_source"
    )

    cfg1 = provider.get_postgres_config(resource_name="r1", connector=connector)
    cfg2 = provider.get_postgres_config(resource_name="r2", connector=connector)

    assert cfg1 is not None
    assert cfg2 is not None
    assert cfg1.uri == pg_cfg.uri
    assert cfg2.uri == pg_cfg.uri


def test_provider_bind_from_bindings_supports_connector_hash() -> None:
    connector = TableConnector(
        table_name="t1",
        schema_name="public",
        resource_name="people",
    )
    pg_cfg = PostgresConfig(
        uri="postgresql://localhost:5432/db",
        username="u",
        password="p",
        database="db",
        schema_name="public",
    )

    bindings = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": connector.hash, "conn_proxy": "pg"}],
    )

    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="pg",
        config=PostgresGeneralizedConnConfig(config=pg_cfg),
    )
    provider.bind_from_bindings(bindings=bindings)

    cfg = provider.get_postgres_config(resource_name="r1", connector=connector)
    assert cfg is not None
    assert cfg.uri == pg_cfg.uri


def test_provider_bind_from_bindings_resolves_by_connector_name() -> None:
    connector = TableConnector(table_name="t1", schema_name="public", name="c1")
    pg_cfg = PostgresConfig(
        uri="postgresql://localhost:5432/db",
        username="u",
        password="p",
        database="db",
        schema_name="public",
    )

    bindings = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": "c1", "conn_proxy": "pg"}],
    )

    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="pg",
        config=PostgresGeneralizedConnConfig(config=pg_cfg),
    )
    provider.bind_from_bindings(bindings=bindings)

    cfg = provider.get_postgres_config(resource_name="r1", connector=connector)
    assert cfg is not None
    assert cfg.uri == pg_cfg.uri


def test_provider_bind_from_bindings_resolves_by_connector_hash() -> None:
    connector = TableConnector(table_name="t1", schema_name="public", name="c1")
    pg_cfg = PostgresConfig(
        uri="postgresql://localhost:5432/db",
        username="u",
        password="p",
        database="db",
        schema_name="public",
    )

    bindings = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": connector.hash, "conn_proxy": "pg"}],
    )

    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="pg",
        config=PostgresGeneralizedConnConfig(config=pg_cfg),
    )
    provider.bind_from_bindings(bindings=bindings)

    cfg = provider.get_postgres_config(resource_name="r1", connector=connector)
    assert cfg is not None
    assert cfg.uri == pg_cfg.uri


def test_bind_single_config_for_bindings_binds_and_validates() -> None:
    connector = TableConnector(
        table_name="t1",
        schema_name="public",
        resource_name="people",
    )
    pg_cfg = PostgresConfig(
        uri="postgresql://localhost:5432/db",
        username="u",
        password="p",
        database="db",
        schema_name="public",
    )

    bindings = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": connector.hash, "conn_proxy": "pg"}],
    )

    provider = InMemoryConnectionProvider()
    provider.bind_single_config_for_bindings(
        bindings=bindings,
        conn_proxy="pg",
        config=PostgresGeneralizedConnConfig(config=pg_cfg),
    )

    cfg = provider.get_postgres_config(resource_name="r1", connector=connector)
    assert cfg is not None
    assert cfg.uri == pg_cfg.uri

    # Mismatch between helper conn_proxy and manifest conn_proxy should fail fast.
    bindings_mismatch = Bindings(
        connectors=[
            TableConnector(
                table_name="t1",
                schema_name="public",
                name="c_people",
                resource_name="people",
            ),
            TableConnector(
                table_name="t2",
                schema_name="public",
                name="c_products",
                resource_name="products",
            ),
        ],
        connector_connection=[
            {"connector": "c_people", "conn_proxy": "pg"},
            {"connector": "c_products", "conn_proxy": "pg2"},
        ],
    )
    provider_mismatch = InMemoryConnectionProvider()
    with pytest.raises(ValueError, match="Expected all connector_connection mappings"):
        provider_mismatch.bind_single_config_for_bindings(
            bindings=bindings_mismatch,
            conn_proxy="pg",
            config=PostgresGeneralizedConnConfig(config=pg_cfg),
        )


def test_bindings_generated_shape_supports_proxy_resolution() -> None:
    """Match infer_manifest-style bindings: connector names + conn_proxy mapping."""
    connector = TableConnector(table_name="users", schema_name="public")
    bindings = Bindings()
    bindings.add_connector(connector)
    bindings.bind_resource("users", connector)
    bindings.bind_connector_to_conn_proxy(connector, "postgres_source")

    resolved = bindings.get_connectors_for_resource("users")
    assert len(resolved) == 1
    assert resolved[0].name == "users"
    assert bindings.get_conn_proxy_for_connector(resolved[0]) == "postgres_source"


def test_api_connector_conn_proxy_resolution() -> None:
    connector = APIConnector(name="users_api", path="/api/users")
    bindings = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": "users_api", "conn_proxy": "api_source"}],
    )
    assert bindings.get_conn_proxy_for_connector(connector) == "api_source"

    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="api_source",
        config=ApiGeneralizedConnConfig(
            config=RestApiConnConfig(base_url="https://api.example.com")
        ),
    )
    provider.bind_connector_to_conn_proxy(connector=connector, conn_proxy="api_source")
    cfg = provider.get_generalized_conn_config(connector)
    assert isinstance(cfg, ApiGeneralizedConnConfig)
    assert cfg.config.base_url == "https://api.example.com"


def test_register_api_config_from_env_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USER_SERVICE_BASE_URL", "https://users.example.com")
    monkeypatch.setenv("USER_SERVICE_AUTH_TYPE", "bearer")
    monkeypatch.setenv("USER_SERVICE_TOKEN", "secret-token")

    provider = InMemoryConnectionProvider()
    provider.register_api_config_from_env(conn_proxy="user_service")

    cfg = provider.get_generalized_config_by_proxy("user_service")
    assert isinstance(cfg, ApiGeneralizedConnConfig)
    assert cfg.config.base_url == "https://users.example.com"
    assert cfg.config.auth is not None
    assert cfg.config.auth.auth_type == "bearer"
    assert cfg.config.auth.token == "secret-token"


def test_register_api_config_from_env_basic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USER_SERVICE_BASE_URL", "https://users.example.com")
    monkeypatch.setenv("USER_SERVICE_AUTH_TYPE", "basic")
    monkeypatch.setenv("USER_SERVICE_USERNAME", "alice")
    monkeypatch.setenv("USER_SERVICE_PASSWORD", "s3cret")

    provider = InMemoryConnectionProvider()
    provider.register_api_config_from_env(conn_proxy="user_service")

    cfg = provider.get_generalized_config_by_proxy("user_service")
    assert isinstance(cfg, ApiGeneralizedConnConfig)
    assert cfg.config.auth is not None
    assert cfg.config.auth.auth_type == "basic"
    assert cfg.config.auth.username == "alice"
    assert cfg.config.auth.password == "s3cret"


def test_register_api_config_from_env_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORDER_SERVICE_BASE_URL", "https://orders.example.com")
    monkeypatch.setenv("ORDER_SERVICE_AUTH_TYPE", "api_key")
    monkeypatch.setenv("ORDER_SERVICE_TOKEN", "key-123")
    monkeypatch.setenv("ORDER_SERVICE_HEADER_NAME", "X-Api-Key")

    provider = InMemoryConnectionProvider()
    provider.register_api_config_from_env(conn_proxy="order_service")

    cfg = provider.get_generalized_config_by_proxy("order_service")
    assert isinstance(cfg, ApiGeneralizedConnConfig)
    assert cfg.config.auth is not None
    assert cfg.config.auth.auth_type == "api_key"
    assert cfg.config.auth.token == "key-123"
    assert cfg.config.auth.header_name == "X-Api-Key"


def test_register_api_config_from_env_custom_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REST_API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("REST_API_AUTH_TYPE", "bearer")
    monkeypatch.setenv("REST_API_TOKEN", "override-token")

    provider = InMemoryConnectionProvider()
    provider.register_api_config_from_env(
        conn_proxy="user_service",
        env_prefix="REST_API_",
    )

    cfg = provider.get_generalized_config_by_proxy("user_service")
    assert isinstance(cfg, ApiGeneralizedConnConfig)
    assert cfg.config.base_url == "https://api.example.com"
    assert cfg.config.auth is not None
    assert cfg.config.auth.token == "override-token"


def test_register_api_config_from_env_token_without_auth_type_has_no_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("USER_SERVICE_BASE_URL", "https://users.example.com")
    monkeypatch.setenv("USER_SERVICE_TOKEN", "ignored-without-auth-type")

    provider = InMemoryConnectionProvider()
    provider.register_api_config_from_env(conn_proxy="user_service")

    cfg = provider.get_generalized_config_by_proxy("user_service")
    assert isinstance(cfg, ApiGeneralizedConnConfig)
    assert cfg.config.auth is None


def test_register_api_config_from_env_missing_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("USER_SERVICE_BASE_URL", raising=False)

    provider = InMemoryConnectionProvider()
    with pytest.raises(ValueError, match="USER_SERVICE_BASE_URL"):
        provider.register_api_config_from_env(conn_proxy="user_service")


def test_register_all_api_configs_from_env_multi_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("USER_SERVICE_BASE_URL", "https://users.example.com")
    monkeypatch.setenv("USER_SERVICE_AUTH_TYPE", "bearer")
    monkeypatch.setenv("USER_SERVICE_TOKEN", "user-token")
    monkeypatch.setenv("ORDER_SERVICE_BASE_URL", "https://orders.example.com")
    monkeypatch.setenv("ORDER_SERVICE_AUTH_TYPE", "bearer")
    monkeypatch.setenv("ORDER_SERVICE_TOKEN", "order-token")

    users_connector = APIConnector(name="users_api", path="/api/users")
    orders_connector = APIConnector(name="orders_api", path="/api/orders")
    bindings = Bindings(
        connectors=[users_connector, orders_connector],
        connector_connection=[
            {"connector": "users_api", "conn_proxy": "user_service"},
            {"connector": "orders_api", "conn_proxy": "order_service"},
        ],
    )

    provider = InMemoryConnectionProvider()
    provider.register_all_api_configs_from_env(bindings=bindings)

    users_cfg = provider.get_generalized_conn_config(users_connector)
    orders_cfg = provider.get_generalized_conn_config(orders_connector)
    assert isinstance(users_cfg, ApiGeneralizedConnConfig)
    assert isinstance(orders_cfg, ApiGeneralizedConnConfig)
    assert users_cfg.config.base_url == "https://users.example.com"
    assert users_cfg.config.auth is not None
    assert users_cfg.config.auth.token == "user-token"
    assert orders_cfg.config.base_url == "https://orders.example.com"
    assert orders_cfg.config.auth is not None
    assert orders_cfg.config.auth.token == "order-token"


def test_register_all_api_configs_from_env_prefix_map(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("USERS_API_BASE_URL", "https://users.example.com")
    monkeypatch.setenv("USERS_API_AUTH_TYPE", "bearer")
    monkeypatch.setenv("USERS_API_TOKEN", "mapped-token")
    monkeypatch.setenv("ORDER_SERVICE_BASE_URL", "https://orders.example.com")

    users_connector = APIConnector(name="users_api", path="/api/users")
    orders_connector = APIConnector(name="orders_api", path="/api/orders")
    bindings = Bindings(
        connectors=[users_connector, orders_connector],
        connector_connection=[
            {"connector": "users_api", "conn_proxy": "user_service"},
            {"connector": "orders_api", "conn_proxy": "order_service"},
        ],
    )

    provider = InMemoryConnectionProvider()
    provider.register_all_api_configs_from_env(
        bindings=bindings,
        env_prefix_map={"user_service": "USERS_API_"},
    )

    users_cfg = provider.get_generalized_conn_config(users_connector)
    orders_cfg = provider.get_generalized_conn_config(orders_connector)
    assert isinstance(users_cfg, ApiGeneralizedConnConfig)
    assert isinstance(orders_cfg, ApiGeneralizedConnConfig)
    assert users_cfg.config.base_url == "https://users.example.com"
    assert users_cfg.config.auth is not None
    assert users_cfg.config.auth.token == "mapped-token"
    assert orders_cfg.config.base_url == "https://orders.example.com"
    assert orders_cfg.config.auth is None


def test_register_all_api_configs_from_env_skips_non_api_connectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_SOURCE_BASE_URL", "https://api.example.com")

    api_connector = APIConnector(name="users_api", path="/api/users")
    table_connector = TableConnector(
        table_name="users",
        schema_name="public",
        name="users_table",
    )
    bindings = Bindings(
        connectors=[api_connector, table_connector],
        connector_connection=[
            {"connector": "users_api", "conn_proxy": "api_source"},
            {"connector": "users_table", "conn_proxy": "postgres_source"},
        ],
    )

    provider = InMemoryConnectionProvider()
    provider.register_all_api_configs_from_env(bindings=bindings)

    api_cfg = provider.get_generalized_conn_config(api_connector)
    table_cfg = provider.get_generalized_conn_config(table_connector)
    assert isinstance(api_cfg, ApiGeneralizedConnConfig)
    assert api_cfg.config.base_url == "https://api.example.com"
    assert table_cfg is None
    assert provider.get_generalized_config_by_proxy("postgres_source") is None
