from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import Bindings, TableConnector
from graflo.db import PostgresConfig
from graflo.hq.connection_provider import (
    InMemoryConnectionProvider,
    PostgresGeneralizedConnConfig,
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
