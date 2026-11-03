from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    ConnectorUpdate,
    FileConnector,
    KafkaConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.connections.onto import Neo4jConfig, PostgresConfig, SparqlEndpointConfig
from graflo.connections.provider import (
    ApiAuth,
    EmptyConnectionProvider,
    InMemoryConnectionProvider,
    KafkaConnConfig,
    RestApiConnConfig,
    S3ConnConfig,
    connector_config,
    proxy_env_prefix,
    resolve_db_secrets_from_env,
    resolve_secrets_from_env,
)

PG = PostgresConfig(
    uri="postgresql://localhost:5432/db",
    username="u",
    password="p",
    database="db",
    schema_name="public",
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


def test_provider_maps_labels_to_configs() -> None:
    api = RestApiConnConfig(base_url="https://api.example.com")
    provider = InMemoryConnectionProvider({"pg": PG, "api": api})

    assert provider.get_config("pg") is PG
    assert provider.get_config("api") is api
    assert provider.get_config("missing") is None
    assert EmptyConnectionProvider().get_config("pg") is None

    s3 = S3ConnConfig(bucket="staging")
    provider.register("s3", s3)
    assert provider.get_config("s3") is s3


def test_connector_config_resolves_through_the_bindings() -> None:
    table = TableConnector(table_name="t1", schema_name="public", name="c1")
    api = APIConnector(name="users_api", path="/api/users")
    rest = RestApiConnConfig(base_url="https://users.example.com")
    bindings = Bindings(
        connectors=[table, api],
        connector_connection=[
            {"connector": "c1", "conn_proxy": "pg"},
            {"connector": "users_api", "conn_proxy": "user_service"},
        ],
    )
    provider = InMemoryConnectionProvider({"pg": PG, "user_service": rest})

    assert connector_config(table, bindings, provider, PostgresConfig) is PG
    assert connector_config(api, bindings, provider, RestApiConnConfig) is rest


def test_connector_config_follows_a_patched_connector() -> None:
    """The provider holds no connector hashes, so a patch after building it is seen."""
    table = TableConnector(table_name="t1", schema_name="public", name="c1")
    bindings = Bindings(
        connectors=[table],
        connector_connection=[{"connector": "c1", "conn_proxy": "pg"}],
    )
    provider = InMemoryConnectionProvider({"pg": PG})

    bindings.apply_connector_update(
        ConnectorUpdate.model_validate({"connector": "c1", "table_name": "t2"})
    )
    patched = bindings.connectors[0]

    assert patched.hash != table.hash
    assert connector_config(patched, bindings, provider, PostgresConfig) is PG


def test_connector_config_errors_name_connector_and_proxy() -> None:
    table = TableConnector(table_name="t1", schema_name="public", name="c1")
    unbound = TableConnector(table_name="t2", schema_name="public", name="c2")
    bindings = Bindings(
        connectors=[table, unbound],
        connector_connection=[{"connector": "c1", "conn_proxy": "pg"}],
    )

    with pytest.raises(ValueError, match="'c2' has no conn_proxy"):
        connector_config(
            unbound, bindings, InMemoryConnectionProvider(), PostgresConfig
        )
    with pytest.raises(ValueError, match="registered for conn_proxy 'pg'"):
        connector_config(table, bindings, InMemoryConnectionProvider(), PostgresConfig)
    wrong = InMemoryConnectionProvider({"pg": RestApiConnConfig(base_url="http://x")})
    with pytest.raises(ValueError, match="holds a RestApiConnConfig; a PostgresConfig"):
        connector_config(table, bindings, wrong, PostgresConfig)


def test_conn_proxies_groups_connectors_by_label() -> None:
    a = TableConnector(table_name="a", schema_name="public", name="a")
    b = TableConnector(table_name="b", schema_name="public", name="b")
    api = APIConnector(name="api", path="/x")
    files = FileConnector(regex=r".*\.csv", name="files")
    bindings = Bindings(
        connectors=[a, b, api, files],
        connector_connection=[
            {"connector": "b", "conn_proxy": "pg"},
            {"connector": "a", "conn_proxy": "pg"},
            {"connector": "api", "conn_proxy": "api"},
        ],
    )

    proxies = bindings.conn_proxies()

    assert list(proxies) == ["api", "pg"]
    assert [c.name for c in proxies["pg"]] == ["a", "b"]


def _mixed_bindings() -> Bindings:
    return Bindings(
        connectors=[
            TableConnector(table_name="users", schema_name="public", name="users"),
            APIConnector(name="orders_api", path="/api/orders"),
            KafkaConnector(name="events", topics=["events"], group_id="g"),
            SparqlConnector(
                name="labels",
                rdf_class="http://example.org/Label",
                endpoint_url="http://localhost:3030/ds/sparql",
            ),
        ],
        connector_connection=[
            {"connector": "users", "conn_proxy": "shop_db"},
            {"connector": "orders_api", "conn_proxy": "billing-api"},
            {"connector": "events", "conn_proxy": "kafka_local"},
            {"connector": "labels", "conn_proxy": "kb"},
        ],
    )


def test_from_env_loads_every_proxy_by_its_connector_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SHOP_DB_URI", "postgresql://db.example.com:5432")
    monkeypatch.setenv("SHOP_DB_USERNAME", "shop")
    monkeypatch.setenv("SHOP_DB_PASSWORD", "s3cret")
    monkeypatch.setenv("SHOP_DB_DATABASE", "shop")
    monkeypatch.setenv("BILLING_API_BASE_URL", "https://billing.example.com")
    monkeypatch.setenv("BILLING_API_TOKEN", "t0ken")
    monkeypatch.setenv("KAFKA_LOCAL_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KB_USERNAME", "reader")

    provider = InMemoryConnectionProvider.from_env(_mixed_bindings())

    db = provider.get_config("shop_db")
    assert isinstance(db, PostgresConfig)
    assert (db.username, db.password, db.database) == ("shop", "s3cret", "shop")
    assert db.uri is not None and db.uri.startswith("postgresql://db.example.com")
    api = provider.get_config("billing-api")
    assert isinstance(api, RestApiConnConfig)
    assert api.base_url == "https://billing.example.com"
    assert api.auth is not None and api.auth.token == "t0ken"
    kafka = provider.get_config("kafka_local")
    assert isinstance(kafka, KafkaConnConfig)
    assert kafka.bootstrap_servers == "localhost:9092"
    sparql = provider.get_config("kb")
    assert isinstance(sparql, SparqlEndpointConfig)
    assert sparql.username == "reader"


def test_from_env_reports_every_missing_proxy_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "SHOP_DB_URI",
        "BILLING_API_BASE_URL",
        "KAFKA_LOCAL_BOOTSTRAP_SERVERS",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError) as excinfo:
        InMemoryConnectionProvider.from_env(_mixed_bindings())

    message = str(excinfo.value)
    assert "SHOP_DB_URI" in message
    assert "BILLING_API_BASE_URL" in message
    assert "KAFKA_LOCAL_BOOTSTRAP_SERVERS" in message


def test_from_env_prefix_map_overrides_the_derived_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BILLING_PROD_BASE_URL", "https://billing.example.com")
    monkeypatch.delenv("BILLING_API_BASE_URL", raising=False)
    bindings = Bindings(
        connectors=[APIConnector(name="orders_api", path="/api/orders")],
        connector_connection=[{"connector": "orders_api", "conn_proxy": "billing-api"}],
    )

    provider = InMemoryConnectionProvider.from_env(
        bindings, env_prefix_map={"billing-api": "BILLING_PROD_"}
    )

    api = provider.get_config("billing-api")
    assert isinstance(api, RestApiConnConfig)
    assert api.base_url == "https://billing.example.com"
    # No credential in the environment: the API is open, not bearer with no token.
    assert api.auth is None


def test_from_env_refuses_a_label_shared_by_different_kinds() -> None:
    bindings = Bindings(
        connectors=[
            TableConnector(table_name="t", schema_name="public", name="t"),
            APIConnector(name="api", path="/x"),
        ],
        connector_connection=[
            {"connector": "t", "conn_proxy": "shared"},
            {"connector": "api", "conn_proxy": "shared"},
        ],
    )

    with pytest.raises(ValueError, match="'shared' is used by connectors needing"):
        InMemoryConnectionProvider.from_env(bindings)


def test_from_env_skips_labels_named_only_by_file_connectors() -> None:
    bindings = Bindings(
        connectors=[FileConnector(regex=r".*\.csv", name="files")],
        connector_connection=[{"connector": "files", "conn_proxy": "lake"}],
    )

    assert InMemoryConnectionProvider.from_env(bindings).get_config("lake") is None


@pytest.mark.parametrize(
    ("auth_type", "env", "expected"),
    [
        ("bearer", {"TOKEN": "secret-token"}, {"token": "secret-token"}),
        (
            "basic",
            {"USERNAME": "alice", "PASSWORD": "s3cret"},
            {"username": "alice", "password": "s3cret"},
        ),
        (
            "api_key",
            {"TOKEN": "key-123", "HEADER_NAME": "X-Api-Key"},
            {"token": "key-123", "header_name": "X-Api-Key"},
        ),
    ],
)
def test_rest_api_from_env_reads_each_auth_type(
    monkeypatch: pytest.MonkeyPatch,
    auth_type: str,
    env: dict[str, str],
    expected: dict[str, str],
) -> None:
    monkeypatch.setenv("USER_SERVICE_BASE_URL", "https://users.example.com")
    monkeypatch.setenv("USER_SERVICE_AUTH_TYPE", auth_type)
    for key, value in env.items():
        monkeypatch.setenv(f"USER_SERVICE_{key}", value)

    cfg = RestApiConnConfig.from_env(env_prefix="USER_SERVICE_")

    assert cfg.auth is not None
    assert cfg.auth.auth_type == auth_type
    for key, value in expected.items():
        assert getattr(cfg.auth, key) == value


def test_db_config_from_proxy_env_reads_no_backend_segment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SHOP_DB_URI", "postgresql://localhost:5432")
    monkeypatch.setenv("SHOP_DB_SCHEMA_NAME", "sales")
    monkeypatch.setenv("SHOP_DB_POSTGRES_URI", "postgresql://wrong:5432")

    cfg = PostgresConfig.from_proxy_env("shop_db")

    assert cfg.uri is not None and cfg.uri.startswith("postgresql://localhost")
    assert cfg.schema_name == "sales"
    assert isinstance(cfg, PostgresConfig)
    explicit = PostgresConfig.from_proxy_env("shop_db", env_prefix="SHOP_DB_POSTGRES_")
    assert explicit.uri is not None and "wrong" in explicit.uri


def test_s3_conn_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STAGING_BUCKET", "bulk")
    monkeypatch.setenv("STAGING_AWS_ACCESS_KEY_ID", "key")
    monkeypatch.delenv("STAGING_REGION", raising=False)

    cfg = S3ConnConfig.from_env("STAGING_")

    assert (cfg.bucket, cfg.aws_access_key_id, cfg.region) == ("bulk", "key", None)


def test_proxy_env_prefix_uppercases_and_normalises_dashes() -> None:
    assert proxy_env_prefix("sensor-feed") == "SENSOR_FEED_"


def test_resolve_secrets_from_env_fills_api_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SENSOR_FEED_TOKEN", "t0ken")
    stored = RestApiConnConfig(
        base_url="http://localhost:8811", auth=ApiAuth(auth_type="bearer")
    )

    resolved = resolve_secrets_from_env(stored, "sensor_feed")

    assert isinstance(resolved, RestApiConnConfig)
    assert resolved.auth is not None
    assert resolved.auth.token == "t0ken"
    assert resolved.auth.auth_type == "bearer"
    assert stored.auth is not None and stored.auth.token is None


def test_resolve_secrets_from_env_creates_auth_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_SOURCE_TOKEN", "t0ken")
    stored = RestApiConnConfig(base_url="http://x")

    resolved = resolve_secrets_from_env(stored, "api_source")

    assert isinstance(resolved, RestApiConnConfig)
    assert resolved.auth is not None
    assert resolved.auth.token == "t0ken"


def test_resolve_secrets_from_env_fills_postgres_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POSTGRES_USERNAME", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.setenv("MAINTENANCE_DB_USERNAME", "maintenance")
    monkeypatch.setenv("MAINTENANCE_DB_PASSWORD", "secret")
    stored = PostgresConfig(uri="postgresql://localhost:55432", database="mx")

    resolved = resolve_secrets_from_env(stored, "maintenance_db")

    assert isinstance(resolved, PostgresConfig)
    assert resolved.username == "maintenance"
    assert resolved.password == "secret"
    assert resolved.database == "mx"


def test_resolve_secrets_from_env_keeps_stored_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_SOURCE_TOKEN", "from-env")
    stored = RestApiConnConfig(base_url="http://x", auth=ApiAuth(token="stored"))

    assert resolve_secrets_from_env(stored, "api_source") is stored


def test_resolve_secrets_from_env_honours_explicit_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTHER_TOKEN", "t0ken")
    stored = RestApiConnConfig(base_url="http://x")

    resolved = resolve_secrets_from_env(stored, "api_source", env_prefix="OTHER_")

    assert isinstance(resolved, RestApiConnConfig)
    assert resolved.auth is not None
    assert resolved.auth.token == "t0ken"


def test_a_graph_target_takes_its_missing_credentials_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRAPH_USERNAME", "neo4j")
    monkeypatch.setenv("GRAPH_PASSWORD", "from-env")
    stored = Neo4jConfig(uri="bolt://localhost:7687")

    filled = resolve_db_secrets_from_env(stored, "graph")

    assert (filled.username, filled.password) == ("neo4j", "from-env")
    assert isinstance(filled, Neo4jConfig) and stored.password is None
    kept = Neo4jConfig(uri="bolt://localhost:7687", password="stored")
    assert resolve_db_secrets_from_env(kept, "graph").password == "stored"
