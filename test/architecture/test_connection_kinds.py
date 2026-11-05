"""Every kind of connection parses back to its class and says what it is used as."""

from __future__ import annotations

import pytest

from graflo.connections.kinds import (
    CONNECTION_ADAPTER,
    ConnectionKind,
    Role,
    connection_kind,
    parse_connection,
    roles,
)
from graflo.connections.onto import (
    GraphDBConfig,
    Neo4jConfig,
    PostgresConfig,
    SparqlEndpointConfig,
)
from graflo.connections.provider import resolve_secrets_from_env
from graflo.connections.sources import (
    FilesConnConfig,
    KafkaConnConfig,
    RestApiConnConfig,
)
from graflo.onto import DBType


@pytest.mark.parametrize(
    ("data", "cls", "kind"),
    [
        (
            {"kind": "database", "db_type": "postgres", "uri": "postgresql://h/db"},
            PostgresConfig,
            ConnectionKind.DATABASE,
        ),
        (
            {"kind": "database", "db_type": "graphdb", "uri": "http://h:7200"},
            GraphDBConfig,
            ConnectionKind.DATABASE,
        ),
        (
            {"kind": "api", "base_url": "http://h"},
            RestApiConnConfig,
            ConnectionKind.API,
        ),
        (
            {"kind": "stream", "bootstrap_servers": "k:9092"},
            KafkaConnConfig,
            ConnectionKind.STREAM,
        ),
        ({"kind": "files", "root": "/data"}, FilesConnConfig, ConnectionKind.FILES),
    ],
)
def test_a_stored_connection_parses_back_to_its_class(data, cls, kind) -> None:
    conn = parse_connection(data)

    assert type(conn) is cls
    assert connection_kind(conn) == kind
    dumped = conn.model_dump(mode="json", exclude_none=True)
    assert type(parse_connection(dumped)) is cls
    assert type(CONNECTION_ADAPTER.validate_python(dumped)) is cls


def test_a_connection_without_a_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="needs a 'kind'"):
        parse_connection({"db_type": "postgres", "uri": "postgresql://h/db"})


def test_an_unknown_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown connection kind"):
        parse_connection({"kind": "ftp", "root": "/x"})


def test_a_database_flavour_without_a_config_is_refused() -> None:
    with pytest.raises(ValueError, match="no connection config serves"):
        parse_connection({"kind": "database", "db_type": "mysql"})


def test_a_config_built_from_the_environment_keeps_its_flavour() -> None:
    """``from_proxy_env`` builds a subclass; its flavour is still PostgreSQL."""
    assert PostgresConfig.from_proxy_env("shop_db").connection_type == DBType.POSTGRES
    assert GraphDBConfig(uri="http://h:7200").connection_type == DBType.GRAPHDB


@pytest.mark.parametrize(
    ("conn", "expected"),
    [
        (PostgresConfig(uri="postgresql://h/db"), {Role.SOURCE, Role.TARGET}),
        (SparqlEndpointConfig(uri="http://h:3030"), {Role.SOURCE}),
        (GraphDBConfig(uri="http://h:7200"), {Role.SOURCE}),
        (Neo4jConfig(uri="bolt://h:7687"), {Role.TARGET}),
        (RestApiConnConfig(base_url="http://h"), {Role.SOURCE}),
        (KafkaConnConfig(bootstrap_servers="k:9092"), {Role.SOURCE}),
        (FilesConnConfig(root="s3://bucket/raw"), {Role.SOURCE}),
    ],
)
def test_roles_follow_from_kind_and_flavour(conn, expected) -> None:
    assert roles(conn) == expected


class TestFilesRoot:
    def test_an_s3_root_names_its_bucket_and_prefix(self) -> None:
        files = FilesConnConfig(root="s3://landing/raw/2026/")
        assert (files.bucket, files.prefix) == ("landing", "raw/2026")
        assert files.s3_config().bucket == "landing"

    def test_a_local_root_must_be_absolute(self) -> None:
        with pytest.raises(ValueError, match="absolute path or an s3:// URL"):
            FilesConnConfig(root="data/raw")

    def test_s3_settings_on_a_local_root_are_refused(self) -> None:
        with pytest.raises(ValueError, match="apply to an s3:// root only"):
            FilesConnConfig(root="/data", endpoint_url="http://minio:9000")

    def test_an_s3_root_needs_a_bucket(self) -> None:
        with pytest.raises(ValueError, match="names no bucket"):
            FilesConnConfig(root="s3://")


class TestSecretsFromTheEnvironment:
    def test_kafka_takes_its_sasl_credentials(self, monkeypatch) -> None:
        monkeypatch.setenv("EVENTS_SASL_USERNAME", "svc")
        monkeypatch.setenv("EVENTS_SASL_PASSWORD", "pw")
        conn = resolve_secrets_from_env(
            KafkaConnConfig(bootstrap_servers="k:9092"), "events"
        )
        assert (conn.sasl_username, conn.sasl_password) == ("svc", "pw")

    def test_s3_files_take_their_keys(self, monkeypatch) -> None:
        monkeypatch.setenv("LAKE_ACCESS_KEY_ID", "key")
        monkeypatch.setenv("LAKE_SECRET_ACCESS_KEY", "secret")
        conn = resolve_secrets_from_env(FilesConnConfig(root="s3://b"), "lake")
        assert (conn.access_key_id, conn.secret_access_key) == ("key", "secret")

    def test_a_stored_value_is_never_overridden(self, monkeypatch) -> None:
        monkeypatch.setenv("LAKE_ACCESS_KEY_ID", "env-key")
        conn = resolve_secrets_from_env(
            FilesConnConfig(root="s3://b", access_key_id="stored"), "lake"
        )
        assert conn.access_key_id == "stored"

    def test_a_graph_target_takes_its_credentials(self, monkeypatch) -> None:
        monkeypatch.setenv("GRAPH_PASSWORD", "pw")
        conn = resolve_secrets_from_env(Neo4jConfig(uri="bolt://h:7687"), "graph")
        assert conn.password == "pw"
