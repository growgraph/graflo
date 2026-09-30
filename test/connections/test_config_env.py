"""What connection configs take from the environment, and the ports they fill in."""

from __future__ import annotations

import pytest

from graflo.connections.onto import (
    DBConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    TigergraphConfig,
)
from graflo.connections.sources import RestApiConnConfig


class TestSchemaNameFromEnvironment:
    @pytest.mark.parametrize(
        "config_class,variable",
        [
            (TigergraphConfig, "TIGERGRAPH_SCHEMA_NAME"),
            (NebulaConfig, "NEBULA_SCHEMA_NAME"),
            (PostgresConfig, "POSTGRES_SCHEMA_NAME"),
        ],
    )
    def test_prefixed_variable_is_read(
        self,
        monkeypatch: pytest.MonkeyPatch,
        config_class: type[DBConfig],
        variable: str,
    ) -> None:
        monkeypatch.setenv(variable, "sales")
        assert config_class().schema_name == "sales"

    @pytest.mark.parametrize("variable", ["SCHEMA", "SCHEMA_NAME"])
    def test_unprefixed_variable_is_ignored(
        self, monkeypatch: pytest.MonkeyPatch, variable: str
    ) -> None:
        monkeypatch.setenv(variable, "someone-elses")
        assert PostgresConfig().schema_name is None

    def test_profile_and_suffix_qualify_the_variable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("POSTGRES_DEV_SCHEMA_NAME", "by_profile")
        monkeypatch.setenv("POSTGRES_SCHEMA_NAME_QA", "by_suffix")
        assert PostgresConfig.from_env(profile="DEV").schema_name == "by_profile"
        assert PostgresConfig.from_env(suffix="QA").schema_name == "by_suffix"


class TestSchemaKey:
    @pytest.mark.parametrize("key", ["schema", "schema_name"])
    def test_both_spellings_load_from_a_config_mapping(self, key: str) -> None:
        config = DBConfig.from_dict({"db_type": "postgres", key: "sales"})
        assert config.schema_name == "sales"

    def test_schema_keyword_is_accepted_in_code(self) -> None:
        assert TigergraphConfig(schema="g").schema_name == "g"

    def test_schema_key_wins_over_the_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("POSTGRES_SCHEMA_NAME", "from_env")
        config = DBConfig.from_dict({"db_type": "postgres", "schema": "from_code"})
        assert config.schema_name == "from_code"


class TestNeo4jDefaultPort:
    @pytest.mark.parametrize(
        "uri,completed",
        [
            ("bolt://localhost", "bolt://localhost:7687"),
            ("bolt+s://db.example.com", "bolt+s://db.example.com:7687"),
            ("neo4j://localhost", "neo4j://localhost:7687"),
            ("neo4j+ssc://localhost", "neo4j+ssc://localhost:7687"),
            ("http://localhost", "http://localhost:7474"),
            ("https://localhost", "https://localhost:7474"),
        ],
    )
    def test_port_follows_the_scheme(self, uri: str, completed: str) -> None:
        assert Neo4jConfig(uri=uri).uri == completed

    def test_written_port_is_kept(self) -> None:
        assert Neo4jConfig(uri="bolt://localhost:7688").uri == "bolt://localhost:7688"

    def test_bolt_port_completes_a_bolt_uri(self) -> None:
        config = Neo4jConfig(uri="bolt://localhost", bolt_port=7999)
        assert config.uri == "bolt://localhost:7999"
        assert config.bolt_port == 7999


class TestRestApiConfigFromEnvironment:
    def test_no_credential_means_no_auth(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OPEN_BASE_URL", "https://api.example.com")
        assert RestApiConnConfig.from_env("OPEN_").auth is None

    def test_auth_type_alone_is_not_a_credential(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OPEN_BASE_URL", "https://api.example.com")
        monkeypatch.setenv("OPEN_AUTH_TYPE", "basic")
        assert RestApiConnConfig.from_env("OPEN_").auth is None

    def test_token_builds_bearer_auth(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SVC_BASE_URL", "https://api.example.com")
        monkeypatch.setenv("SVC_TOKEN", "t0k3n")
        auth = RestApiConnConfig.from_env("SVC_").auth
        assert auth is not None
        assert (auth.auth_type, auth.token) == ("bearer", "t0k3n")

    def test_username_builds_basic_auth(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SVC_BASE_URL", "https://api.example.com")
        monkeypatch.setenv("SVC_AUTH_TYPE", "basic")
        monkeypatch.setenv("SVC_USERNAME", "reader")
        monkeypatch.setenv("SVC_PASSWORD", "secret")
        auth = RestApiConnConfig.from_env("SVC_").auth
        assert auth is not None
        assert (auth.auth_type, auth.username, auth.password) == (
            "basic",
            "reader",
            "secret",
        )


class TestBulkLoadJobOptions:
    def test_run_only_is_refused(self) -> None:
        from graflo.connections.onto import TigergraphBulkLoadJobOptions

        with pytest.raises(ValueError, match="session id"):
            TigergraphBulkLoadJobOptions.model_validate({"run_mode": "run_only"})

    def test_create_and_run_still_loads(self) -> None:
        from graflo.connections.onto import TigergraphBulkLoadJobOptions

        options = TigergraphBulkLoadJobOptions.model_validate(
            {"run_mode": "create_and_run"}
        )
        assert options.run_mode == "create_and_run"
