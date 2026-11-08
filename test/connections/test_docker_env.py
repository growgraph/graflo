"""The bundled docker services' settings: one file per service, read by compose and graflo."""

from __future__ import annotations

from pathlib import Path

import pytest

from graflo.connections.env import (
    DOCKER_ENV_FILE,
    DOCKER_ROOT,
    docker_env,
    parse_env_file,
)
from graflo.connections.onto import (
    ArangoConfig,
    FalkordbConfig,
    MemgraphConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    SparqlEndpointConfig,
    TigergraphConfig,
)
from graflo.object_storage.config import S3EndpointConfig


def test_values_are_unquoted_and_earlier_keys_expand(tmp_path: Path) -> None:
    path = tmp_path / DOCKER_ENV_FILE
    path.write_text(
        '# a comment\n\nSPEC=graflo\nCONTAINER_NAME="${SPEC}.neo4j"\n'
        "PASSWORD='a=b'\nLATER=${UNSET}\n",
        encoding="utf-8",
    )

    assert parse_env_file(path) == {
        "SPEC": "graflo",
        "CONTAINER_NAME": "graflo.neo4j",
        "PASSWORD": "a=b",
        "LATER": "${UNSET}",
    }


def test_a_missing_settings_file_raises_unless_optional(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=DOCKER_ENV_FILE):
        docker_env("neo4j", tmp_path)
    assert docker_env("seaweedfs", tmp_path, required=False) == {}


def test_every_compose_service_has_a_settings_file() -> None:
    services = sorted(p.parent.name for p in DOCKER_ROOT.glob("*/docker-compose.yml"))

    assert services
    assert [
        s for s in services if not (DOCKER_ROOT / s / DOCKER_ENV_FILE).exists()
    ] == []


@pytest.mark.parametrize(
    "config_cls",
    [
        ArangoConfig,
        Neo4jConfig,
        TigergraphConfig,
        FalkordbConfig,
        MemgraphConfig,
        NebulaConfig,
        PostgresConfig,
        SparqlEndpointConfig,
    ],
)
def test_each_config_loads_from_its_bundled_settings(config_cls) -> None:
    conf = config_cls.from_docker_env()

    assert conf.uri


def test_the_neo4j_config_takes_the_containers_bolt_port_and_password() -> None:
    env = docker_env("neo4j")
    conf = Neo4jConfig.from_docker_env()

    assert conf.uri is not None and conf.uri.endswith(f":{env['NEO4J_BOLT_PORT']}")
    assert conf.password == env["NEO4J_AUTH"].split("/", 1)[1]


def test_the_s3_config_reads_the_seaweedfs_settings() -> None:
    env = docker_env("seaweedfs")

    assert S3EndpointConfig.from_docker_env().endpoint_url.endswith(
        f":{env['S3_API_PORT']}"
    )
