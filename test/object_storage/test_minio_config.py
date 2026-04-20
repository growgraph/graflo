"""Tests for MinioConfig.from_docker_env with a temporary .env file."""

from __future__ import annotations

from pathlib import Path

import pytest

from graflo.object_storage import MinioConfig


def test_from_docker_env_parses_minio_env(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "MINIO_HOSTNAME=127.0.0.1",
                "MINIO_API_PORT=19000",
                "MINIO_PROTOCOL=http",
                "MINIO_ROOT_USER=testkey",
                "MINIO_ROOT_PASSWORD=testsecret",
                "MINIO_STAGING_BUCKET=my-bucket",
                "AWS_REGION=us-west-2",
                "",
            ]
        ),
        encoding="utf-8",
    )

    cfg = MinioConfig.from_docker_env(docker_dir=tmp_path)

    assert cfg.endpoint_url == "http://127.0.0.1:19000"
    assert cfg.access_key == "testkey"
    assert cfg.secret_key == "testsecret"
    assert cfg.bucket == "my-bucket"
    assert cfg.region == "us-west-2"


def test_from_docker_env_loader_endpoint_for_tigergraph(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "MINIO_HOSTNAME=127.0.0.1",
                "MINIO_API_PORT=9003",
                "MINIO_PROTOCOL=http",
                "MINIO_ROOT_USER=k",
                "MINIO_ROOT_PASSWORD=s",
                "MINIO_LOADER_ENDPOINT=http://172.17.0.1:9003",
                "",
            ]
        ),
        encoding="utf-8",
    )
    cfg = MinioConfig.from_docker_env(docker_dir=tmp_path)
    assert cfg.endpoint_url == "http://127.0.0.1:9003"
    assert cfg.loader_endpoint_url == "http://172.17.0.1:9003"


def test_from_docker_env_minio_endpoint_override(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "MINIO_ENDPOINT=https://s3.example.com\n"
        "MINIO_ROOT_USER=a\n"
        "MINIO_ROOT_PASSWORD=b\n",
        encoding="utf-8",
    )
    cfg = MinioConfig.from_docker_env(docker_dir=tmp_path)
    assert cfg.endpoint_url == "https://s3.example.com"


def test_from_docker_env_requires_credentials(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("MINIO_ROOT_USER=onlyuser\n", encoding="utf-8")
    with pytest.raises(ValueError, match="MINIO_ROOT"):
        MinioConfig.from_docker_env(docker_dir=tmp_path)
