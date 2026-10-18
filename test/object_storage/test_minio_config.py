"""Tests for MinioConfig.from_docker_env with a temporary .env file."""

from __future__ import annotations

from pathlib import Path

from graflo.object_storage import MinioConfig


def test_from_docker_env_parses_s3_env(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "S3_HOSTNAME=127.0.0.1\nS3_API_PORT=18333\nS3_PROTOCOL=http\n"
        "S3_ACCESS_KEY=testkey\nS3_SECRET_KEY=testsecret\nS3_BUCKET=my-bucket\n"
        "AWS_REGION=us-west-2\n",
        encoding="utf-8",
    )

    cfg = MinioConfig.from_docker_env(docker_dir=tmp_path)

    assert cfg.endpoint_url == "http://127.0.0.1:18333"
    assert cfg.access_key == "testkey"
    assert cfg.secret_key == "testsecret"
    assert cfg.bucket == "my-bucket"
    assert cfg.region == "us-west-2"


def test_from_docker_env_loader_endpoint_for_tigergraph(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "S3_API_PORT=8333\nS3_LOADER_ENDPOINT=http://172.17.0.1:8333\n",
        encoding="utf-8",
    )
    cfg = MinioConfig.from_docker_env(docker_dir=tmp_path)
    assert cfg.endpoint_url == "http://127.0.0.1:8333"
    assert cfg.loader_endpoint_url == "http://172.17.0.1:8333"


def test_from_docker_env_endpoint_override(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "S3_ENDPOINT=https://s3.example.com\n", encoding="utf-8"
    )
    cfg = MinioConfig.from_docker_env(docker_dir=tmp_path)
    assert cfg.endpoint_url == "https://s3.example.com"


def test_from_docker_env_defaults_without_env_file(tmp_path: Path) -> None:
    cfg = MinioConfig.from_docker_env(docker_dir=tmp_path)
    assert cfg.endpoint_url == "http://127.0.0.1:8333"
    assert cfg.access_key and cfg.secret_key
    assert cfg.bucket == "graflo-staging"
    assert cfg.loader_endpoint_url is None
