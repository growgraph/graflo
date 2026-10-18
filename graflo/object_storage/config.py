"""S3-compatible endpoint configuration (SeaweedFS, AWS S3, MinIO, etc.).

Used for TigerGraph bulk CSV staging and other boto3 S3 clients. Not a graph
database target. :meth:`MinioConfig.from_docker_env` reads ``docker/seaweedfs``,
the S3 service this repository ships.

``S3EndpointConfig`` is an alias for :class:`MinioConfig` (name reflects any S3 API).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from graflo.connections.provider import S3GeneralizedConnConfig


def parse_dotenv_file(env_file: Path) -> dict[str, str]:
    """Parse a simple ``KEY=value`` .env file (no shell expansion)."""
    env_vars: dict[str, str] = {}
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip().strip('"').strip("'")
    return env_vars


class MinioConfig(BaseModel):
    """Credentials and endpoint for any S3-compatible API (boto3)."""

    model_config = ConfigDict(extra="forbid")

    endpoint_url: str = Field(
        description="Base URL for the S3 API, e.g. http://127.0.0.1:8333",
    )
    access_key: str = Field(description="S3 access key id.")
    secret_key: str = Field(description="S3 secret access key.")
    bucket: str = Field(
        default="graflo-staging",
        description="Default bucket for staged bulk CSV objects.",
    )
    region: str = Field(
        default="us-east-1",
        description="AWS region string passed to boto3 (us-east-1 for a local S3 service).",
    )
    loader_endpoint_url: str | None = Field(
        default=None,
        description=(
            "Optional S3 API URL for TigerGraph LOADING JOB (CREATE DATA_SOURCE). "
            "Set when TigerGraph runs in Docker and the S3 service is on the host "
            "(e.g. http://172.17.0.1:8333). Boto3 uses endpoint_url above."
        ),
    )

    def to_s3_generalized_conn_config(self) -> S3GeneralizedConnConfig:
        """Map to runtime provider config for :class:`~graflo.connections.provider.S3GeneralizedConnConfig`."""
        from graflo.connections.provider import S3GeneralizedConnConfig

        return S3GeneralizedConnConfig(
            bucket=self.bucket,
            region=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            loader_endpoint_url=self.loader_endpoint_url,
        )

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> MinioConfig:
        """Load the S3 service settings of ``docker/seaweedfs``.

        Reads ``<docker_dir>/.env`` when it exists; every setting has a default
        matching the bundled compose file, whose S3 API accepts any key pair, so
        the file is optional.

        Settings: ``S3_ENDPOINT`` (or ``S3_PROTOCOL``, ``S3_HOSTNAME``,
        ``S3_API_PORT``), ``S3_ACCESS_KEY``, ``S3_SECRET_KEY``, ``S3_BUCKET``,
        ``AWS_REGION`` and ``S3_LOADER_ENDPOINT`` (the address TigerGraph uses).
        """
        if docker_dir is None:
            docker_dir = (
                Path(__file__).resolve().parent.parent.parent / "docker" / "seaweedfs"
            )
        else:
            docker_dir = Path(docker_dir)

        env_file = docker_dir / ".env"
        env_vars = parse_dotenv_file(env_file) if env_file.exists() else {}
        data: dict[str, Any] = {}

        if raw_endpoint := env_vars.get("S3_ENDPOINT"):
            data["endpoint_url"] = raw_endpoint
        else:
            host = env_vars.get("S3_HOSTNAME", "127.0.0.1")
            port = env_vars.get("S3_API_PORT", "8333")
            protocol = env_vars.get("S3_PROTOCOL", "http")
            data["endpoint_url"] = f"{protocol}://{host}:{port}"

        data["access_key"] = env_vars.get("S3_ACCESS_KEY", "graflo")
        data["secret_key"] = env_vars.get("S3_SECRET_KEY", "graflo-secret")

        if bucket := env_vars.get("S3_BUCKET"):
            data["bucket"] = bucket
        if region := env_vars.get("AWS_REGION"):
            data["region"] = region
        if loader_ep := env_vars.get("S3_LOADER_ENDPOINT"):
            data["loader_endpoint_url"] = loader_ep

        return cls(**data)


S3EndpointConfig = MinioConfig
"""Alias: any S3-compatible endpoint, not only MinIO."""
