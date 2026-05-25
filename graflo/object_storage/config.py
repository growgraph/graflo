"""S3-compatible endpoint configuration (MinIO, AWS S3, etc.).

Used for TigerGraph bulk CSV staging and other boto3 S3 clients. Not a graph
database target. Load from ``docker/minio/.env`` via :meth:`MinioConfig.from_docker_env`.

``S3EndpointConfig`` is an alias for :class:`MinioConfig` (name reflects any S3 API).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from graflo.hq.connection_provider import S3GeneralizedConnConfig


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
    """Credentials and endpoint for MinIO or any S3-compatible API (boto3)."""

    model_config = ConfigDict(extra="forbid")

    endpoint_url: str = Field(
        description="Base URL for the S3 API, e.g. http://127.0.0.1:9000",
    )
    access_key: str = Field(
        description="S3 access key id (MinIO root user or IAM key)."
    )
    secret_key: str = Field(description="S3 secret access key.")
    bucket: str = Field(
        default="graflo-staging",
        description="Default bucket for staged bulk CSV objects.",
    )
    region: str = Field(
        default="us-east-1",
        description="AWS region string passed to boto3 (use us-east-1 for MinIO).",
    )
    loader_endpoint_url: str | None = Field(
        default=None,
        description=(
            "Optional S3 API URL for TigerGraph LOADING JOB (CREATE DATA_SOURCE). "
            "Set when TigerGraph runs in Docker and MinIO is on the host "
            "(e.g. http://172.17.0.1:9003). Boto3 uses endpoint_url above."
        ),
    )

    def to_s3_generalized_conn_config(self) -> "S3GeneralizedConnConfig":
        """Map to runtime provider config for :class:`~graflo.hq.connection_provider.S3GeneralizedConnConfig`."""
        from graflo.hq.connection_provider import S3GeneralizedConnConfig

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
        """Load from ``docker/minio/.env`` (same layout as other ``docker/*`` stacks)."""
        if docker_dir is None:
            docker_dir = (
                Path(__file__).resolve().parent.parent.parent / "docker" / "minio"
            )
        else:
            docker_dir = Path(docker_dir)

        env_file = docker_dir / ".env"
        if not env_file.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        env_vars = parse_dotenv_file(env_file)
        data: dict[str, Any] = {}

        if raw_endpoint := env_vars.get("MINIO_ENDPOINT"):
            data["endpoint_url"] = raw_endpoint
        else:
            host = env_vars.get("MINIO_HOSTNAME", "127.0.0.1")
            port = env_vars.get("MINIO_API_PORT", "9000")
            protocol = env_vars.get("MINIO_PROTOCOL", "http")
            data["endpoint_url"] = f"{protocol}://{host}:{port}"

        access = env_vars.get("MINIO_ACCESS_KEY") or env_vars.get("MINIO_ROOT_USER")
        secret = env_vars.get("MINIO_SECRET_KEY") or env_vars.get("MINIO_ROOT_PASSWORD")
        if not access or not secret:
            raise ValueError(
                "MinIO docker .env must set MINIO_ROOT_USER/MINIO_ROOT_PASSWORD "
                "or MINIO_ACCESS_KEY/MINIO_SECRET_KEY"
            )
        data["access_key"] = access
        data["secret_key"] = secret

        if bucket := env_vars.get("MINIO_STAGING_BUCKET") or env_vars.get(
            "BULK_S3_BUCKET"
        ):
            data["bucket"] = bucket
        if region := env_vars.get("AWS_REGION"):
            data["region"] = region

        if loader_ep := env_vars.get("MINIO_LOADER_ENDPOINT") or env_vars.get(
            "MINIO_TIGERGRAPH_ENDPOINT"
        ):
            data["loader_endpoint_url"] = loader_ep.strip().strip('"').strip("'")

        return cls(**data)


S3EndpointConfig = MinioConfig
"""Alias: any S3-compatible endpoint, not only MinIO."""
