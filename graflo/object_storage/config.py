"""S3-compatible endpoint configuration (SeaweedFS, AWS S3, MinIO, etc.).

Used by boto3 S3 clients. Not a graph database target.
:meth:`S3EndpointConfig.from_docker_env` reads ``docker/seaweedfs``, the S3
service this repository ships.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from graflo.connections.sources import FilesConnConfig


class S3EndpointConfig(BaseModel):
    """Credentials and endpoint for any S3-compatible API (boto3)."""

    model_config = ConfigDict(extra="forbid")

    endpoint_url: str = Field(
        description="Base URL for the S3 API, e.g. http://127.0.0.1:8333",
    )
    access_key: str = Field(description="S3 access key id.")
    secret_key: str = Field(description="S3 secret access key.")
    bucket: str = Field(
        default="graflo-staging",
        description="Default bucket.",
    )
    region: str = Field(
        default="us-east-1",
        description="AWS region string passed to boto3 (us-east-1 for a local S3 service).",
    )

    def to_files_conn_config(self) -> FilesConnConfig:
        """The files connection of :attr:`bucket`, as a connection provider stores it."""
        from graflo.connections.sources import FilesConnConfig

        return FilesConnConfig(
            root=f"s3://{self.bucket}",
            region=self.region,
            endpoint_url=self.endpoint_url,
            access_key_id=self.access_key,
            secret_access_key=self.secret_key,
        )

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> S3EndpointConfig:
        """Load the S3 service settings of ``docker/seaweedfs``.

        Reads ``<docker_dir>/.env.example`` when it exists; every setting has a default
        matching the bundled compose file, whose S3 API accepts any key pair, so
        the file is optional.

        Settings: ``S3_ENDPOINT`` (or ``S3_PROTOCOL``, ``S3_HOSTNAME``,
        ``S3_API_PORT``), ``S3_ACCESS_KEY``, ``S3_SECRET_KEY``, ``S3_BUCKET``
        and ``AWS_REGION``.
        """
        from graflo.connections.env import docker_env

        env_vars = docker_env("seaweedfs", docker_dir, required=False)
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

        return cls(**data)
