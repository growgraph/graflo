"""Pydantic models for runtime source connection configuration.

Leaf module: no imports from ``bindings``, ``data_source``, ``hq``, or ``connection_provider``.
Used by :mod:`graflo.hq.connection_provider` and :mod:`graflo.data_source.api`.
"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field

from graflo.db.connection import PostgresConfig, SparqlEndpointConfig


class SparqlAuth(BaseModel):
    """Authentication payload for SPARQL endpoint access."""

    username: str | None = None
    password: str | None = None


class ApiAuth(BaseModel):
    """Authentication payload for REST API source access."""

    auth_type: Literal["bearer", "basic", "digest", "api_key"] = "bearer"
    token: str | None = None
    username: str | None = None
    password: str | None = None
    header_name: str = "Authorization"
    prefix: str = "Bearer"


class RestApiConnConfig(BaseModel):
    """Runtime REST API connection settings (base URL and credentials)."""

    base_url: str
    auth: ApiAuth | None = None
    default_headers: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_env(cls, env_prefix: str = "REST_API_") -> RestApiConnConfig:
        """Load REST API config from environment variables."""
        base_url = os.environ.get(f"{env_prefix}BASE_URL")
        if not base_url:
            raise ValueError(
                f"Environment variable {env_prefix}BASE_URL is required for RestApiConnConfig"
            )
        token = os.environ.get(f"{env_prefix}TOKEN")
        username = os.environ.get(f"{env_prefix}USERNAME")
        password = os.environ.get(f"{env_prefix}PASSWORD")
        auth: ApiAuth | None = None
        if token is not None:
            auth = ApiAuth(auth_type="bearer", token=token)
        elif username is not None or password is not None:
            auth = ApiAuth(auth_type="basic", username=username, password=password)
        return cls(base_url=base_url, auth=auth)


class PostgresGeneralizedConnConfig(BaseModel):
    """Generalized runtime config variant for SQL/Postgres connections."""

    kind: Literal["postgres"] = "postgres"
    config: PostgresConfig


class SparqlGeneralizedConnConfig(BaseModel):
    """Generalized runtime config variant for SPARQL endpoint connections."""

    kind: Literal["sparql"] = "sparql"
    config: SparqlEndpointConfig


class S3GeneralizedConnConfig(BaseModel):
    """Runtime credentials and defaults for S3 staging (TigerGraph bulk ingest)."""

    kind: Literal["s3"] = "s3"
    bucket: str | None = Field(
        default=None,
        description="Default bucket when TigergraphBulkLoadConfig.s3_bucket is unset.",
    )
    region: str | None = Field(default=None)
    aws_access_key_id: str | None = Field(default=None)
    aws_secret_access_key: str | None = Field(default=None)
    endpoint_url: str | None = Field(
        default=None, description="For S3-compatible endpoints (MinIO, etc.)."
    )
    loader_endpoint_url: str | None = Field(
        default=None,
        description=(
            "S3 endpoint URL as seen by TigerGraph when it runs in another network "
            "namespace (e.g. Docker). Used only in CREATE DATA_SOURCE for LOADING JOB; "
            "boto3 continues to use endpoint_url."
        ),
    )


class ApiGeneralizedConnConfig(BaseModel):
    """Generalized runtime config variant for REST API connections."""

    kind: Literal["rest_api"] = "rest_api"
    config: RestApiConnConfig


GeneralizedConnConfig = (
    PostgresGeneralizedConnConfig
    | SparqlGeneralizedConnConfig
    | S3GeneralizedConnConfig
    | ApiGeneralizedConnConfig
)
