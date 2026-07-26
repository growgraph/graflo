"""Pydantic models for runtime source connection configuration.

Leaf module: no imports from ``bindings``, ``data_source``, ``hq``, or ``connection_provider``.
Used by :mod:`graflo.hq.connection_provider`, :mod:`graflo.data_source.api`,
and :mod:`graflo.data_source.kafka`.
"""

from __future__ import annotations

import os
from typing import Literal, cast

from pydantic import BaseModel, Field

from graflo.db.connection import PostgresConfig, SparqlEndpointConfig

AuthType = Literal["bearer", "basic", "digest", "api_key"]
_VALID_AUTH_TYPES = frozenset({"bearer", "basic", "digest", "api_key"})


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
    def from_env(cls, env_prefix: str) -> RestApiConnConfig:
        """Load REST API config from environment variables.

        Supported variables (all prefixed with *env_prefix*):

        - ``BASE_URL`` (required)
        - ``AUTH_TYPE``: ``bearer``, ``basic``, ``digest``, or ``api_key`` (default: ``bearer``)
        - ``TOKEN``, ``USERNAME``, ``PASSWORD``
        - ``HEADER_NAME``, ``PREFIX`` (bearer / api_key)
        """
        base_url = os.environ.get(f"{env_prefix}BASE_URL")
        if not base_url:
            raise ValueError(
                f"Environment variable {env_prefix}BASE_URL is required for RestApiConnConfig"
            )

        auth_type_raw = os.environ.get(f"{env_prefix}AUTH_TYPE", "bearer")
        auth_type_lower = auth_type_raw.lower()
        if auth_type_lower not in _VALID_AUTH_TYPES:
            raise ValueError(
                f"Invalid {env_prefix}AUTH_TYPE={auth_type_raw!r}; "
                "expected bearer, basic, digest, or api_key"
            )
        auth = ApiAuth(
            auth_type=cast(AuthType, auth_type_lower),
            token=os.environ.get(f"{env_prefix}TOKEN"),
            username=os.environ.get(f"{env_prefix}USERNAME"),
            password=os.environ.get(f"{env_prefix}PASSWORD"),
            header_name=cast(
                str,
                os.environ.get(f"{env_prefix}HEADER_NAME") or "Authorization",
            ),
            prefix=cast(
                str,
                os.environ.get(f"{env_prefix}PREFIX") or "Bearer",
            ),
        )

        return cls(base_url=cast(str, base_url), auth=auth)


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


KafkaSecurityProtocol = Literal["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"]
_VALID_KAFKA_SECURITY_PROTOCOLS = frozenset(
    {"PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"}
)


class KafkaConnConfig(BaseModel):
    """Runtime Kafka broker connection settings (bootstrap and auth)."""

    bootstrap_servers: str
    security_protocol: KafkaSecurityProtocol = "PLAINTEXT"
    client_id: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    @classmethod
    def from_env(cls, env_prefix: str) -> KafkaConnConfig:
        """Load Kafka config from environment variables.

        Supported variables (all prefixed with *env_prefix*):

        - ``BOOTSTRAP_SERVERS`` (required)
        - ``SECURITY_PROTOCOL``: ``PLAINTEXT``, ``SASL_PLAINTEXT``, ``SASL_SSL``,
          or ``SSL`` (default: ``PLAINTEXT``)
        - ``CLIENT_ID``
        - ``SASL_MECHANISM``, ``SASL_USERNAME``, ``SASL_PASSWORD``
        """
        bootstrap_servers = os.environ.get(f"{env_prefix}BOOTSTRAP_SERVERS")
        if not bootstrap_servers:
            raise ValueError(
                f"Environment variable {env_prefix}BOOTSTRAP_SERVERS is required "
                "for KafkaConnConfig"
            )

        security_raw = os.environ.get(f"{env_prefix}SECURITY_PROTOCOL", "PLAINTEXT")
        security_upper = security_raw.upper()
        if security_upper not in _VALID_KAFKA_SECURITY_PROTOCOLS:
            raise ValueError(
                f"Invalid {env_prefix}SECURITY_PROTOCOL={security_raw!r}; "
                "expected PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, or SSL"
            )

        return cls(
            bootstrap_servers=cast(str, bootstrap_servers),
            security_protocol=cast(KafkaSecurityProtocol, security_upper),
            client_id=os.environ.get(f"{env_prefix}CLIENT_ID"),
            sasl_mechanism=os.environ.get(f"{env_prefix}SASL_MECHANISM"),
            sasl_username=os.environ.get(f"{env_prefix}SASL_USERNAME"),
            sasl_password=os.environ.get(f"{env_prefix}SASL_PASSWORD"),
        )

    def to_consumer_config(self) -> dict[str, str]:
        """Build a confluent-kafka consumer config dict from connection settings."""
        cfg: dict[str, str] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.client_id:
            cfg["client.id"] = self.client_id
        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username is not None:
            cfg["sasl.username"] = self.sasl_username
        if self.sasl_password is not None:
            cfg["sasl.password"] = self.sasl_password
        return cfg


class KafkaGeneralizedConnConfig(BaseModel):
    """Generalized runtime config variant for Kafka connections."""

    kind: Literal["kafka"] = "kafka"
    config: KafkaConnConfig


GeneralizedConnConfig = (
    PostgresGeneralizedConnConfig
    | SparqlGeneralizedConnConfig
    | S3GeneralizedConnConfig
    | ApiGeneralizedConnConfig
    | KafkaGeneralizedConnConfig
)
