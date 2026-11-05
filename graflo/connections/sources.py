"""Pydantic models for runtime source connection configuration.

Leaf module: no imports from ``bindings``, ``data_source``, ``hq``, or
:mod:`graflo.connections.provider`. Used by :mod:`graflo.connections.provider`,
:mod:`graflo.data_source.api`, and :mod:`graflo.data_source.kafka`.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import Literal, cast

from pydantic import BaseModel, Field, model_validator

from graflo.connections.onto import PostgresConfig, SparqlEndpointConfig

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


_PROXY_NAME_DESCRIPTION = (
    "Non-secret runtime handle. Matches conn_proxy in manifest bindings. "
    "Must be unique per registry instance."
)
_ALIAS_DESCRIPTION = "Human-friendly display label."


class RestApiConnConfig(BaseModel):
    """Runtime REST API connection settings (base URL and credentials)."""

    kind: Literal["api"] = "api"
    base_url: str
    auth: ApiAuth | None = None
    default_headers: dict[str, str] = Field(default_factory=dict)
    proxy_name: str | None = Field(default=None, description=_PROXY_NAME_DESCRIPTION)
    alias: str | None = Field(default=None, description=_ALIAS_DESCRIPTION)

    @classmethod
    def from_env(cls, env_prefix: str) -> RestApiConnConfig:
        """Load REST API config from environment variables.

        Supported variables (all prefixed with *env_prefix*):

        - ``BASE_URL`` (required)
        - ``AUTH_TYPE``: ``bearer``, ``basic``, ``digest``, or ``api_key`` (default: ``bearer``)
        - ``TOKEN``, ``USERNAME``, ``PASSWORD``
        - ``HEADER_NAME``, ``PREFIX`` (bearer / api_key)

        With none of ``TOKEN``, ``USERNAME`` and ``PASSWORD`` set the API is
        taken to be open: ``auth`` is ``None`` and no credential header is sent.
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
        token = os.environ.get(f"{env_prefix}TOKEN")
        username = os.environ.get(f"{env_prefix}USERNAME")
        password = os.environ.get(f"{env_prefix}PASSWORD")
        if not (token or username or password):
            return cls(base_url=cast(str, base_url))

        auth = ApiAuth(
            auth_type=cast(AuthType, auth_type_lower),
            token=token,
            username=username,
            password=password,
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


class S3ConnConfig(BaseModel):
    """What a boto3 S3 client needs: endpoint, region and keys, plus a default bucket.

    Not a registered connection on its own: a :class:`FilesConnConfig` with an
    ``s3://`` root produces one (:meth:`FilesConnConfig.s3_config`).
    """

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

    @classmethod
    def from_env(cls, env_prefix: str) -> S3ConnConfig:
        """Load S3 staging config from ``{env_prefix}<FIELD>`` variables.

        Reads ``BUCKET``, ``REGION``, ``AWS_ACCESS_KEY_ID``,
        ``AWS_SECRET_ACCESS_KEY``, ``ENDPOINT_URL`` and ``LOADER_ENDPOINT_URL``;
        all are optional.
        """
        return cls(
            **{
                name: value
                for name in cls.model_fields
                if (value := os.environ.get(f"{env_prefix}{name.upper()}") or None)
                is not None
            }
        )


KafkaSecurityProtocol = Literal["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"]
_VALID_KAFKA_SECURITY_PROTOCOLS = frozenset(
    {"PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"}
)


class KafkaConnConfig(BaseModel):
    """Runtime Kafka broker connection settings (bootstrap and auth)."""

    kind: Literal["stream"] = "stream"
    bootstrap_servers: str
    security_protocol: KafkaSecurityProtocol = "PLAINTEXT"
    client_id: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    proxy_name: str | None = Field(default=None, description=_PROXY_NAME_DESCRIPTION)
    alias: str | None = Field(default=None, description=_ALIAS_DESCRIPTION)

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


_S3_SCHEME = "s3://"


class FilesConnConfig(BaseModel):
    """A place files are read from: a local directory or an S3 bucket prefix.

    ``root`` is an absolute local path, or ``s3://<bucket>[/<prefix>]``. A
    ``FileConnector`` bound to this connection reads ``sub_path`` relative to
    the root. The S3 fields apply to an ``s3://`` root only; keys come from
    ``{PREFIX}ACCESS_KEY_ID`` / ``{PREFIX}SECRET_ACCESS_KEY`` at run time.
    """

    kind: Literal["files"] = "files"
    root: str = Field(min_length=1)
    endpoint_url: str | None = Field(
        default=None, description="For S3-compatible endpoints (MinIO, SeaweedFS)."
    )
    region: str | None = None
    loader_endpoint_url: str | None = Field(
        default=None,
        description=(
            "The S3 endpoint as a bulk loader in another network namespace sees "
            "it; boto3 keeps using endpoint_url."
        ),
    )
    access_key_id: str | None = None
    secret_access_key: str | None = None
    proxy_name: str | None = Field(default=None, description=_PROXY_NAME_DESCRIPTION)
    alias: str | None = Field(default=None, description=_ALIAS_DESCRIPTION)

    @model_validator(mode="after")
    def _check_root(self) -> FilesConnConfig:
        if self.is_s3:
            if not self.bucket:
                raise ValueError(f"root {self.root!r} names no bucket")
        else:
            if not PurePosixPath(self.root).is_absolute():
                raise ValueError(
                    f"root {self.root!r} must be an absolute path or an s3:// URL"
                )
            s3_only = [
                name
                for name in (
                    "endpoint_url",
                    "region",
                    "loader_endpoint_url",
                    "access_key_id",
                    "secret_access_key",
                )
                if getattr(self, name) is not None
            ]
            if s3_only:
                raise ValueError(f"{', '.join(s3_only)} apply to an s3:// root only")
        return self

    @property
    def is_s3(self) -> bool:
        return self.root.startswith(_S3_SCHEME)

    @property
    def bucket(self) -> str | None:
        """The bucket of an ``s3://`` root."""
        if not self.is_s3:
            return None
        return self.root[len(_S3_SCHEME) :].split("/", 1)[0] or None

    @property
    def prefix(self) -> str:
        """The key prefix of an ``s3://`` root, without slashes at either end."""
        if not self.is_s3:
            return ""
        _, _, rest = self.root[len(_S3_SCHEME) :].partition("/")
        return rest.strip("/")

    @property
    def local_root(self) -> Path:
        """The directory of a local root."""
        if self.is_s3:
            raise ValueError(f"root {self.root!r} is not a local directory")
        return Path(self.root).expanduser()

    def s3_config(self) -> S3ConnConfig:
        """The boto3 settings of an ``s3://`` root."""
        if not self.is_s3:
            raise ValueError(f"root {self.root!r} is not an s3:// URL")
        return S3ConnConfig(
            bucket=self.bucket,
            region=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            endpoint_url=self.endpoint_url,
            loader_endpoint_url=self.loader_endpoint_url,
        )

    @classmethod
    def from_env(cls, env_prefix: str) -> FilesConnConfig:
        """Load from ``{env_prefix}<FIELD>``: ``ROOT`` is required, the rest optional."""
        if not os.environ.get(f"{env_prefix}ROOT"):
            raise ValueError(
                f"Environment variable {env_prefix}ROOT is required for FilesConnConfig"
            )
        return cls.model_validate(
            {
                name: value
                for name in cls.model_fields
                if name != "kind"
                and (value := os.environ.get(f"{env_prefix}{name.upper()}") or None)
                is not None
            }
        )


ConnConfig = (
    PostgresConfig
    | SparqlEndpointConfig
    | RestApiConnConfig
    | KafkaConnConfig
    | FilesConnConfig
)
"""A runtime connection a ``conn_proxy`` label resolves to."""
