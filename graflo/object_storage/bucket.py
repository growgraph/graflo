"""Ensure S3 buckets exist (idempotent) for bulk staging."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError, EndpointConnectionError

if TYPE_CHECKING:
    from graflo.object_storage.config import MinioConfig

logger = logging.getLogger(__name__)


def ensure_bucket_exists(client: Any, bucket: str) -> str:
    """Create *bucket* if it does not exist. Return *bucket*.

    Safe to call repeatedly. Raises :exc:`botocore.exceptions.ClientError`
    for non-recoverable API errors.
    """
    try:
        client.head_bucket(Bucket=bucket)
        logger.info("S3 bucket %r already exists", bucket)
        return bucket
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        http = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        missing = code in ("404", "NoSuchBucket", "NotFound") or http == 404
        if not missing:
            raise

    try:
        client.create_bucket(Bucket=bucket)
        logger.info("Created S3 bucket %r", bucket)
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            logger.info(
                "S3 bucket %r already exists (race or concurrent create)", bucket
            )
        else:
            raise

    return bucket


def ensure_staging_bucket_for_config(cfg: MinioConfig | None = None) -> str:
    """Ensure :attr:`~graflo.object_storage.config.MinioConfig.bucket` exists.

    If *cfg* is ``None``, loads :meth:`~graflo.object_storage.config.MinioConfig.from_docker_env`.
    """
    from graflo.object_storage.config import MinioConfig
    from graflo.object_storage.s3_client import boto3_s3_client_from_minio

    resolved = cfg or MinioConfig.from_docker_env()
    client = boto3_s3_client_from_minio(resolved)
    try:
        return ensure_bucket_exists(client, resolved.bucket)
    except EndpointConnectionError as e:
        raise RuntimeError(
            f"Cannot connect to {resolved.endpoint_url!r}. "
            "Start the S3 service (e.g. `docker compose --profile graflo.seaweedfs up -d` "
            "in `docker/seaweedfs`) and ensure `S3_API_PORT` / `S3_ENDPOINT` matches the "
            "published S3 API port (default host 8333)."
        ) from e
