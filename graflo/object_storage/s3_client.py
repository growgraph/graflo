"""Shared boto3 S3 client construction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from graflo.connections.sources import S3ConnConfig
    from graflo.object_storage.config import MinioConfig


def boto3_s3_client(cfg: S3ConnConfig) -> Any:
    """Build a boto3 S3 client from :class:`~graflo.connections.sources.S3ConnConfig`."""
    import boto3

    return boto3.client(
        "s3",
        region_name=cfg.region or None,
        endpoint_url=cfg.endpoint_url or None,
        aws_access_key_id=cfg.aws_access_key_id or None,
        aws_secret_access_key=cfg.aws_secret_access_key or None,
    )


def boto3_s3_client_from_minio(cfg: MinioConfig) -> Any:
    """Build a boto3 S3 client from :class:`~graflo.object_storage.config.MinioConfig`."""
    import boto3

    return boto3.client(
        "s3",
        region_name=cfg.region or None,
        endpoint_url=cfg.endpoint_url or None,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )
