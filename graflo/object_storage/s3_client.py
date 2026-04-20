"""Shared boto3 S3 client construction for staging and uploads."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from graflo.hq.connection_provider import S3GeneralizedConnConfig

    from graflo.object_storage.config import MinioConfig


def boto3_s3_client_from_generalized(cfg: "S3GeneralizedConnConfig") -> Any:
    """Build a boto3 S3 client from :class:`~graflo.hq.connection_provider.S3GeneralizedConnConfig`."""
    import boto3

    return boto3.client(
        "s3",
        region_name=cfg.region or None,
        endpoint_url=cfg.endpoint_url or None,
        aws_access_key_id=cfg.aws_access_key_id or None,
        aws_secret_access_key=cfg.aws_secret_access_key or None,
    )


def boto3_s3_client_from_minio(cfg: "MinioConfig") -> Any:
    """Build a boto3 S3 client from :class:`~graflo.object_storage.config.MinioConfig`."""
    import boto3

    return boto3.client(
        "s3",
        region_name=cfg.region or None,
        endpoint_url=cfg.endpoint_url or None,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )
