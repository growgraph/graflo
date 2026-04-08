"""Upload local bulk CSV staging files to S3 for TigerGraph loading."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from graflo.hq.connection_provider import S3GeneralizedConnConfig


def upload_staged_csvs(
    *,
    staged_files: dict[str, Path],
    bucket: str,
    key_prefix: str,
    session_id: str,
    s3_cfg: S3GeneralizedConnConfig,
) -> dict[str, str]:
    """Upload files and return manifest key -> ``s3://bucket/key`` for GSQL."""
    client = boto3.client(
        "s3",
        region_name=s3_cfg.region or None,
        endpoint_url=s3_cfg.endpoint_url or None,
        aws_access_key_id=s3_cfg.aws_access_key_id or None,
        aws_secret_access_key=s3_cfg.aws_secret_access_key or None,
    )
    base = "/".join(p.strip("/") for p in (key_prefix, session_id) if p)
    urls: dict[str, str] = {}
    for label, path in staged_files.items():
        safe_name = path.name
        s3_key = f"{base}/{safe_name}" if base else safe_name
        client.upload_file(str(path), bucket, s3_key)
        urls[label] = f"s3://{bucket}/{s3_key}"
    return urls
