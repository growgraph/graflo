"""S3-compatible object storage helpers (boto3): config, client, bucket ensure.

This package is separate from graph :class:`~graflo.db.ConnectionManager` targets.
Use :class:`~graflo.object_storage.config.S3EndpointConfig`; its
:meth:`~graflo.object_storage.config.S3EndpointConfig.from_docker_env` reads
the bundled S3 service's settings, as ``Neo4jConfig.from_docker_env`` does.
"""

from graflo.object_storage.bucket import (
    ensure_bucket_exists,
    ensure_staging_bucket_for_config,
)
from graflo.object_storage.config import (
    S3EndpointConfig,
)
from graflo.object_storage.s3_client import (
    boto3_s3_client,
    boto3_s3_client_from_endpoint,
)

__all__ = [
    "S3EndpointConfig",
    "boto3_s3_client",
    "boto3_s3_client_from_endpoint",
    "ensure_bucket_exists",
    "ensure_staging_bucket_for_config",
]
