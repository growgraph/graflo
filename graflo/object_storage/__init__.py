"""S3-compatible object storage helpers (boto3): config, client, bucket ensure.

This package is separate from graph :class:`~graflo.db.ConnectionManager` targets.
Use :class:`~graflo.object_storage.config.MinioConfig` with
:meth:`~graflo.object_storage.config.MinioConfig.from_docker_env` alongside
``docker/minio/.env``, same idea as ``Neo4jConfig.from_docker_env``.
"""

from graflo.object_storage.bucket import (
    ensure_bucket_exists,
    ensure_staging_bucket_for_config,
)
from graflo.object_storage.config import (
    MinioConfig,
    S3EndpointConfig,
    parse_dotenv_file,
)
from graflo.object_storage.s3_client import (
    boto3_s3_client,
    boto3_s3_client_from_minio,
)

__all__ = [
    "MinioConfig",
    "S3EndpointConfig",
    "boto3_s3_client",
    "boto3_s3_client_from_minio",
    "ensure_bucket_exists",
    "ensure_staging_bucket_for_config",
    "parse_dotenv_file",
]
