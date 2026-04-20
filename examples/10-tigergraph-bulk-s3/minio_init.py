"""
Ensure the staging bucket exists on MinIO (or any S3-compatible endpoint).

Delegates to :func:`graflo.object_storage.ensure_staging_bucket_for_config`.
Loads ``docker/minio/.env`` via :class:`~graflo.db.MinioConfig` when no config is passed.
"""

from __future__ import annotations

import logging
import sys

from botocore.exceptions import ClientError

from graflo.object_storage import ensure_staging_bucket_for_config

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ensure_staging_bucket_for_config()


if __name__ == "__main__":
    try:
        main()
    except ClientError as e:
        logger.error("S3 error: %s", e)
        sys.exit(1)
