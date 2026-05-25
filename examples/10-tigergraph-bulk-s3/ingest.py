"""
Example 10: TigerGraph bulk CSV + LOADING JOB with optional S3 staging.

- Manifest: `manifest.yaml` includes `bindings.staging_proxy` (names only).
- Runtime: register `S3GeneralizedConnConfig` under `conn_proxy=minio_bulk`.
- Target: `TigergraphConfig.bulk_load` enables CSV staging; finalize uploads to S3
  when `s3_staging_name` / bucket / provider are set.

Emulate S3 locally with MinIO (see README): `MinioConfig.from_docker_env()` reads
`docker/minio/.env` like other examples use `Neo4jConfig.from_docker_env()`.
For pure local CSV without upload, set ``BULK_USE_S3=0``; LOADING JOB will use
local absolute paths only (TigerGraph must see those paths).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.db import MinioConfig, TigergraphConfig
from graflo.db.connection import TigergraphBulkLoadConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams
from graflo.hq.connection_provider import InMemoryConnectionProvider
from graflo.object_storage import ensure_staging_bucket_for_config

logger = logging.getLogger(__name__)


def _bulk_staging_dir() -> Path:
    base = Path(__file__).resolve().parent / "bulk_staging"
    base.mkdir(parents=True, exist_ok=True)
    return base


def main() -> None:
    logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
    logging.getLogger("graflo").setLevel(logging.INFO)

    manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
    manifest.finish_init()

    conn_conf = TigergraphConfig.from_docker_env()
    conn_conf.max_job_size = 5000

    use_s3 = os.environ.get("BULK_USE_S3", "1").lower() in ("1", "true", "yes")
    provider = InMemoryConnectionProvider()
    if use_s3:
        minio_conf = MinioConfig.from_docker_env()
        if bucket_override := os.environ.get("BULK_S3_BUCKET"):
            minio_conf = minio_conf.model_copy(update={"bucket": bucket_override})
        conn_conf.bulk_load = TigergraphBulkLoadConfig(
            enabled=True,
            staging_dir=str(_bulk_staging_dir()),
            s3_staging_name="bulk_s3",
            s3_bucket=minio_conf.bucket,
            s3_key_prefix=os.environ.get("BULK_S3_PREFIX", "demo"),
        )
        provider.register_generalized_config(
            conn_proxy="minio_bulk",
            config=minio_conf.to_s3_generalized_conn_config(),
        )
        ensure_staging_bucket_for_config(minio_conf)
    else:
        conn_conf.bulk_load = TigergraphBulkLoadConfig(
            enabled=True,
            staging_dir=str(_bulk_staging_dir()),
        )

    db_type = conn_conf.connection_type
    engine = GraphEngine(target_db_flavor=db_type)

    ingestion_params = IngestionParams(clear_data=True)
    engine.define_and_ingest(
        manifest=manifest,
        target_db_config=conn_conf,
        ingestion_params=ingestion_params,
        recreate_schema=True,
        connection_provider=provider,
    )
    logger.info("Ingest finished (bulk_load=%s)", conn_conf.bulk_load)


if __name__ == "__main__":
    main()
