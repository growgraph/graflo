"""
Example 10: TigerGraph bulk CSV + LOADING JOB with optional S3 staging.

- Manifest: `manifest.yaml` includes `bindings.staging_proxy` (names only).
- Runtime: register `S3GeneralizedConnConfig` under `conn_proxy=minio_bulk`.
- Target: `TigergraphConfig.bulk_load` enables CSV staging; finalize uploads to S3
  when `s3_staging_name` / bucket / provider are set.

Emulate S3 locally with MinIO (see README): `MinioConfig.from_docker_env()` reads
the MinIO docker environment file like other examples use
`Neo4jConfig.from_docker_env()`. For pure local CSV without upload, set
``BULK_USE_S3=0``; LOADING JOB will use local absolute paths only (TigerGraph
must see those paths).

    uv run python ingest.py
    uv run python inspect_bulk.py    # check what was staged and what landed
"""

from __future__ import annotations

import logging
import os

from _common import (
    STAGING_DIR,
    example_workdir,
    load_manifest,
    loader_endpoint_is_loopback,
    minio_config,
    tigergraph_config,
    use_s3,
)

from graflo.connections import (
    InMemoryConnectionProvider,
    TigergraphBulkLoadConfig,
    TigergraphConfig,
)
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams
from graflo.object_storage import ensure_staging_bucket_for_config

logger = logging.getLogger(__name__)


def configure_bulk_load(
    conn_conf: TigergraphConfig, provider: InMemoryConnectionProvider
) -> None:
    """Enable CSV staging on *conn_conf*, uploading to S3 unless BULK_USE_S3=0."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    if not use_s3():
        conn_conf.bulk_load = TigergraphBulkLoadConfig(
            enabled=True,
            staging_dir=str(STAGING_DIR),
        )
        return

    minio_conf = minio_config()
    conn_conf.bulk_load = TigergraphBulkLoadConfig(
        enabled=True,
        staging_dir=str(STAGING_DIR),
        s3_staging_name="bulk_s3",
        s3_bucket=minio_conf.bucket,
        s3_key_prefix=os.environ.get("BULK_S3_PREFIX", "demo"),
    )
    provider.register_generalized_config(
        conn_proxy="minio_bulk",
        config=minio_conf.to_s3_generalized_conn_config(),
    )

    # Preflight: raises with an actionable message when the S3 API is unreachable,
    # so a missing MinIO fails here rather than as an empty graph after a
    # "successful" ingest.
    ensure_staging_bucket_for_config(minio_conf)

    if loader_endpoint_is_loopback(minio_conf):
        logger.warning(
            "MinIO endpoint %s is a loopback address and MINIO_LOADER_ENDPOINT is "
            "unset, so TigerGraph's CREATE DATA_SOURCE will point there too. That "
            "works only if TigerGraph runs on this host; from a container the "
            "LOADING JOB reads nothing and the ingest still reports success. Set "
            "MINIO_LOADER_ENDPOINT for the MinIO docker stack, or run with "
            "BULK_USE_S3=0. Confirm either way with inspect_bulk.py.",
            minio_conf.endpoint_url,
        )


def main() -> None:
    logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
    logging.getLogger("graflo").setLevel(logging.INFO)
    logger.setLevel(logging.INFO)

    with example_workdir():
        manifest = load_manifest()

        conn_conf = tigergraph_config()
        provider = InMemoryConnectionProvider()
        configure_bulk_load(conn_conf, provider)

        engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
        engine.define_and_ingest(
            manifest=manifest,
            target_db_config=conn_conf,
            ingestion_params=IngestionParams(clear_data=True),
            recreate_schema=True,
            connection_provider=provider,
        )
    logger.info("Ingest finished (bulk_load=%s)", conn_conf.bulk_load)


if __name__ == "__main__":
    main()
