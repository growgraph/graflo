# Example 10: TigerGraph bulk load and S3 staging

This example shows how to combine:

- **`TigergraphConfig.bulk_load`** — CSV staging + native `LOADING JOB` instead of REST++ upserts for the ingest run.
- **`Bindings.staging_proxy`** — manifest-visible **names** that map to **`S3GeneralizedConnConfig`** on an [`InMemoryConnectionProvider`](../reference/connections/provider.md) (no secrets in YAML).

The companion directory is:

- [`examples/10-tigergraph-bulk-s3/`](https://github.com/growgraph/graflo/tree/main/examples/10-tigergraph-bulk-s3)

## Prerequisites

- A running TigerGraph instance (for example `TigergraphConfig.from_docker_env()` against the `docker/tigergraph` stack).
- For S3 upload during finalize: either **AWS S3**, or a **MinIO** (or other S3-compatible) server reachable from your machine **and** from TigerGraph if the loader must read `s3://` URIs (network and IAM policies are deployment-specific).

## Manifest: `staging_proxy`

The manifest adds a small staging table beside ordinary connectors:

```yaml
bindings:
  staging_proxy:
    - name: bulk_s3
      conn_proxy: minio_bulk
```

The label `bulk_s3` is referenced from **`TigergraphConfig.bulk_load.s3_staging_name`**. The label `minio_bulk` is the key used when registering `S3GeneralizedConnConfig` in Python.

## Runtime: register S3 config and ingest

The example builds the S3 config from the MinIO docker stack rather than assembling one by hand, so the endpoint, credentials, bucket and the loader-side endpoint all come from one place:

```python
from graflo.connections import InMemoryConnectionProvider, TigergraphBulkLoadConfig
from graflo.object_storage import MinioConfig, ensure_staging_bucket_for_config

minio_conf = MinioConfig.from_docker_env()

conn_conf.bulk_load = TigergraphBulkLoadConfig(
    enabled=True,
    staging_dir=str(staging_dir),
    s3_staging_name="bulk_s3",      # -> bindings.staging_proxy row
    s3_bucket=minio_conf.bucket,
    s3_key_prefix="demo",
)

provider = InMemoryConnectionProvider()
provider.register_generalized_config(
    conn_proxy="minio_bulk",
    config=minio_conf.to_s3_generalized_conn_config(),
)
ensure_staging_bucket_for_config(minio_conf)   # preflight: fails fast if unreachable

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=ingestion_params,
    connection_provider=provider,
)
```

For AWS or any other endpoint, construct [`S3GeneralizedConnConfig`](../reference/connections/provider.md) directly and register it under the same `conn_proxy` label — `MinioConfig.to_s3_generalized_conn_config()` is a convenience, not a requirement.

Set `BULK_USE_S3=0` to skip upload entirely: the `LOADING JOB` then references local absolute paths, which TigerGraph must be able to see on its own filesystem.

## Verifying the run

Bulk ingest can finish successfully and leave the graph empty — the `LOADING JOB` reads the staged files itself, so an `s3://` URL that TigerGraph cannot resolve is not an error GraFlo observes. The example ships `inspect_bulk.py`, which reports the staged CSVs (one file per vertex type, `edge_<relation>.csv` per relation) alongside what is actually in the graph, and exits non-zero when either is missing.

The usual cause of an empty graph is an endpoint that is valid for boto3 but not for the database: GraFlo emits `CREATE DATA_SOURCE` using `loader_endpoint_url` when set and `endpoint_url` otherwise, so a loopback address reaches a containerised TigerGraph unchanged. Set **`MINIO_LOADER_ENDPOINT`** to an address valid inside that container. `ingest.py` warns about this combination before it ingests.

## Emulating S3 locally

The [TigerGraph bulk load guide](../guides/tigergraph_bulk_load.md#emulating-s3-in-development) compares **MinIO**, **LocalStack**, and **moto**. For MinIO, use the repo's compose stack under `docker/minio` — it carries the image pin and the port and credential defaults that `MinioConfig.from_docker_env()` reads:

```bash
cd docker/minio
docker compose --env-file .env --profile graflo.minio up -d
```

The example creates the staging bucket on its own; `minio_init.py` does the same thing standalone.
