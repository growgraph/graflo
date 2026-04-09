# Example 10: TigerGraph bulk load and S3 staging

This example shows how to combine:

- **`TigergraphConfig.bulk_load`** — CSV staging + native `LOADING JOB` instead of REST++ upserts for the ingest run.
- **`Bindings.staging_proxy`** — manifest-visible **names** that map to **`S3GeneralizedConnConfig`** on an [`InMemoryConnectionProvider`](../reference/hq/connection_provider.md) (no secrets in YAML).

The companion directory is:

- [`examples/10-tigergraph-bulk-s3/`](https://github.com/growgraph/graflo/tree/main/examples/10-tigergraph-bulk-s3)

## Prerequisites

- A running TigerGraph instance (for example `TigergraphConfig.from_docker_env()` against `docker/tigergraph/.env`).
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

```python
import os
from graflo.hq.connection_provider import InMemoryConnectionProvider, S3GeneralizedConnConfig

provider = InMemoryConnectionProvider()
provider.register_generalized_config(
    conn_proxy="minio_bulk",
    config=S3GeneralizedConnConfig(
        bucket=os.environ.get("BULK_S3_BUCKET", "graflo-staging"),
        region="us-east-1",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    ),
)

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=ingestion_params,
    connection_provider=provider,
)
```

See **`ingest.py`** in the example folder for a full script that sets `bulk_load` on the TigerGraph config and runs `define_and_ingest`.

## Emulating S3 locally

The [concept page](../concepts/tigergraph_bulk_load.md#emulating-s3-in-development) compares **MinIO**, **LocalStack**, and **moto**. For a quick MinIO container:

```bash
docker run -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Create a bucket (e.g. `graflo-staging`) in the console, then point `MINIO_ENDPOINT` at `http://127.0.0.1:9000` when running the example.
