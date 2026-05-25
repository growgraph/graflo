# Object storage (S3-compatible)

GraFlo uses **S3-compatible object storage** (MinIO, AWS S3, etc.) in one primary way today: **TigerGraph bulk staging** — uploading staged CSV files so a `LOADING JOB` can reference `s3://` URLs. This is separate from **ingestion sources** (files, SQL, SPARQL) described elsewhere.

## Staging (TigerGraph bulk load)

When `TigergraphConfig.bulk_load` uploads staged CSVs:

1. The manifest can declare **`bindings.staging_proxy`**: logical names (e.g. `bulk_s3`) map to **`conn_proxy`** keys (non-secret).
2. At runtime, [`InMemoryConnectionProvider`](../reference/hq/connection_provider.md) registers [`S3GeneralizedConnConfig`](../reference/hq/connection_provider.md) for that proxy.
3. **`graflo.object_storage.upload_staged_csvs`** uploads files with boto3; GSQL uses the returned `s3://bucket/key` paths behind a **`CREATE DATA_SOURCE`** so TigerGraph can read MinIO (not bare `s3://` against AWS). Optional **`MINIO_LOADER_ENDPOINT`** in `docker/minio/.env` sets the endpoint as seen from the TigerGraph server when it differs from the host URL used by boto3.

Secrets stay out of YAML; credentials come from environment / `docker/minio/.env` via `MinioConfig.from_docker_env()`.

```mermaid
flowchart LR
    M[GraphManifest staging_proxy]
    P[ConnectionProvider conn_proxy]
    S3[S3GeneralizedConnConfig]
    U[graflo.object_storage upload]
    TG[TigerGraph LOADING JOB]
    M --> P --> S3 --> U --> TG
```

## Configuration: `MinioConfig` / `S3EndpointConfig`

`MinioConfig` (alias **`S3EndpointConfig`**) in **`graflo.object_storage`** holds endpoint URL, access key, secret key, default bucket, and region. It is **not** a graph database target and does not use `ConnectionManager` for graph backends.

Loading from the repo’s Docker stack matches other services:

```python
from graflo.db import MinioConfig
# or: from graflo.object_storage import MinioConfig

cfg = MinioConfig.from_docker_env()  # reads docker/minio/.env
```

The **`graflo.object_storage`** package exposes bucket helpers (`ensure_staging_bucket_for_config`, `ensure_bucket_exists`), boto3 client factories (`boto3_s3_client_from_generalized`, `boto3_s3_client_from_minio`), and `upload_staged_csvs`.

## Not a `ResourceConnector`

`FileConnector` and other **resource connectors** describe **ingestion inputs** (local paths, tables, SPARQL). **Staging** does not add a new `BoundSourceKind`: it is an **output path** for bulk CSV upload, not a row source.

Future **“ingest from S3”** (read objects as a source) would be a **new connector type** and data source implementation, not an overload of `FileConnector`.

## See also

- [TigerGraph bulk load](../guides/tigergraph_bulk_load.md)
- [Concepts overview](index.md) (bindings and `staging_proxy`)
- Example: `examples/10-tigergraph-bulk-s3/` in the repository
