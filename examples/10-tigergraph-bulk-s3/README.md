# Example 10: TigerGraph bulk load + S3 staging

This example extends the [CSV + edge weights](../../docs/examples/example-3.md) style manifest with:

- **`bindings.staging_proxy`** — maps the logical name `bulk_s3` to the runtime proxy key `minio_bulk`.
- **`TigergraphConfig.bulk_load`** — set in `ingest.py` so the target uses CSV staging + `LOADING JOB` (see [TigerGraph bulk load](../../docs/concepts/tigergraph_bulk_load.md)).

Secrets stay out of YAML: the script registers [`S3GeneralizedConnConfig`](../../graflo/hq/connection_provider.py) on [`InMemoryConnectionProvider`](../../graflo/hq/connection_provider.py).

## Run (from this directory)

```bash
cd examples/10-tigergraph-bulk-s3
uv run python ingest.py
```

Ensure TigerGraph env matches `docker/tigergraph/.env` (or override `TIGERGRAPH_*`).

## Emulate S3 with MinIO

1. Start MinIO (example):

   ```bash
   docker run -p 9000:9000 -p 9001:9001 \
     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
     minio/minio server /data --console-address ":9001"
   ```

2. Open the console (`http://127.0.0.1:9001`), create bucket **`graflo-staging`** (or set `BULK_S3_BUCKET`).

3. Export credentials for boto3 (used during bulk finalize):

   ```bash
   export MINIO_ENDPOINT=http://127.0.0.1:9000
   export MINIO_ACCESS_KEY=minioadmin
   export MINIO_SECRET_KEY=minioadmin
   export BULK_S3_BUCKET=graflo-staging
   ```

4. **TigerGraph** must be able to read the resulting `s3://` URLs (same cloud, IAM, or connector config on the TG side). If you only want **local** paths in `LOADING JOB`, disable S3 in the example:

   ```bash
   export BULK_USE_S3=0
   uv run python ingest.py
   ```

## Other ways to “fake” S3 in Python

| Tool | Use case |
|------|-----------|
| **MinIO** | Dev/prod-like S3 API; real HTTP; works with boto3 `endpoint_url`. |
| **LocalStack** | Full local AWS surface; S3 endpoint for integration tests. |
| **moto** | In-process mock (`@mock_aws`); great for **unit tests** of upload code, not for a live TigerGraph cluster reading `s3://`. |

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | Schema, ingestion, `staging_proxy` wiring. |
| `data/relations.csv` | Tiny CSV for `relations` resource. |
| `ingest.py` | Enables `bulk_load`, registers S3 provider, runs `define_and_ingest`. |
