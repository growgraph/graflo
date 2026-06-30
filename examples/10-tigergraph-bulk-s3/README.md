# Example 10: TigerGraph bulk load + S3 staging

This example extends the [CSV + edge weights](../../docs/examples/example-3.md) style manifest with:

- **`bindings.staging_proxy`** — maps the logical name `bulk_s3` to the runtime proxy key `minio_bulk`.
- **`TigergraphConfig.bulk_load`** — set in `ingest.py` so the target uses CSV staging + `LOADING JOB` (see [TigerGraph bulk load](../../docs/guides/tigergraph_bulk_load.md)).

Secrets stay out of YAML: the script registers [`S3GeneralizedConnConfig`](../../graflo/hq/connection_provider.py) on [`InMemoryConnectionProvider`](../../graflo/hq/connection_provider.py).

## Run (from this directory)

```bash
cd examples/10-tigergraph-bulk-s3
uv run python ingest.py
```

Ensure TigerGraph env matches `docker/tigergraph/.env` (or override `TIGERGRAPH_*`).

Staging uploads use **`MinioConfig.from_docker_env()`**, which reads `docker/minio/.env` the same way examples like [Neo4j ingest](../4-ingest-neo4j/ingest.py) use `Neo4jConfig.from_docker_env()` — no need to export `MINIO_*` in the shell unless you want to override. Bucket ensure and upload helpers live in **`graflo.object_storage`**; see [Object storage (S3 staging)](../../docs/concepts/operations/object_storage.md).

## Emulate S3 with MinIO

1. Start MinIO from the repo compose stack (recommended; image pin lives in `docker/minio/.env`):

   ```bash
   cd ../../docker/minio
   docker compose --env-file .env --profile graflo.minio up -d
   ```

   Or run a standalone container (same defaults as `docker/minio`):

   ```bash
   docker run -p 9000:9000 -p 9001:9001 \
     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
     minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1 \
     server /data --console-address ":9001"
   ```

2. Optional: ensure the staging bucket exists ( **`ingest.py` does this automatically** when `BULK_USE_S3=1`):

   ```bash
   uv run python minio_init.py
   ```

3. **TigerGraph** must be able to read the resulting `s3://` URLs (same cloud, IAM, or connector config on the TG side). If you only want **local** paths in `LOADING JOB`, disable S3 in the example:

   ```bash
   export BULK_USE_S3=0
   uv run python ingest.py
   ```

## Troubleshooting

- **`Connection refused` to `127.0.0.1:9000` (or your `MINIO_API_PORT`)**: The script talks to the MinIO **S3 API**, not the web console. A URL like `http://127.0.0.1:9001/endpoints` is the **console** (different port and service); boto3 must use `MINIO_API_PORT` / `MINIO_ENDPOINT` from `docker/minio/.env`. Verify `docker ps` shows `graflo.minio` as **Up**. If the container is stuck in **Created** or never starts, check compose logs: `Bind for ... :9001 failed: port is already allocated` means another process already uses that host port. Set `MINIO_CONSOLE_PORT` (and `MINIO_API_PORT` if needed) to free ports, run `docker rm -f graflo.minio`, then bring the stack up again. See **MinIO** in [`docker/README.md`](../../docker/README.md).

- **Ingest finishes but the graph is empty (S3 / `BULK_USE_S3=1`)**: TigerGraph loads from `s3://` using a **GSQL DATA_SOURCE** (credentials + MinIO endpoint). That endpoint must be reachable **from the TigerGraph process** (often inside Docker). If GraFlo and MinIO run on the host but TigerGraph is in a container, `127.0.0.1` in `docker/minio/.env` is wrong for the loader. Set **`MINIO_LOADER_ENDPOINT`** (or `MINIO_TIGERGRAPH_ENDPOINT`) to a URL the TigerGraph container can use (e.g. `http://172.17.0.1:9003` on Linux, or `http://host.docker.internal:9003` where supported). Python/boto3 still uses `MINIO_HOSTNAME` + `MINIO_API_PORT` for uploads. Alternatively use **`BULK_USE_S3=0`** so the LOADING JOB uses local file paths (TigerGraph must see those paths on its filesystem).

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
| `minio_init.py` | Thin CLI for `graflo.object_storage.ensure_staging_bucket_for_config` (same as `ingest.py` when `BULK_USE_S3=1`). |
| `ingest.py` | Enables `bulk_load`, registers S3 provider, runs `define_and_ingest`. |
