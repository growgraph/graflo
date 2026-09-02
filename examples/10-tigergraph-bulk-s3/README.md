# Example 10: TigerGraph bulk load + S3 staging

This example extends the [CSV + edge weights](../../docs/examples/example-3.md) style manifest with:

- **`bindings.staging_proxy`** — maps the logical name `bulk_s3` to the runtime proxy key `minio_bulk`.
- **`TigergraphConfig.bulk_load`** — set in `ingest.py` so the target uses CSV staging + `LOADING JOB` (see [TigerGraph bulk load](../../docs/guides/tigergraph_bulk_load.md)).

Secrets stay out of YAML: the script registers [`S3GeneralizedConnConfig`](../../graflo/connections/sources.py) on [`InMemoryConnectionProvider`](../../graflo/connections/provider.py).

## Run

```bash
uv run python examples/10-tigergraph-bulk-s3/ingest.py
uv run python examples/10-tigergraph-bulk-s3/inspect_bulk.py
```

The scripts chdir into the example themselves (`_common.example_workdir()`), so the manifest's `sub_path: data` connector resolves from any working directory.

Ensure TigerGraph env matches the `docker/tigergraph` environment file (or override `TIGERGRAPH_*`).

Staging uploads use **`MinioConfig.from_docker_env()`**, which reads the `docker/minio` environment file the same way examples like [Neo4j ingest](../4-ingest-neo4j/ingest.py) use `Neo4jConfig.from_docker_env()` — no need to export `MINIO_*` in the shell unless you want to override. Bucket ensure and upload helpers live in **`graflo.object_storage`**; see [Object storage (S3 staging)](../../docs/concepts/operations/object_storage.md).

## Verify

A bulk ingest can report success and leave the graph empty: the `LOADING JOB` reads the staged files itself, so an `s3://` URL that TigerGraph cannot resolve is not an error GraFlo ever observes. `inspect_bulk.py` closes that gap and exits non-zero when either half is missing:

```bash
uv run python examples/10-tigergraph-bulk-s3/inspect_bulk.py
uv run python examples/10-tigergraph-bulk-s3/inspect_bulk.py --staged-only   # no DB connection
```

Expected output:

- **Staged** — `company.csv` (4 rows) and `edge_relates.csv` (2 rows) under the newest `bulk_staging/<session>/`. A session with only `company.csv` means the pipeline's edge step never fired.
- **Loaded** — 3 `company` vertices (Acme, Beta, Gamma — the duplicate `Acme` row upserts) and 2 `relates` edges.

`ingest.py` also preflights the S3 path before ingesting: an unreachable MinIO fails immediately, and a loopback endpoint with no `MINIO_LOADER_ENDPOINT` logs a warning, because that combination is what silently produces an empty graph.

## Emulate S3 with MinIO

1. Start MinIO from the repo compose stack (recommended; the image pin lives in the `docker/minio` environment file):

   ```bash
   cd ../../docker/minio
   docker compose --env-file .env --profile graflo.minio up -d
   ```

2. Optional: ensure the staging bucket exists ( **`ingest.py` does this automatically** when `BULK_USE_S3=1`):

   ```bash
   uv run python minio_init.py
   ```

3. **TigerGraph** must be able to read the resulting `s3://` URLs (same cloud, IAM, or connector config on the TG side). If you only want **local** paths in `LOADING JOB`, disable S3 in the example:

   ```bash
   export BULK_USE_S3=0
   uv run python examples/10-tigergraph-bulk-s3/ingest.py
   ```

## Troubleshooting

- **`Connection refused` to `127.0.0.1:9000` (or your `MINIO_API_PORT`)**: The script talks to the MinIO **S3 API**, not the web console. A URL like `http://127.0.0.1:9001/endpoints` is the **console** (different port and service); boto3 must use `MINIO_API_PORT` / `MINIO_ENDPOINT` from the `docker/minio` environment file. Verify `docker ps` shows `graflo.minio` as **Up**. If the container is stuck in **Created** or never starts, check compose logs: `Bind for ... :9001 failed: port is already allocated` means another process already uses that host port. Set `MINIO_CONSOLE_PORT` (and `MINIO_API_PORT` if needed) to free ports, run `docker rm -f graflo.minio`, then bring the stack up again. See **MinIO** in [`docker/README.md`](../../docker/README.md).

- **`inspect_bulk.py` reports an empty graph (S3 / `BULK_USE_S3=1`)**: TigerGraph loads from `s3://` using a **GSQL DATA_SOURCE** (credentials + MinIO endpoint). That endpoint must be reachable **from the TigerGraph process** (often inside Docker). If GraFlo and MinIO run on the host but TigerGraph is in a container, a `127.0.0.1` endpoint is wrong for the loader — this is the case `ingest.py` warns about at startup. Set **`MINIO_LOADER_ENDPOINT`** (or `MINIO_TIGERGRAPH_ENDPOINT`) to a URL the TigerGraph container can use (e.g. `http://172.17.0.1:9003` on Linux, or `http://host.docker.internal:9003` where supported). Python/boto3 still uses `MINIO_HOSTNAME` + `MINIO_API_PORT` for uploads. Alternatively use **`BULK_USE_S3=0`** so the LOADING JOB uses local file paths (TigerGraph must see those paths).

- **`inspect_bulk.py` reports staged vertices but no `edge_relates.csv`**: staging never received an edge batch, so the problem is upstream of the loader — check the `relations` resource pipeline rather than the S3 or loader configuration.

## Other ways to "fake" S3 in Python

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
| `_common.py` | Paths, config loaders, the `example_workdir()` chdir helper, staging-session lookup. |
| `minio_init.py` | Thin CLI for `graflo.object_storage.ensure_staging_bucket_for_config` (same as `ingest.py` when `BULK_USE_S3=1`). |
| `ingest.py` | Enables `bulk_load`, registers S3 provider, preflights the endpoint, runs `define_and_ingest`. |
| `inspect_bulk.py` | Reports staged CSVs and the loaded graph; non-zero exit on either gap. |

Staged CSVs land in `bulk_staging/<session>/` and are gitignored — each run writes a new session directory.
