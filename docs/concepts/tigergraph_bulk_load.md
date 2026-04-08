# TigerGraph bulk load (CSV staging + LOADING JOB)

For TigerGraph targets, GraFlo can optionally bypass per-record REST++ JSON upserts and instead **append typed CSV files** during ingestion, then run a single **`CREATE LOADING JOB` / `RUN LOADING JOB`** sequence at the end of the ingest. The default REST path is unchanged when bulk load is disabled.

## When to use it

- Large **batch** or **initial** loads where REST++ overhead dominates.
- When TigerGraph can read staged files from **local disk** (same host as the loader) or from **S3** (or an S3-compatible endpoint).

## Configuration

### Target database (`TigergraphConfig`)

Set a nested **`bulk_load`** block on [`TigergraphConfig`](../reference/db/connection/onto.md) (see `TigergraphBulkLoadConfig` in code):

| Field | Role |
|-------|------|
| `enabled` | Turn bulk mode on. |
| `staging_dir` | Local directory for CSV files (a session subfolder is created per run). |
| `separator`, `include_header`, etc. | CSV layout passed through to `LOAD ... USING`. |
| `loading_job` | `concurrency`, `batch_size`, `job_name_prefix`, `run_mode`, `drop_job_after_run`. |
| `s3_staging_name` | Optional: name of a row in **`bindings.staging_proxy`** → resolves `conn_proxy`. |
| `s3_conn_proxy` | Optional: use this proxy label directly (no manifest row). |
| `s3_bucket`, `s3_key_prefix` | Destination for upload; bucket may also live on `S3GeneralizedConnConfig`. |

### Manifest (`Bindings.staging_proxy`)

**Input** connectors stay on `connectors` / `resource_connector` / `connector_connection` as today. **Staging** (S3 credentials) uses a parallel list that only carries **names**, not secrets:

```yaml
bindings:
  staging_proxy:
    - name: bulk_s3
      conn_proxy: minio_bulk
```

At runtime, `conn_proxy` must be registered on [`InMemoryConnectionProvider`](../reference/hq/connection_provider.md) as an [`S3GeneralizedConnConfig`](../reference/hq/connection_provider.md).

### Runtime provider

Call `register_generalized_config(conn_proxy="minio_bulk", config=S3GeneralizedConnConfig(...))` and pass that provider into [`GraphEngine.ingest`](../reference/hq/graph_engine.md) (or [`Caster.ingest`](../reference/hq/caster.md)). The manifest never stores AWS keys.

## Execution flow

1. **Begin** — First batch opens a bulk session (CSV writers under `staging_dir/<session_id>/`).
2. **Append** — Each cast batch appends rows per physical vertex/edge type.
3. **Finalize** — After all resources: optionally **upload** to S3, build GSQL with `DEFINE FILENAME` pointing at `file://` or `s3://` URLs, **`RUN LOADING JOB`**, optionally **`DROP JOB`**.

## Limitations (current release)

- **`blank_vertices`** in the logical schema are rejected at `bulk_load_begin`.
- Resources with **`extra_weights`** (DB lookups during ingest) cannot use bulk for that resource; use REST ingest or remove extra weights for those resources.

Upsert semantics differ from REST: native **LOAD** is oriented toward **append** semantics; plan idempotency and clears according to your operations model.

## Emulating S3 in development

GraFlo uses **boto3**. Any endpoint that speaks the S3 API works if you set `endpoint_url` on `S3GeneralizedConnConfig`:

1. **[MinIO](https://min.io/)** (very common): run a container, create a bucket, point `endpoint_url` at `http://127.0.0.1:9000` (or your mapped port), use the MinIO root user/password as `aws_access_key_id` / `aws_secret_access_key`.
2. **[LocalStack](https://localstack.cloud/)** — S3-compatible endpoint for local stacks.
3. **moto** (Python tests) — `@mock_aws` from the `moto` library mocks boto3 calls in-process; useful for **unit tests**, not for TigerGraph itself (the database still needs a real `s3://` it can reach unless you test upload-only code paths).

For **end-to-end** tests against a real loader, MinIO or cloud S3 with matching VPC/network access is the usual approach.

## See also

- [Example 10: TigerGraph bulk load and S3 staging](../examples/example-10.md)
- Implementation: `graflo/db/tigergraph/bulk_csv.py` (CSV layout), `graflo/db/tigergraph/bulk_gsql.py` (GSQL generation)
