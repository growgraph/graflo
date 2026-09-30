# TigerGraph bulk load

By default GraFlo writes to TigerGraph record by record through its REST
interface. For a large initial load that is slow. With bulk load switched on,
GraFlo writes the records to CSV files during ingestion and, at the end, runs
one TigerGraph loading job that reads them. This guide switches bulk load on,
first with files on local disk and then with files staged in an S3 bucket.

## What you need

- GraFlo installed (`pip install graflo`) and a running TigerGraph. The
  repository ships containers for TigerGraph and MinIO under `docker/`.
- A manifest that ingests into TigerGraph without bulk load. Bulk load
  changes how records are written, not what the manifest declares.
- For S3 staging: a bucket on MinIO or another S3-compatible service, and
  its credentials.

## Steps

### 1. Switch bulk load on

Set `bulk_load` on the TigerGraph config:

```python
from graflo.connections import TigergraphBulkLoadConfig, TigergraphConfig

conn_conf = TigergraphConfig.from_env()
conn_conf.bulk_load = TigergraphBulkLoadConfig(
    enabled=True,
    staging_dir="bulk_staging",
)
```

`staging_dir` is required. Each run writes its files into a new
subdirectory of it, named after the run's session id.

### 2. Ingest as usual

```python
from suthing import FileHandle

from graflo import GraphEngine, GraphManifest
from graflo.hq import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

The first batch opens a bulk session. Every batch appends rows to one CSV file
per vertex type and one per edge type. When all resources are done, GraFlo
creates a loading job for the files, runs it, and drops it. With local files,
TigerGraph must be able to read `staging_dir` at the same path, so this
works when TigerGraph runs on the same machine or mounts that directory.

### 3. Stage the files in S3 instead

When TigerGraph cannot see your disk, upload the files to a bucket. Name the
bucket connection in the manifest by label only:

```yaml
bindings:
  staging_proxy:
    - name: bulk_s3
      conn_proxy: minio_bulk
```

Register the credentials under the label, point the bulk config at the
staging name, and pass the provider to the ingest:

```python
from graflo.connections import InMemoryConnectionProvider
from graflo.object_storage import MinioConfig, ensure_staging_bucket_for_config

minio = MinioConfig.from_docker_env()
ensure_staging_bucket_for_config(minio)

provider = InMemoryConnectionProvider()
provider.register_generalized_config(
    conn_proxy="minio_bulk",
    config=minio.to_s3_generalized_conn_config(),
)

conn_conf.bulk_load = TigergraphBulkLoadConfig(
    enabled=True,
    staging_dir="bulk_staging",
    s3_staging_name="bulk_s3",
    s3_bucket=minio.bucket,
)

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
    connection_provider=provider,
)
```

`ensure_staging_bucket_for_config` creates the bucket if it is missing and
fails early if MinIO cannot be reached. At the end of the ingest GraFlo
uploads the files to `s3://<bucket>/graflo-bulk/<session id>/`, creates a
TigerGraph data source with the same credentials, and runs the loading job
against the uploaded objects.

If TigerGraph runs in a container and MinIO on your machine, TigerGraph needs
a different address for MinIO than GraFlo does. Set `MINIO_LOADER_ENDPOINT`
in the settings under `docker/minio` (or `loader_endpoint_url` on `MinioConfig`);
see [Object storage](../concepts/operations/object_storage.md#two-endpoints).

## What you should see

`staging_dir` holds one subdirectory per run with a CSV file per vertex type
(`<vertex type>.csv`) and per edge type (`edge_<relation>.csv`). The files
stay there after the run. With S3 staging the same files are in the bucket
under the session id. TigerGraph holds the loaded vertices and edges.

Count the loaded records after the first bulk load. If TigerGraph could not
reach the files, the loading job loads nothing and the ingest still finishes
without an error.

## Options

All options live on `TigergraphBulkLoadConfig`:

| Option | Default | Meaning |
|---|---|---|
| `enabled` | `False` | Use bulk load for this target |
| `staging_dir` | `None` | Local directory for the CSV files; required when enabled |
| `s3_staging_name` | `None` | Name of a `bindings.staging_proxy` entry; its `conn_proxy` label finds the S3 credentials |
| `s3_conn_proxy` | `None` | The `conn_proxy` label itself; takes precedence over `s3_staging_name` |
| `s3_bucket` | `None` | Bucket for the upload; falls back to the bucket of the registered S3 config |
| `s3_key_prefix` | `graflo-bulk` | Key prefix for uploaded files; the session id is appended |
| `separator` | `,` | CSV field separator, also passed to the loading job |
| `include_header` | `True` | Write a header row, and tell the loading job it is there |
| `quote_char` | `"` | CSV quote character used when writing the files |
| `line_terminator` | `\n` | CSV line terminator used when writing the files |
| `loading_job` | see below | Options of the TigerGraph loading job |

`loading_job` takes:

| Option | Default | Meaning |
|---|---|---|
| `concurrency` | `4` | `CONCURRENCY` of `RUN LOADING JOB` |
| `batch_size` | `50000` | `BATCH_SIZE` of `RUN LOADING JOB` |
| `job_name_prefix` | `graflo_bulk` | The job is named `<prefix>_<session id>` |
| `drop_job_after_run` | `True` | Drop the job, and the S3 data source, after a successful run |
| `run_mode` | `create_and_run` | `run_only` runs an existing job instead of creating one |

Leave `run_mode` at `create_and_run`. The job name contains the session id,
which is new on every run, so a job for `run_only` to find does not exist.

## Emulating S3 in development

GraFlo uploads through boto3, so any service that speaks the S3 API works when
its address is the `endpoint_url`:

- MinIO, as in the container under `docker/minio` that
  `MinioConfig.from_docker_env()` reads. TigerGraph can load from it through
  the data source GraFlo creates.
- LocalStack, which emulates S3 among other cloud services.
- moto, which replaces boto3's calls inside one Python process. It can test
  the upload, but TigerGraph cannot read from it, so no loading job can.

## Limits

- A manifest with blank vertices (`blank: true`) cannot be bulk loaded:
  starting the session raises `ValueError`.
- A resource with `extra_weights` cannot be bulk loaded, because those need
  lookups in the database during ingestion: writing its first batch raises
  `ValueError`. Use the default record-by-record path for it.
- If the staging label has no S3 config on the connection provider, nothing
  is uploaded and the loading job is given the local paths, without an error.
- Batches are written one after another in a bulk session, so ingestion does
  not overlap casting and writing as it does on the default path.

## What to read next

- [Object storage](../concepts/operations/object_storage.md): how bucket credentials reach GraFlo, and the helpers.
- [Bulk load into TigerGraph (13)](../examples/tigergraph-bulk-s3/index.md): a runnable example with MinIO.
- [Parallelism](../concepts/ingestion/parallelism.md): what runs concurrently during ingestion, and what bulk load changes.
