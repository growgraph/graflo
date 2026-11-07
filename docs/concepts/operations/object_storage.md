# Object storage (S3-compatible)

GraFlo reads files from S3-compatible object storage (SeaweedFS, AWS S3 or any
service that speaks the S3 API) through a files connection whose root is an
`s3://` URL. This page explains how the bucket credentials reach GraFlo
without being written into the manifest, and what the helpers in
`graflo.object_storage` do.

## The idea

A `FileConnector` is bound to a files connection (`FilesConnConfig`) by a
`conn_proxy` label. The connection's `root` is a local directory or
`s3://<bucket>[/<prefix>]`; the connector reads its `sub_path` relative to that
root, for sampling and ingestion alike. The credentials are registered at run
time on a connection provider under the label, the same way database and API
credentials are (see [connection proxy](../glossary.md#connection-proxy)).

```python
from graflo.connections import InMemoryConnectionProvider
from graflo.object_storage import MinioConfig

s3 = MinioConfig.from_docker_env()
provider = InMemoryConnectionProvider({"lake": s3.to_files_conn_config()})
```

Pass `provider` as `connection_provider` to `GraphEngine.ingest` or
`define_and_ingest`.

## `MinioConfig`

`MinioConfig` (also available as `S3EndpointConfig`) holds the endpoint and
credentials of an S3-compatible service. It is not a database config and is
not used with `ConnectionManager`.

| Field | Default | Meaning |
|---|---|---|
| `endpoint_url` | required | S3 API URL, such as `http://127.0.0.1:8333` |
| `access_key` | required | Access key id |
| `secret_key` | required | Secret access key |
| `bucket` | `graflo-staging` | Default bucket |
| `region` | `us-east-1` | Region passed to boto3; keep the default for a local service |

`MinioConfig.from_docker_env()` reads the settings of the S3 service shipped
under `docker/seaweedfs` (`S3_ENDPOINT` or `S3_PROTOCOL`, `S3_HOSTNAME`,
`S3_API_PORT`; `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `AWS_REGION`).
`to_files_conn_config()` turns it into the files connection (root
`s3://<bucket>`) that a connection provider stores; you can also build a
`FilesConnConfig` directly with `root`, `region`, `endpoint_url`,
`access_key_id` and `secret_access_key`, or read those from `{PREFIX}ROOT`,
`{PREFIX}REGION`, … with `FilesConnConfig.from_env(prefix)`.

`graflo.object_storage` also exports:

| Helper | What it does |
|---|---|
| `ensure_staging_bucket_for_config(config)` | Creates the config's bucket if it does not exist; raises with a hint when the endpoint cannot be reached |
| `ensure_bucket_exists(client, bucket)` | The same for any boto3 client |
| `boto3_s3_client_from_minio(config)`, `boto3_s3_client(config)` | Build a boto3 S3 client from a `MinioConfig` or an `S3ConnConfig` |

## What to read next

- [Connections and discovery](../../guides/connections_and_discovery.md): files connections and how they are stored.
- [Database connections](../../guides/database_connections.md): the other configs a run needs.
