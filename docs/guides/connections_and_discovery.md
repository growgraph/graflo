# Connections and discovery

A connection says how to reach something: a database, a REST API, a Kafka
cluster, or a place files live. Every kind is one config class with a `kind`
field, so a stored connection always reads back as the same class, and what it
can be used for follows from the config alone. This guide shows the kinds, how
to store and read them, and how discovery turns a connection into connectors
you can sample and ingest.

## The kinds

| `kind` | Class | Read from | Written to | Discovery lists |
|---|---|---|---|---|
| `database` | `DBConfig` subclass, by `db_type` | `postgres`, `sparql`, `graphdb` | `postgres` and the graph databases | tables (`TableConnector`), or classes with instances (`SparqlConnector`) |
| `api` | `RestApiConnConfig` | yes | no | nothing: endpoints come from the models that bind them |
| `stream` | `KafkaConnConfig` | yes | no | topics (`KafkaConnector`), internal `__` topics left out |
| `files` | `FilesConnConfig` | yes | no | files whose extension says how to read them, a numbered run as one (`FileConnector`) |

`roles(conn)` returns `source`, `target` or both. A graph database is only
written to; discovery refuses it.

A files connection's `root` is an absolute local directory or
`s3://<bucket>[/<prefix>]`. An S3 root also takes `endpoint_url` and `region`
for an S3-compatible store. A `FileConnector` wired to the connection reads its
`sub_path` relative to the root, for sampling and ingestion alike.

## Storing and reading a connection

`parse_connection(data)` reads any stored connection; `kind` is required, and a
database also needs `db_type`. `model_dump()` writes both back.

```python
from graflo.connections import FilesConnConfig, parse_connection

lake = FilesConnConfig(root="s3://landing/raw", endpoint_url="http://minio:9000")
stored = lake.model_dump(mode="json", exclude_none=True)
assert parse_connection(stored) == lake
```

`CONNECTION_ADAPTER` is the pydantic adapter of the whole union, for a caller
that validates or publishes its JSON schema.

## Secrets

A stored connection holds no secret. `resolve_secrets_from_env(conn, proxy)`
fills what it lacks from `{PREFIX}<NAME>`, where the prefix is the connection's
`conn_proxy` label in upper case with `-` turned into `_`:

| Kind | Variables |
|---|---|
| database, SPARQL | `USERNAME`, `PASSWORD` |
| REST API | `TOKEN`, `USERNAME`, `PASSWORD` |
| Kafka | `SASL_USERNAME`, `SASL_PASSWORD` |
| files on S3 | `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` |

A value already on the config is kept.

## Discovery

```python
from graflo import GraphEngine
from graflo.connections import InMemoryConnectionProvider

engine = GraphEngine()
found = engine.discover(lake)
for resource in found.resources:
    print(resource.name, resource.size_bytes)

bindings = found.bindings("lake", ["orders_2026"])
sample = engine.sample_resources(
    bindings, connection_provider=InMemoryConnectionProvider({"lake": lake})
)
```

Each `DiscoveredResource` holds a resource name, unique within the discovery,
and the connector that reads it. A table also carries its columns, keys,
comment and row estimate; a SPARQL class its instance count; files their
count and total size. `limit` caps how many are listed, and `truncated` says
more exist.

Files are candidates when their extension says how to read them: `.json`,
`.jsonl`, `.csv`, `.tsv` and `.parquet`, compressed or not. Files in
one directory whose names differ only in runs of digits are one candidate:
`part-0001.csv` and `part-0002.csv` become a connector with
`regex: ^part\-\d+\.csv$`, so files that arrive later are read too. A file
alone keeps a regex of its exact name. To read other files, write the
`FileConnector` yourself, with any `regex`, and `file_type` where the name does
not show the format; `graflo.data_source.files.list_files(files, connector)`
lists what it matches.

`Discovery.bindings(conn_proxy, names)` turns a pick into a bindings block:
one resource per name, read by its connector, wired to `conn_proxy`. Sample it
with `sample_resources`, or put it in a manifest and ingest it.

## Rules

- The bindings' default `conn_proxy` never reaches a file or push connector. A
  file connector reads through a files connection only when
  `connector_connection` wires it to one; otherwise it reads its local
  `sub_path`.
- A SPARQL connector without `endpoint_url` reads the endpoint of the SPARQL
  connection its `conn_proxy` names.
- S3 staging for a TigerGraph bulk load is a files connection with an `s3://`
  root; its prefix comes before `s3_key_prefix`.
- A Kafka connector found by discovery reads in the consumer group
  `graflo-<name>`; change `group_id` before ingesting if another name is
  needed.
