# Guides

A guide takes you through one task from start to finish: what you need, the
steps, and what you should see at the end. Find your task in the table below.
If you are new to GraFlo, start with the [Quick start](../getting_started/quickstart.md)
and [Creating a manifest](../getting_started/creating_manifest.md); the guides
assume you have a manifest or a graph to work with.

## I want to...

| I want to... | Guide | Runnable example |
|---|---|---|
| Connect GraFlo to my database, with settings from environment variables, code or a YAML file | [Database connections](database_connections.md) | [Credentials outside the manifest (11)](../examples/connection-proxy/index.md) |
| Choose which database, graph or namespace my data is written to, or use one an administrator created | [Graph namespace and schema](graph_namespace_and_schema.md) | — |
| Find which fields identify my records before I write the manifest | [Identity inference](identity_inference.md) | [Find what identifies a record (15)](../examples/identity-inference/index.md) |
| Get a first manifest from a SQL database I already have | [SQL schema inference](sql_schema_inference.md) | [A graph from a PostgreSQL database (09)](../examples/infer-from-postgres/index.md) |
| Change my manifest: rename, add, re-key or remove a type or a property | [Evolving a manifest](evolving_a_manifest.md) | [Version control for a manifest (22)](../examples/version-control/index.md) |
| Read REST API addresses and credentials from environment variables | [API env wiring](api_env_wiring.md) | [API sources from environment variables (12)](../examples/api-env-config/index.md) |
| Load a large graph into TigerGraph quickly | [TigerGraph bulk load](tigergraph_bulk_load.md) | [Bulk load into TigerGraph (13)](../examples/tigergraph-bulk-s3/index.md) |
| Move a graph from one database to another | [Graph DB migration](graph_db_migration.md) | [A graph on disk, without a database (14)](../examples/file-backend-export/index.md) |
| Save a graph to files and load it back | [Graph export and replay](graph_export_and_replay.md) | [A graph on disk, without a database (14)](../examples/file-backend-export/index.md) |
| Import only the parts of GraFlo I use, and know which layer my code belongs to | [Importing and layering](importing.md) | — |
| Add support for a new database | [Adding a database backend](adding_a_backend.md) | — |

## Tasks without a guide

These tasks are covered by a concept page or an example instead.

| I want to... | Read |
|---|---|
| Ingest messages from Kafka topics | [Kafka connector](../concepts/connectors/kafka_connector.md) |
| Look at a source before writing a manifest | [Sampling and profiling](../concepts/schema/sampling_and_profiling.md) |
| Link records that carry only an alternative identifier, such as a serial number | [Vertex identity](../concepts/schema/vertex_identity.md#a-source-that-knows-only-an-alternative-identifier), [Link by an alternative identifier (16)](../examples/secondary-identities/index.md) |
| Change the schema of a database that already holds data | [Schema migration](../concepts/operations/migration_and_practices.md) |
| Keep the history of a manifest and merge branches | [Version control](../concepts/schema/versioning.md), [Version control for a manifest (22)](../examples/version-control/index.md) |
| Build a graph from an ontology and RDF data | [A graph from an ontology and RDF data (10)](../examples/infer-from-rdf/index.md) |

## What to read next

- [Examples](../examples/index.md): runnable scripts, one question each.
- [Concepts overview](../concepts/index.md): the manifest and how ingestion works.
- [Glossary](../concepts/glossary.md): the terms used across these pages.
