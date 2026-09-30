# Examples

Each example answers one question with a small data set, a manifest and a
script you can run. Find the question closest to yours, read its page, then run
it from its directory under
[`examples/`](https://github.com/growgraph/graflo/tree/main/examples) in a
clone of the repository. The repository has a Docker Compose setup under
`docker/` for every database these examples use; see
[`docker/README.md`](https://github.com/growgraph/graflo/blob/main/docker/README.md).

## Basic ingestion

| No. | Question | Needs to run |
|---|---|---|
| 01 | [How do I ingest CSV files into a graph?](csv-two-resources/index.md) | ArangoDB |
| 02 | [How do I link records that refer to records of the same kind?](json-self-edges/index.md) | ArangoDB |
| 03 | [How do I keep several different relations between the same two things?](csv-relation-field/index.md) | ArangoDB |
| 04 | [How do I turn nested JSON into a graph when the key names say what the relation is?](json-relation-from-key/index.md) | ArangoDB |

## Pipeline features

| No. | Question | Needs to run |
|---|---|---|
| 05 | [How do I turn price columns into measurements and skip invalid values?](vertex-filters-and-weights/index.md) | ArangoDB |
| 06 | [How do I ingest a row that mentions the same kind of thing in several roles?](vertex-roles-edge-links/index.md) | ArangoDB |
| 07 | [How do I ingest one table that holds many kinds of things?](vertex-router-type-map/index.md) | ArangoDB |
| 08 | [How do I ingest a relations table where each row names its own types?](vertex-router-flat-rows/index.md) | ArangoDB |

## Schema inference

| No. | Question | Needs to run |
|---|---|---|
| 09 | [How do I get a graph from a PostgreSQL database without writing a schema?](infer-from-postgres/index.md) | PostgreSQL and ArangoDB |
| 10 | [How do I turn an OWL ontology and RDF data into a property graph?](infer-from-rdf/index.md) | ArangoDB; the RDF is read from files |

## Connections

| No. | Question | Needs to run |
|---|---|---|
| 11 | [How do I keep database credentials out of the manifest?](connection-proxy/index.md) | PostgreSQL and ArangoDB |
| 12 | [How do I configure several API sources from environment variables?](api-env-config/index.md) | Environment variables only; no API or database is contacted |
| 13 | [How do I load a large graph into TigerGraph quickly?](tigergraph-bulk-s3/index.md) | TigerGraph and MinIO |

## File backend

| No. | Question | Needs to run |
|---|---|---|
| 14 | [How do I try GraFlo without a database, and export a graph to files?](file-backend-export/index.md) | Nothing to write the graph to disk; Neo4j to export from, ArangoDB to move the graph into |

## Identity

| No. | Question | Needs to run |
|---|---|---|
| 15 | [My data has no obvious key. How do I find out what identifies a record?](identity-inference/index.md) | Nothing; the graph is written to disk |
| 16 | [How do I link to a record when the source only knows its alternative identifier?](secondary-identities/index.md) | Nothing; the graph is written to disk |
| 17 | [Records arrive with different identifiers filled in. How do I still get one vertex per thing?](identity-funnel/index.md) | Nothing; the graph is written to disk |
| 18 | [Two systems describe the same customers. How do I find the columns that match them?](cross-resource-identity/index.md) | Nothing |

## Evolution

[Evolving a manifest](../guides/evolving_a_manifest.md) walks through changing
one manifest and recording the change; examples 19 and 22 go further, with
both directions of a relation and with two histories to reconcile.
[Merging manifests](../concepts/schema/merging_manifests.md) explains the
union behind examples 20 and 21.

| No. | Question | Needs to run |
|---|---|---|
| 19 | [How do I get both directions of a relation without declaring every edge twice?](edge-inverses/index.md) | Nothing |
| 20 | [Two teams modeled the same things under different names. How do I combine their manifests?](manifest-union/index.md) | Nothing |
| 21 | [How do I combine manifests when one source decides the type per row?](router-union-alignment/index.md) | Nothing |
| 22 | [Two people changed the same manifest. How do I merge their changes and keep the history?](version-control/index.md) | Nothing |

## Profiles

| No. | Question | Needs to run |
|---|---|---|
| 23 | [How do I turn a plain schema into one that tracks state and measurements over time?](state-core-lift/index.md) | Nothing |

## What to read next

- [Quick start](../getting_started/quickstart.md): example 01, step by step.
- [Database connections](../guides/database_connections.md): how to run an
  example against your own database instead of a container.
- [Creating a manifest](../getting_started/creating_manifest.md): the three
  blocks every example's manifest is made of.
