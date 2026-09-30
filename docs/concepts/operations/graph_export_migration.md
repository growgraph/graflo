# Graph export and migration

You have a labeled property graph in a database and you want it somewhere
else: in another database, on disk, or in memory as Python objects. GraFlo
reads the graph without a [manifest](../glossary.md#manifest), recovers its
schema, and writes it to any backend it supports. This page explains the three
operations behind that, what the file backend is, and where the limits are.
Two guides do the tasks:
[move a graph from one database to another](../../guides/graph_db_migration.md)
and [save a graph to files and load it back](../../guides/graph_export_and_replay.md).

## Three operations

All three are methods of `GraphEngine`. Each takes the connection config of
the database to read.

| You want | Call | Returns |
|---|---|---|
| The schema of an existing graph | `infer_schema_from_graph(source)` | a `Schema` |
| Schema and data in memory | `export_graph(source)` | a `GraFloOutput`: `graph_schema` plus `data`, a `GraphContainer` (the graph held in memory before it is written) |
| Schema and data in another backend | `migrate_graph(source, target)` | nothing; the target holds the graph |

`infer_schema_from_graph` reads the schema and adapts its names to the
engine's `target_db_flavor`. `export_graph` reads the schema, then every
vertex and edge, and keeps the source's names. `migrate_graph` does what `export_graph`
does, then creates the schema on the target and writes the data. None of the
three needs a manifest: the schema comes from the source.

```python
from graflo import DBType, GraphEngine
from graflo.connections import ArangoConfig, Neo4jConfig

engine = GraphEngine(target_db_flavor=DBType.ARANGO)
engine.migrate_graph(
    Neo4jConfig.from_env(),
    ArangoConfig.from_env(),
    recreate_schema=True,
)
```

## Which backends can do what

Reading a schema and reading a whole graph are two different capabilities,
and not every backend has both.

| Backend | Schema can be introspected | Can be read as a graph (source of `export_graph` and `migrate_graph`) | Migration target |
|---|---|---|---|
| ArangoDB | yes | yes | yes |
| Neo4j | yes | yes | yes |
| PostgreSQL | yes | yes, in the table layout GraFlo writes | yes, as tables |
| GraFlo file backend | yes | yes | yes |
| TigerGraph | yes | no | yes |
| FalkorDB | yes | no | yes |
| Memgraph | yes | no | yes |
| NebulaGraph | yes | no | yes |

`infer_schema_from_graph` works on every row. `export_graph` and
`migrate_graph` need a source that can be read as a graph, so a source in the
last four rows raises `ValueError` before anything is read. To move a graph
out of one of those, build a file backend from the data it was built from
(see [the file backend](#the-file-backend)) and migrate from there.

PostgreSQL is a graph source only when its tables have the layout GraFlo
writes to a PostgreSQL target (described [below](#what-a-migration-does)).
To build a graph from an ordinary relational database, infer a manifest from
it instead; see [A graph from a PostgreSQL database (09)](../../examples/infer-from-postgres/index.md).

The lists come from the connection classes, so code can ask:

```python
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager

ConnectionManager.graph_export_flavors()
# [DBType.ARANGO, DBType.NEO4J, DBType.POSTGRES, DBType.GRAFLO_BACKEND]
ConnectionManager.flavors_supporting(ConnectionCapability.SCHEMA_INTROSPECTION)
# all eight
```

## What a migration does

`migrate_graph(source, target)` runs these steps in order:

1. Opens one connection to the source and reads its schema. Neo4j and
   ArangoDB have no schema catalog, so the schema is recovered by sampling
   `sample_limit` records per type (default 100); a property that appears in
   none of the sampled records is missed. PostgreSQL reads its catalog and the
   file backend reads its `schema.yaml`; neither samples.
2. Reads every vertex and every edge into a `GraphContainer`. `data_limit`
   caps the number of records per vertex type and per edge type; use it for a
   trial run.
3. Adapts the schema to the target, which comes from the target config, not
   from the engine. A name the target cannot store, such as a reserved word,
   gets a stored name, and the source name stays the logical one. When the
   target is a file backend, this step runs only if the config carries a
   `target_flavor_hint`.
4. Creates the target namespace (the database, graph or space) unless
   `create_namespace=False`, then the vertex and edge types.
   `recreate_schema` (default `True`) drops existing types first;
   `graph_target_namespace` overrides the namespace name. See
   [Graph namespace and schema](../../guides/graph_namespace_and_schema.md).
5. Removes existing records if `clear_data=True`, then writes vertices and
   edges.

On a PostgreSQL target each vertex type becomes a table. Each edge type
becomes a table named `{source}_{target}_{relation}_edges` (`relates` when the
edge has no relation) with the endpoint columns, one column per edge property,
and an `id` key so that parallel edges can coexist. An endpoint whose vertex
type has one identity field is stored in `source_id` or `target_id`. One with
a composite identity is stored in a column per field, `source__<field>` or
`target__<field>`; edge queries (`graph_neighbors`) refuse such a table,
because they address a vertex by one value.

## The file backend

The file backend is a directory that holds one graph: its schema and its data
in compressed chunks. It appears in the backend table above like any database,
so it can be the target of a migration or an ingest, and the source of another
migration. It needs no database server.

```text
artifacts/plant-graph/
├── INDEX.json                              record counts and chunk paths per type
├── schema.yaml                             the Schema, no data
├── vertices/
│   └── machine.000.jsonl.gz
└── edges/
    └── work_order__services__machine.000.jsonl.gz
```

Chunks are gzip-compressed JSON Lines, one record per line, at most
`chunk_size` records per file (default 50 000). An edge type is named
`{source}__{relation}__{target}`; an edge without a relation leaves the middle
empty (`person____department`). When a name contains characters other than
letters, digits and underscores, `INDEX.json` uses a JSON array of the edge
key instead and the chunk file gets an encoded name. `INDEX.json` also records
the GraFlo version, the creation time and a hash of the schema.

Several writers, in one process or in several, can write one directory at the
same time. Each claims its own chunk files and adds them to `INDEX.json` while
holding a lock on the file `.lock` in the directory.

The config is `GraFloBackendConfig` from `graflo.connections`:

```python
from pathlib import Path

from graflo.connections import GraFloBackendConfig

backend = GraFloBackendConfig(
    output_dir=Path("artifacts/plant-graph"),
    chunk_size=50_000,
)
```

The file backend appends. Every record that reaches it is added to the chunks;
it does not look up and replace a record with the same identity, as a
database does. Two consequences:

- Ingesting the same data twice into one directory stores it twice. Pass
  `IngestionParams(clear_data=True)` or `recreate_schema=True` to start from
  an empty directory.
- When two resources of one manifest produce the same vertex, the directory
  holds one record per resource. A database target merges them when the
  directory is loaded into it.

To read a directory without an engine, use `GraFloBackendReader` from
`graflo.architecture.backend`: `read_index()`, `read_schema()`,
`iter_vertex_batches()` and `iter_edge_batches()` stream the chunks.

### Writing for a known target

`GraFloBackendConfig` also takes `target_flavor_hint`, a `DBType`. With it,
step 3 of the migration runs for that flavor: `schema.yaml` records the stored
names, and chunk files and records use them. A directory written this way
records how the graph would be stored in that database.

Reading such a directory back, with `export_graph`, `migrate_graph` or
`GraFloBackendReader.load_graph_container()`, returns the records under the
names of its `schema.yaml`.

## Limits

- `export_graph` and `migrate_graph` hold the whole graph in memory between
  reading and writing. `data_limit` bounds a trial run; there is no streaming
  move. The file backend writes chunks as it goes, but the read side still
  reads all records first.
- Only ArangoDB, Neo4j, PostgreSQL and the file backend can be read as a
  graph.
- A sampled schema is a lower bound: it lists what the sample showed. Raise
  `sample_limit` when types have rare properties, or check the result of
  `infer_schema_from_graph` before you migrate.
- The file backend appends instead of merging records, as described above.
- `GraFloBackendConfig.from_docker_env()` raises `NotImplementedError`: a
  file backend has no container, so give it an `output_dir`.
- Edge keys are tuples `(source, target, relation)` in Python. In JSON output
  a key becomes a JSON array string such as `["work_order","machine","services"]`.

## What to read next

- [Graph DB migration](../../guides/graph_db_migration.md): move a graph from one database to another.
- [Graph export and replay](../../guides/graph_export_and_replay.md): save a graph to files and load it back.
- [A graph on disk, without a database (14)](../../examples/file-backend-export/index.md): runnable scripts for both.
