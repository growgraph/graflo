# Graph export and replay

You want a copy of a graph on disk: to keep it, to move it between machines,
or to load it into several databases from one export. GraFlo writes a graph to
a directory of compressed chunks called a file backend, and reads that
directory back as if it were a database. This guide saves a graph to files,
looks at what was written, and loads it back. No database is needed until the
last step.

To move a graph straight from one database to another, read
[Graph DB migration](graph_db_migration.md). For the layout of the directory
and the limits of export, read
[Graph export and migration](../concepts/operations/graph_export_migration.md).

## What you need

- GraFlo installed (`pip install graflo`).
- A graph to export: Neo4j, ArangoDB, PostgreSQL holding a graph that GraFlo
  wrote, or another file backend. If you have source files and a manifest
  instead, go to [step 4](#4-build-the-files-from-source-data-instead).
- A directory to write to.

## Steps

### 1. Save the graph to disk

A file backend is a target like any other, so saving is a migration whose
target is a `GraFloBackendConfig`:

```python
from pathlib import Path

from graflo import DBType, GraphEngine
from graflo.connections import GraFloBackendConfig, Neo4jConfig

backend = GraFloBackendConfig(output_dir=Path("artifacts/plant-graph"))
engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)
engine.migrate_graph(Neo4jConfig.from_env(), backend, recreate_schema=True)
```

This reads the schema and every record from Neo4j and writes `schema.yaml`,
`INDEX.json` and the chunk files under `vertices/` and `edges/`.

`export_graph` is a different call for a different task: it returns the
schema and the data as Python objects and writes nothing.

```python
output = engine.export_graph(Neo4jConfig.from_env())
output.graph_schema  # Schema
output.data  # GraphContainer: vertices and edges in memory
```

Use it when you want to inspect or transform the graph in code. Use
`migrate_graph` with a file-backend target when you want files.

### 2. Look at what was written

```python
from graflo.architecture.backend import GraFloBackendReader

reader = GraFloBackendReader(Path("artifacts/plant-graph"))
index = reader.read_index()
for name, entry in index.vertices.items():
    print(name, entry.record_count, entry.chunks)
for name, entry in index.edges.items():
    print(name, entry.record_count, entry.chunks)
```

This prints each vertex type and edge type with its record count and chunk
files. `read_schema()` returns the `Schema`. `iter_vertex_batches(name)` and
`iter_edge_batches((source, target, relation))` stream records in batches
without loading the whole graph. A vertex record is a JSON object; an edge
record is a list of three objects: the source vertex's identity, the target
vertex's identity, and the edge properties.

### 3. Load it back into a database

The file backend is also a source, so loading is another migration:

```python
from graflo.connections import ArangoConfig, PostgresConfig

GraphEngine(target_db_flavor=DBType.ARANGO).migrate_graph(
    backend, ArangoConfig.from_env(), recreate_schema=True
)
GraphEngine(target_db_flavor=DBType.POSTGRES).migrate_graph(
    backend, PostgresConfig.from_env(), recreate_schema=True
)
```

The same export can be loaded into as many targets as you like. Each load
reads `schema.yaml` and the chunks, adapts the names to the target and
writes.

### 4. Build the files from source data instead

If the graph does not exist yet, ingest the manifest into the file backend
the way you would into a database. Only the target config differs:

```python
from suthing import FileHandle

from graflo import GraphManifest
from graflo.hq import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=GraFloBackendConfig(output_dir=Path("artifacts/plant-graph")),
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

Use this to test a manifest without a database, and to get a graph out of a
source that GraFlo cannot read as a graph: build the files from the original
data, then load them where you want them.

## What you should see

After step 1 or step 4 the directory looks like this:

```text
artifacts/plant-graph/
├── INDEX.json
├── schema.yaml
├── vertices/
│   └── machine.000.jsonl.gz
└── edges/
    └── work_order__services__machine.000.jsonl.gz
```

`INDEX.json` lists every type with its record count and chunk paths. After
step 3 the target database holds the same types and records.

The file backend appends records rather than merging them. After step 4, a
vertex that two resources both produce is stored once per resource, so a
record count can be higher than the number of distinct vertices; the
database merges the copies in step 3. For the same reason, `engine.ingest()`
into a directory that already holds data adds a second copy of every record
unless `IngestionParams(clear_data=True)` is set.

## What to read next

- [Graph export and migration](../concepts/operations/graph_export_migration.md): directory layout, chunk naming, limits.
- [Graph DB migration](graph_db_migration.md): the direct database-to-database move.
- [A graph on disk, without a database (14)](../examples/file-backend-export/index.md): the same steps as runnable scripts.
