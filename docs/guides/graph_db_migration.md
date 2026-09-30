# Graph DB migration

You have a graph in one database and want the same graph in another: Neo4j to
ArangoDB, ArangoDB to TigerGraph, a graph database to PostgreSQL tables. This
guide moves it with one call and no manifest: GraFlo reads the schema and the
data from the source, adapts the names to the target and writes them. After
this guide you will have the graph in the target and know how to check it.

To save a graph to disk and load it later, read
[Graph export and replay](graph_export_and_replay.md) instead. For what the
operations do and which backends support them, read
[Graph export and migration](../concepts/operations/graph_export_migration.md).

## What you need

- GraFlo installed (`pip install graflo`). The drivers for every backend come
  with it.
- A source that GraFlo can read as a graph: Neo4j, ArangoDB, PostgreSQL
  holding a graph that GraFlo wrote, or a GraFlo file backend.
- A target: any of the eight backends (ArangoDB, Neo4j, TigerGraph, FalkorDB,
  Memgraph, NebulaGraph, PostgreSQL, the file backend).
- Connection settings for both. Each config class loads them with
  `from_env()` (environment variables) or `from_docker_env()` (the container
  settings shipped under `docker/`); see
  [Database connections](database_connections.md).

## Steps

### 1. Describe the two databases

```python
from graflo import DBType, GraphEngine
from graflo.connections import ArangoConfig, Neo4jConfig

source = Neo4jConfig.from_env()
target = ArangoConfig.from_env()
engine = GraphEngine(target_db_flavor=DBType.ARANGO)
```

`migrate_graph` takes the target backend from the target config.
`target_db_flavor` tells the other engine methods which backend the names
should suit, so set it to the same backend.

### 2. Look at the source before moving it

```python
schema = engine.infer_schema_from_graph(source, sample_limit=100)
print([v.name for v in schema.core_schema.vertex_config.vertices])
print([e.edge_id for e in schema.core_schema.edge_config.edges])
```

This prints the vertex types and the edge types that will be moved. Neo4j and
ArangoDB have no schema catalog, so the schema is recovered from
`sample_limit` records per type. If a type has a property that the sample
missed, raise `sample_limit`. PostgreSQL and the file backend read a catalog
and do not sample.

### 3. Move the graph

```python
engine.migrate_graph(
    source,
    target,
    recreate_schema=True,
    clear_data=False,
    sample_limit=100,
)
```

This reads the schema, reads every vertex and edge into memory, creates the
namespace and the types on the target, and writes. `recreate_schema=True`
drops types that already exist on the target. With `recreate_schema=False`, a
target that already holds a graph raises `SchemaExistsError`.

For a trial run on a large graph, add `data_limit=1000`: at most that many
records per vertex type and per edge type are read.

### 4. Check the target

```python
moved = engine.infer_schema_from_graph(target)
print([v.name for v in moved.core_schema.vertex_config.vertices])
```

This works on every backend. If the source had a type that the target cannot
store under its own name, you see the name the target stores it under.

## What you should see

The target holds one vertex type per source vertex type and one edge type per
source edge type, with the same records. On a PostgreSQL target the vertex
types are tables, and each edge type is a table named
`{source}_{target}_{relation}_edges` with `source_id` and `target_id` columns.

## Options

| Argument | Default | Effect |
|---|---|---|
| `recreate_schema` | `True` | Drop the target's existing types before defining them |
| `clear_data` | `False` | Delete existing records on the target before writing |
| `create_namespace` | `True` | Create the target database, graph or space if missing; `False` requires it to exist |
| `graph_target_namespace` | `None` | Name of the target database, graph or space; see [Graph namespace and schema](graph_namespace_and_schema.md) |
| `sample_limit` | `100` | Records sampled per type when the source has no schema catalog |
| `data_limit` | `None` | Cap on records read per vertex type and per edge type |

## Other targets

Only the target config changes, and the engine's flavor with it:

```python
from graflo.connections import PostgresConfig, TigergraphConfig

GraphEngine(target_db_flavor=DBType.TIGERGRAPH).migrate_graph(
    Neo4jConfig.from_env(), TigergraphConfig.from_env(), recreate_schema=True
)
GraphEngine(target_db_flavor=DBType.POSTGRES).migrate_graph(
    ArangoConfig.from_env(), PostgresConfig.from_env(), recreate_schema=True
)
```

A source that GraFlo cannot read as a graph (TigerGraph, FalkorDB, Memgraph,
NebulaGraph) raises `ValueError` naming the backends that can be read. Build
a file backend from the original data instead and migrate from that; see
[Graph export and replay](graph_export_and_replay.md).

## What to read next

- [Graph export and migration](../concepts/operations/graph_export_migration.md): what each operation does and its limits.
- [Graph export and replay](graph_export_and_replay.md): keep a copy on disk between source and target.
- [A graph on disk, without a database (14)](../examples/file-backend-export/index.md): runnable scripts.
