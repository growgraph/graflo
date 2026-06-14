# Graph export and migration

GraFlo 1.8.6 extends the runtime beyond **non-graph → graph** ingestion to support **graph databases as sources**, a typed export artifact (**`GraFloOutput`**), and **PostgreSQL as a graph target**.

## Overview

| Direction | Entry point | Source backends | Target backends |
|---|---|---|---|
| Graph → typed artifact | `GraphEngine.export_graph()` | Neo4j, ArangoDB | — (in-memory `GraFloOutput`) |
| Graph → graph | `GraphEngine.migrate_graph()` | Neo4j, ArangoDB | Any supported LPG target |
| Graph → relational | `GraphEngine.migrate_graph()` | Neo4j, ArangoDB | PostgreSQL (vertex + junction edge tables) |
| Graph schema only | `GraphEngine.infer_schema_from_graph()` | Neo4j, ArangoDB | — (returns `Schema`) |

PostgreSQL remains a **source** for 3NF schema inference and SQL ingestion ([Example 5](../examples/example-5.md)). As a **target**, PostgreSQL stores the logical graph as relational tables rather than native LPG structures.

## Quick start

The three common workflows — export, graph→graph migration, and graph→PostgreSQL — share the same connection configs and differ only in which `GraphEngine` method you call:

```python
from graflo import GraphEngine, DBType
from graflo.architecture.schema import GraFloOutput
from graflo.db import Neo4jConfig, ArangoConfig, PostgresConfig

# Target flavor drives schema sanitization (reserved words, storage names, indexes)
engine = GraphEngine(target_db_flavor=DBType.ARANGO)

neo4j = Neo4jConfig.from_env()       # or Neo4jConfig.from_docker_env()
arango = ArangoConfig.from_env()
postgres = PostgresConfig.from_env()

# --- Export typed output (schema + GraphContainer) ---
output = engine.export_graph(
    neo4j,
    sample_limit=100,   # property sampling during schema inference
    data_limit=None,    # optional cap on rows per vertex/edge type
)
output.to_yaml("export.yaml")

# Reload later (YAML key is "schema", not "graph_schema")
restored = GraFloOutput.from_yaml("export.yaml")
assert restored.data.vertices == output.data.vertices

# --- Neo4j → Arango migration ---
engine.migrate_graph(
    neo4j,
    arango,
    recreate_schema=True,   # drop/recreate target collections or labels
    clear_data=False,
)

# --- Neo4j → Postgres (relational vertex + junction edge tables) ---
pg_engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
pg_engine.migrate_graph(
    neo4j,
    postgres,
    recreate_schema=True,
)
```

**Config loading.** All `*Config` classes support `from_env()`, `from_docker_env()` (reads `docker/<backend>/.env`), or direct constructor arguments. Set the usual environment variables (`NEO4J_URI`, `ARANGO_URI`, `POSTGRES_HOST`, …) before running.

**When to set `target_db_flavor`.** Use the flavor of the database you are writing *to*: Arango when exporting for an Arango-shaped schema, PostgreSQL when migrating into relational tables. The source is always passed as the first argument to `export_graph` / `migrate_graph`.

See [Example 13: Graph export and migration](../examples/example-13.md) for a runnable script and step-by-step walkthrough.

## GraFloOutput

**`GraFloOutput`** pairs a full **`Schema`** with a **`GraphContainer`** so exports are self-describing and round-trip through JSON or YAML:

```python
from graflo.architecture.schema import GraFloOutput
from graflo.hq import GraphEngine
from graflo.db.connection import Neo4jConfig

engine = GraphEngine()
output = engine.export_graph(Neo4jConfig(...))

# Python: output.graph_schema (alias: output.schema)
# JSON/YAML key: "schema"
assert output.core_schema is output.graph_schema.core_schema
assert output.data.vertices  # GraphContainer
```

Serialize for interchange:

```python
payload = output.model_dump(mode="json")
restored = GraFloOutput.model_validate(payload)
```

## GraphContainer edge keys in JSON

In Python, edge keys are tuples `(source, target, relation)`. When serialized to JSON, each key becomes a compact JSON array string, for example `["person","department","works_in"]`. This avoids ambiguity when vertex or relation names contain `|` or other delimiter characters.

```python
from graflo.architecture.graph_types import GraphContainer

gc = GraphContainer.model_validate({
    "vertices": {"person": [{"id": "1"}]},
    "edges": {'["person","person","knows"]': [[{"id": "1"}, {"id": "2"}, {}]]},
})
```

## Export from a graph database

**`GraphEngine.export_graph()`** introspects the source schema, fetches all vertex documents and edge rows, and returns **`GraFloOutput`**.

```python
from graflo.hq import GraphEngine
from graflo.db.connection import Neo4jConfig, ArangoConfig

engine = GraphEngine(target_db_flavor=ArangoConfig.connection_type)

# Full export (schema + data)
output = engine.export_graph(
    Neo4jConfig(...),
    sample_limit=100,   # property sampling during schema inference
    data_limit=None,    # optional cap on fetched rows per vertex/edge type
)

# Schema only
schema = engine.infer_schema_from_graph(Neo4jConfig(...))
```

Supported **source** backends expose **`supports_graph_export = True`** on their `Connection` class. Use **`ConnectionManager.graph_export_flavors()`** to list them (currently **Neo4j** and **ArangoDB**).

Low-level connection API (when not using `GraphEngine`):

- **`ConnectionManager.open_graph_connection(config)`** — opens a graph-capable connection without target-only validation.
- **`conn.introspect_graph_schema(schema_name=..., sample_limit=...)`** — returns a graflo **`Schema`** (not to be confused with PostgreSQL **`introspect_schema()`**, which returns SQL 3NF inference results).
- **`conn.fetch_all_docs(vertex_name, limit=...)`** and **`conn.fetch_all_edges(source, target, relation, limit=...)`** — bulk data export.

Shared inference logic lives in **`graflo.db.graph_introspection`** (`GraphSchemaInferencer`).

## Migrate graph → graph

**`GraphEngine.migrate_graph()`** exports from the source in one connection pass, sanitizes the schema for the target flavor, defines DDL, and loads data:

```python
from graflo.hq import GraphEngine
from graflo.db.connection import Neo4jConfig, ArangoConfig

engine = GraphEngine(target_db_flavor=ArangoConfig.connection_type)
engine.migrate_graph(
    Neo4jConfig(...),          # source
    ArangoConfig(...),         # target
    recreate_schema=True,      # drop/recreate target collections or labels
    clear_data=False,
    sample_limit=100,
)
```

The engine reuses a single source connection for introspection and export, and applies target **`Sanitizer`** rules once before writing.

## Migrate graph → PostgreSQL

PostgreSQL targets map each vertex type to a table and each edge type to a junction table named `{source}_{target}_{relation}_edges` with `source_id`, `target_id`, optional weight columns, and a surrogate primary key for parallel edges.

```python
from graflo.hq import GraphEngine
from graflo.db.connection import Neo4jConfig, PostgresConfig
from graflo.onto import DBType

engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
engine.migrate_graph(
    Neo4jConfig(...),
    PostgresConfig(...),
    recreate_schema=True,
)
```

Vertex upserts use identity fields from the logical schema. Edge tables use foreign keys to vertex tables when the referenced primary keys exist in the target schema.

## Capability guard

**`ConnectionManager.open_graph_connection()`** rejects backends without graph export support:

```python
from graflo.db.manager import ConnectionManager

ConnectionManager.graph_export_flavors()  # e.g. [DBType.NEO4J, DBType.ARANGO]
```

TigerGraph, FalkorDB, Memgraph, and NebulaGraph remain supported **targets** for manifest-driven ingestion; graph-source introspection/export is not yet implemented for those backends.

## Related topics

- [Core components](core_components.md) — `Schema`, `GraphContainer`, `GraphEngine`
- [Features and practices](features_and_practices.md) — schema inference from PostgreSQL and RDF
- [Example 5](../examples/example-5.md) — PostgreSQL as a **source** (3NF inference)
- [Example 13](../examples/example-13.md) — graph export and migration walkthrough
- [PostgreSQL reference](../reference/db/postgres/__init__.md) — connection and SQL-side introspection
