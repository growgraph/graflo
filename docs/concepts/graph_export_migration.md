# Graph export and migration

GraFlo 1.8.7 adds **`GraFloBackendConfig`** — a first-class **source** and **target** that persists graphs as a chunked on-disk directory. Use it for dry-runs without a live database, large-graph exports, and replay into any supported LPG or PostgreSQL target.

## Overview

| Direction | Entry point | Source backends | Target backends |
|---|---|---|---|
| Graph → file backend | `GraphEngine.migrate_graph()` | Neo4j, ArangoDB, file backend | `GraFloBackendConfig` |
| File backend → graph | `GraphEngine.migrate_graph()` | `GraFloBackendConfig` | Any supported LPG target |
| File backend → relational | `GraphEngine.migrate_graph()` | `GraFloBackendConfig` | PostgreSQL (vertex + junction edge tables) |
| Resources → file backend | `GraphEngine.ingest()` / `define_and_ingest()` | CSV / JSON / SQL / API manifest resources | `GraFloBackendConfig` |
| Graph schema only | `GraphEngine.infer_schema_from_graph()` | Neo4j, ArangoDB, file backend | — (returns `Schema`) |
| In-memory export | `GraphEngine.export_graph()` | Neo4j, ArangoDB | — (returns `GraFloOutput`) |

PostgreSQL remains a **source** for 3NF schema inference and SQL ingestion ([Example 5](../examples/example-5.md)). As a **target**, PostgreSQL stores the logical graph as relational tables rather than native LPG structures.

## File backend layout

A GraFlo file backend directory is self-describing:

```
artifacts/neo4j-backend/
├── INDEX.json              # manifest: version, schema hash, chunk inventory
├── schema.yaml             # Schema only (no data)
├── vertices/
│   ├── person.000.jsonl.gz
│   └── person.001.jsonl.gz
└── edges/
    └── person__knows__person.000.jsonl.gz
```

- **Chunks** — gzip-compressed JSONL (one JSON document per line)
- **Edges** — filenames use `{source}__{relation}__{target}` when names are safe; `INDEX.json` keys follow the same convention
- **Default chunk size** — 50 000 records per file (configurable on `GraFloBackendConfig`)

## Quick start

```python
from pathlib import Path

from graflo import GraphEngine, DBType
from graflo.db import Neo4jConfig, ArangoConfig, PostgresConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig

neo4j = Neo4jConfig.from_env()       # or Neo4jConfig.from_docker_env()
arango = ArangoConfig.from_env()
postgres = PostgresConfig.from_env()
backend = GraFloBackendConfig(output_dir=Path("artifacts/neo4j-backend"))

engine = GraphEngine(target_db_flavor=DBType.ARANGO)

# --- Neo4j → file backend (export to disk) ---
engine.migrate_graph(neo4j, backend, recreate_schema=True)

# --- File backend → Arango migration ---
engine.migrate_graph(backend, arango, recreate_schema=True)

# --- File backend → Postgres (relational vertex + junction edge tables) ---
pg_engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
pg_engine.migrate_graph(backend, postgres, recreate_schema=True)
```

**Config loading.** All `*Config` classes support `from_env()`, `from_docker_env()` (reads `docker/<backend>/.env`), or direct constructor arguments.

**Pre-sanitize for a future target.** Set `target_flavor_hint` on the file-backend config so `schema.yaml` is sanitized before it is written:

```python
backend = GraFloBackendConfig(
    output_dir=Path("artifacts/for-arango"),
    target_flavor_hint=DBType.ARANGO,
)
engine.migrate_graph(neo4j, backend, recreate_schema=True)
```

See [Example 13: GraFlo file backend](../examples/example-13.md) for a runnable script including **`ingest()`** to disk.

## Ingest resources into a file backend

`GraFloBackendConfig` works as an **`ingest()`** target the same way as ArangoDB or Neo4j — only the config changes:

```python
from pathlib import Path

from suthing import FileHandle

from graflo import GraphEngine, GraphManifest, DBType
from graflo.db.graflo_backend.config import GraFloBackendConfig
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

backend = GraFloBackendConfig(output_dir=Path("artifacts/csv-backend"))
engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=backend,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

Inspect the result without loading everything into memory:

```python
from graflo.architecture.backend import GraFloBackendReader

reader = GraFloBackendReader(Path("artifacts/csv-backend"))
index = reader.read_index()
print(index.vertices)  # record counts and chunk paths per vertex type
```

## GraFloBackendConfig and Connection API

| Type | Role |
|---|---|
| **`GraFloBackendConfig`** | `DBConfig` subclass: `output_dir`, `chunk_size`, optional `target_flavor_hint` |
| **`GraFloBackendConnection`** | `Connection` implementation registered in `ConnectionManager` |
| **`GraFloBackendWriter`** / **`GraFloBackendReader`** | Low-level I/O primitives in `graflo.architecture.backend` |
| **`GraFloIndex`** | Pydantic model for `INDEX.json` |

**Write path (target):** `init_db` → `upsert_docs_batch` / `insert_edges_batch` → `close()` flushes `INDEX.json`.

**Read path (source):** `introspect_graph_schema()` reads `schema.yaml`; `fetch_all_docs` / `fetch_all_edges` stream from gzip JSONL chunks.

Use **`ConnectionManager.graph_export_flavors()`** to list backends with graph export support — includes **`DBType.GRAFLO_BACKEND`** alongside Neo4j and ArangoDB.

## GraFloOutput (in-memory)

**`GraFloOutput`** pairs a full **`Schema`** with a **`GraphContainer`**. **`GraphEngine.export_graph()`** still returns it for small graphs or programmatic use:

```python
from graflo.hq import GraphEngine
from graflo.db.connection import Neo4jConfig

output = engine.export_graph(Neo4jConfig(...))
assert output.core_schema is output.graph_schema.core_schema
assert output.data.vertices
```

For durable, large exports prefer **`migrate_graph(source, GraFloBackendConfig(...))`** instead of holding the full graph in memory or a single YAML file.

## GraphContainer edge keys in JSON

In Python, edge keys are tuples `(source, target, relation)`. When serialized to JSON, each key becomes a compact JSON array string, for example `["person","department","works_in"]`.

## Migrate graph → graph

**`GraphEngine.migrate_graph()`** exports from the source in one connection pass, sanitizes the schema for the target flavor (unless the target is a file backend without `target_flavor_hint`), defines DDL, and loads data:

```python
engine.migrate_graph(
    Neo4jConfig(...),          # source
    ArangoConfig(...),         # target
    recreate_schema=True,
    clear_data=False,
    sample_limit=100,
)
```

The engine reuses a single source connection for introspection and export, and applies target **`Sanitizer`** rules once before writing.

## Migrate graph → PostgreSQL

PostgreSQL targets map each vertex type to a table and each edge type to a junction table named `{source}_{target}_{relation}_edges` with `source_id`, `target_id`, optional weight columns, and a surrogate primary key for parallel edges.

## Capability guard

**`ConnectionManager.open_graph_connection()`** rejects backends without graph export support:

```python
from graflo.db.manager import ConnectionManager

ConnectionManager.graph_export_flavors()
# e.g. [DBType.NEO4J, DBType.ARANGO, DBType.GRAFLO_BACKEND]
```

TigerGraph, FalkorDB, Memgraph, and NebulaGraph remain supported **targets** for manifest-driven ingestion; native graph-source introspection/export is not yet implemented for those live databases (use a file backend as an intermediate store).

## Related topics

- [Core components](core_components.md) — `Schema`, `GraphContainer`, `GraphEngine`
- [Features and practices](features_and_practices.md) — schema inference from PostgreSQL and RDF
- [Example 5](../examples/example-5.md) — PostgreSQL as a **source** (3NF inference)
- [Example 13](../examples/example-13.md) — file backend export, migration, and ingest walkthrough
- [PostgreSQL reference](../reference/db/postgres/__init__.md) — connection and SQL-side introspection
