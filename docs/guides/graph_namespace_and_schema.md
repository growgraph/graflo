# Graph namespace and schema

GraFlo separates three operations when targeting a graph database:

1. **Create namespace** — graph / database / space / PostgreSQL schema / output directory
2. **Define schema** — vertex and edge types, collections, tables, indexes
3. **Ingest** — write data

## Default flow (GraFlo bootstrap)

By default, `GraphEngine.define_schema()` runs both op 1 and op 2:

```python
from graflo import GraphEngine, GraphManifest
from graflo.db import DBConfig

engine = GraphEngine()
manifest = GraphManifest.model_validate(...)
target = DBConfig.from_dict(...)

engine.define_schema(manifest, target)  # create_namespace=True (default)
engine.ingest(manifest, target)
```

Or in one step:

```python
engine.define_and_ingest(manifest, target)
```

`Connection.init_db()` remains a convenience wrapper that calls `ensure_target_namespace` then `apply_target_schema`.

## Least-privilege flow (pre-provisioned namespace)

When administrators create the graph/database/space and GraFlo should only run in-namespace DDL:

```python
# Admin creates empty TigerGraph graph (or Arango database, Nebula space, etc.)
engine.define_schema(
    manifest,
    target,
    create_namespace=False,
)
engine.ingest(manifest, target)
```

Op 1 only, explicitly:

```python
engine.create_target_namespace(manifest, target)
engine.define_schema(manifest, target, create_namespace=False)
```

CLI:

```bash
uv run ingest ... --init-only --no-create-namespace
```

Server/API: set `"create_namespace": false` on define or ingest request bodies.

## Errors

| Exception | Meaning |
|---|---|
| `NamespaceNotFoundError` | `create_namespace=False` but the target namespace does not exist |
| `SchemaExistsError` | Schema artifacts already present and `recreate_schema=False` |

An **empty** namespace shell (e.g. empty TigerGraph graph with no vertex types) is not a schema collision — GraFlo proceeds to define types.

## TigerGraph notes

- **Default:** GraFlo creates an empty graph if missing, then runs local `SCHEMA_CHANGE` jobs.
- **`create_namespace=False`:** graph must exist; GraFlo runs DDL only. `recreate_schema=True` drops global types attached to the graph but does **not** `DROP GRAPH`.
- **`recreate_schema=True` + `create_namespace=True`:** full teardown (drop graph, orphan global types, recreate graph, redefine schema).
- Vertex/edge types are **global** on the server; recreating types can affect other graphs sharing those type names.

## Required privileges (summary)

| Profile | Typical grants |
|---|---|
| Bootstrap (default) | `CREATE GRAPH` / `CREATE DATABASE` / `CREATE SPACE` + schema DDL |
| Least-privilege | `USE` existing namespace + schema DDL only |
