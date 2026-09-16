# Graph namespace and schema

GraFlo separates three operations when targeting a graph database:

1. **Create namespace** — graph / database / space / PostgreSQL schema / output directory
2. **Define schema** — vertex and edge types, collections, tables, indexes
3. **Ingest** — write data

## Which namespace

`schema.metadata.name` is a label: merges fold it into `left+right`, and it is
excluded from the content hash. The namespace a schema deploys into is resolved
by `Schema.effective_namespace(db_flavor)`, in this order:

1. the call argument (`graph_target_namespace`) or the connection config's own
   `database` / `schema_name`, when set;
2. `db_profile.target_namespace` — validated against the flavor, and refused
   with a suggested spelling rather than rewritten;
3. `metadata.name`, with whatever the flavor rejects rewritten.

| Flavor | Derived form | `cmdb+discovery` |
|---|---|---|
| ArangoDB | letters, digits, `_`, `-`; leading letter; ≤ 64 | `cmdb_discovery` |
| Neo4j | lowercase letters, digits, `-`; leading letter; 3–63; not `system…` | `cmdb-discovery` |
| TigerGraph | letters, digits, `_`; reserved words and `gsql_sys_` escaped | `cmdb_discovery` |
| Nebula | letters, digits, `_` | `cmdb_discovery` |
| FalkorDB, Memgraph | letters, digits, `_`, `-` | `cmdb_discovery` |

A name the flavor already accepts is left as is. The derived name is not written
back onto `db_profile`, so renaming a schema does not change its content hash.
To pin a namespace — or to keep the one an existing deployment already uses —
set `db_profile.target_namespace`, or `target_namespace` on the
`MergeManifestsOp` that produces the merged manifest.

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
