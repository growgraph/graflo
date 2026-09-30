# Graph namespace and schema

Which database, graph or space will your data be written to, and who creates
it? This guide answers both. It shows how GraFlo chooses the name of the
[namespace](../concepts/glossary.md#namespace) (the ArangoDB or Neo4j
database, the TigerGraph graph, the NebulaGraph space), how to pin that name,
and how to run GraFlo against a namespace that an administrator created, with
no right to create databases yourself.

## What you need

- A manifest and a target config; see
  [Database connections](database_connections.md).
- For the last step: a namespace that already exists, and a database user
  allowed to define types in it.

## Steps

### 1. Find out which namespace GraFlo will use

GraFlo writes to the first name it finds in this order:

1. the name in the connection config: `database`, or `schema_name` for
   TigerGraph and NebulaGraph;
2. the `graph_target_namespace` argument of `define_schema`, `ingest`,
   `define_and_ingest` or `migrate_graph`;
3. `db_profile.target_namespace` in the manifest's `schema` block;
4. the schema's `metadata.name`, with every character the backend rejects
   rewritten.

Names from the second and third source are checked and never rewritten: a
name the backend would reject raises `InvalidNamespaceError`, and the message
suggests a valid spelling. Only `metadata.name` is rewritten, because it is a
label rather than an identifier: a union of two manifests, for example, names
the result `maintenance+sensors`. The rewriting per backend:

| Backend | Rewritten into | `maintenance+sensors` becomes |
|---|---|---|
| ArangoDB | letters, digits, `_`, `-`; starts with a letter; at most 64 characters | `maintenance_sensors` |
| Neo4j | lowercase letters, digits, `-`; starts with a letter; 3 to 63 characters; does not start with `system` | `maintenance-sensors` |
| TigerGraph | letters, digits, `_`; reserved words and the `gsql_sys_` prefix escaped | `maintenance_sensors` |
| NebulaGraph | letters, digits, `_` | `maintenance_sensors` |
| FalkorDB, Memgraph | letters, digits, `_`, `-` | `maintenance_sensors` |

A name the backend already accepts is kept as it is. A name longer than the
ArangoDB or Neo4j limit is cut and ends with a short hash of the full name, so
two long names with the same beginning do not collide.

To see the result before anything is created:

```python
from suthing import FileHandle

from graflo import DBType, GraphManifest

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
print(manifest.require_schema().effective_namespace(DBType.NEO4J))
```

On PostgreSQL the namespace is the SQL schema named by the config's
`schema_name` (default `public`). A file backend writes to its `output_dir`.

### 2. Pin the name

The rewritten `metadata.name` changes when the label changes. To keep a fixed
name, for example the one an existing deployment uses, set it in the
manifest. A name of letters and digits only is valid on every backend:

```yaml
schema:
  metadata:
    name: maintenance+sensors
  db_profile:
    target_namespace: plantnorth
```

`target_namespace` is part of the schema's content hash. The rewritten
`metadata.name` is not stored anywhere, so renaming a schema leaves its hash
unchanged. When two manifests are combined, `MergeManifestsOp` also takes a
`target_namespace` for the result.

For a single run, pass `graph_target_namespace="plantnorth"` to
`define_schema`, `ingest`, `define_and_ingest` or `migrate_graph` instead.

### 3. Let GraFlo create the namespace

By default `define_schema` creates the namespace when it is missing, then
defines the vertex and edge types in it:

```python
from graflo import GraphEngine
from graflo.connections import Neo4jConfig

engine = GraphEngine(target_db_flavor=DBType.NEO4J)
target = Neo4jConfig.from_env()

engine.define_schema(manifest, target)  # create_namespace=True
engine.ingest(manifest, target)
```

`engine.define_and_ingest(manifest, target)` does both in one call.
`engine.create_target_namespace(manifest, target)` creates the namespace only,
without defining types.

### 4. Use a namespace an administrator created

When the database user may define types but not create databases, create the
namespace as an administrator first, then pass `create_namespace=False`:

```python
engine.define_schema(manifest, target, create_namespace=False)
engine.ingest(manifest, target)
```

GraFlo then only checks that the namespace exists and defines the types in
it. The same from the command line:

```bash
graflo ingest --db-config-path db.yaml --schema-path manifest.yaml \
  --init-only --no-create-namespace
```

`--init-only` defines the schema and stops; `--no-create-namespace` applies to
that step.

## What you should see

The namespace from step 1 exists and holds the vertex and edge types of the
manifest. These errors tell you that something does not match:

| Error | Meaning |
|---|---|
| `NamespaceNotFoundError` | `create_namespace=False`, and the namespace does not exist |
| `SchemaExistsError` | The namespace already holds a schema or data, and `recreate_schema` is `False` |
| `InvalidNamespaceError` | A name given as an argument or in `target_namespace` is not valid for the backend |

An empty namespace, such as a TigerGraph graph with no types, is not an
existing schema: GraFlo defines the types in it.

## TigerGraph

Vertex and edge types in TigerGraph are global to the server, and a graph
lists the types it uses. This changes what `recreate_schema=True` does:

- With `create_namespace=True` (the default), GraFlo drops the graph, drops
  the types of the schema, creates the graph again and defines the types.
- With `create_namespace=False`, GraFlo keeps the graph and drops only the
  types of the schema.

In both cases a type that another graph on the server uses is kept, so that
graph and its installed queries are not affected.

## Required privileges

| How you run it | The database user needs |
|---|---|
| Default (step 3) | The right to create a database, graph or space, and to define types in it |
| Pre-created namespace (step 4) | Access to the existing namespace and the right to define types in it |

## What to read next

- [Database connections](database_connections.md): the config each backend takes.
- [Graph DB migration](graph_db_migration.md): the same options when moving a graph between databases.
- [Merging manifests](../concepts/schema/merging_manifests.md): where a name such as `maintenance+sensors` comes from.
