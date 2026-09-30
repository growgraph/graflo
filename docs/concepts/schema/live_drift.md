# Live schema drift

A graph database can hold things its manifest does not declare: a property set
by hand, data from a second loader, or the leftovers of a migration that
stopped halfway. This page shows how to find those differences by reading the
database's structure and comparing it with the schema it is supposed to follow.
Use it before a migration or a new load, or when you have a database but not
the manifest that was deployed to it.

```python
from suthing import FileHandle

from graflo import GraphEngine, GraphManifest
from graflo.connections import Neo4jConfig

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
neo4j_config = Neo4jConfig(
    uri="bolt://localhost:7687", username="neo4j", password="..."
)
drift = GraphEngine().diff_live_schema(neo4j_config, manifest.graph_schema)
if drift.has_drift:
    print(drift.undeclared_properties)  # {"machine": ["model_series"]}
```

`diff_live_schema` works on every backend that can describe its own structure
(`supports_schema_introspection`); on any other, it raises before touching the
database. It only reads.

## What is compared

Only presence: which vertex types, edge types and property names exist on one
side and not the other.

| Field | Meaning |
|---|---|
| `undeclared_vertices` | Labels or collections in the database that the schema does not declare |
| `missing_vertices` | Declared vertex types with no node in the database |
| `undeclared_properties` | Per declared vertex type, properties found on nodes but not declared |
| `missing_properties` | Per declared vertex type, declared properties no examined node carries |
| `undeclared_edges` / `missing_edges` | `(source, relation, target)` patterns on one side only |
| `sampled` | Whether the backend examined a sample of rows rather than a full catalog |

`has_drift` is true when the database holds something the schema does not
declare. Missing entries do not count: a declared type with no data yet is not
drift.

Names are the schema's logical names wherever it maps them, so a vertex type
stored under a different label is still reported under its logical name.
Anything undeclared keeps its database name, because it has no other.

## What is not compared, and why

Property types, identities and indexes are left out, because introspection
cannot report them reliably:

- the Cypher backends (Neo4j, Memgraph, FalkorDB) return property names
  without types;
- identity is guessed from property names, since most databases do not record
  which properties identify a node;
- secondary indexes are not read back.

Comparing them would report differences that the introspection made up. To
compare two declared schemas, with a risk rating for each difference, use
`graflo migrate-schema plan` or `SchemaDiff` instead.

## Sampling

Most backends introspect by sampling a bounded number of rows per type
(`sample_limit`, default 100). A property carried by only a few nodes can be
missed, and a declared property absent from the sample is reported as missing
although some node outside the sample may carry it. `sampled` tells you which
case applies; TigerGraph and the PostgreSQL target read a full catalog and
report `sampled=False`.

## What to read next

- [Schema migration](../operations/migration_and_practices.md): plan the
  database changes between two schemas.
- [Manifest evolution](manifest_evolution.md): change the manifest to declare
  what the database holds.
