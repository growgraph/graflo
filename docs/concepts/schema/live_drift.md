# Live schema drift

A graph database drifts from its schema when something writes to it outside
the declared contract: a property set by hand, a second loader, or a migration
that stopped halfway. `GraphEngine.diff_live_schema` finds this by reading the
live database's structure and comparing it with the schema it is supposed to
follow.

```python
from graflo.hq.graph_engine import GraphEngine

drift = GraphEngine().diff_live_schema(neo4j_config, schema)
if drift.has_drift:
    print(drift.undeclared_properties)   # {"server": ["os_family"]}
```

It works on every backend that can describe its own structure
(`supports_schema_introspection`); on any other, it raises before touching the
database.

## What is compared

Only **presence**: which vertex types, edge types and property names exist on
one side and not the other.

| Field | Meaning |
|---|---|
| `undeclared_vertices` | Labels or collections in the database that the schema does not declare |
| `missing_vertices` | Declared vertex types with no node in the database |
| `undeclared_properties` | Per declared vertex type, properties found on nodes but not declared |
| `missing_properties` | Per declared vertex type, declared properties no examined node carries |
| `undeclared_edges` / `missing_edges` | `(source, relation, target)` patterns on one side only |
| `sampled` | Whether the backend examined a sample of rows rather than a full catalogue |

`has_drift` is true when the database holds something the schema does not
declare. Missing entries do not count: a declared type with no data yet is not
drift.

Names are the schema's logical names wherever it maps them, so a vertex type
stored under a different label is still reported under its logical name.
Anything undeclared keeps its raw database name, because it has no other.

## What is not compared, and why

Property **types**, **identities** and **indexes** are left out. Introspection
cannot report them faithfully:

- the Cypher backends (Neo4j, Memgraph, FalkorDB) return property names
  without types;
- identity is guessed from property names, since most databases do not record
  which properties identify a node;
- secondary indexes are not read back.

Comparing them would report differences the introspection invented. For a
schema-to-schema comparison with risk ratings, use `SchemaDiff` on two declared
schemas instead.

## Sampling

Most backends introspect by sampling a bounded number of rows per type
(`sample_limit`, default 100). A property carried by only a few rare nodes can
be missed, and a declared property absent from the sample is reported as
missing although some unsampled node may carry it. `sampled` tells you which
case applies; TigerGraph and the PostgreSQL target read a full catalogue and
report `sampled=False`.
