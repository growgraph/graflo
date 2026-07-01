# Migrate graph DB to graph DB

Move an existing labeled property graph from one database to another — no manifest YAML required. GraFlo introspects the source schema and data, sanitizes for the target flavor, and loads in one `migrate_graph()` call.

## Prerequisites

- Python 3.11+
- Source: **Neo4j**, **ArangoDB**, or a **GraFlo file backend** (see [Capability guard](../concepts/operations/graph_export_migration.md#capability-guard))
- Target: any supported output `DBType` — ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, PostgreSQL, or file backend
- Connection configs via `from_env()`, `from_docker_env()`, or constructors

## When to use this

- **Neo4j → ArangoDB** (or any other LPG) — vendor migration without rewriting ETL
- **Production graph → PostgreSQL** — relational vertex + junction edge tables
- **Any graph source → TigerGraph** — target sanitization handles naming and DDL differences

For large graphs or repeated replays, export to a [GraFlo file backend](graph_export_and_replay.md) first, then load from disk.

## Step 1 — Direct graph → graph

```python
from graflo import GraphEngine, DBType
from graflo.db import Neo4jConfig, ArangoConfig

source = Neo4jConfig.from_docker_env()
target = ArangoConfig.from_docker_env()

engine = GraphEngine(target_db_flavor=DBType.ARANGO)
engine.migrate_graph(
    source,
    target,
    recreate_schema=True,
    clear_data=False,
    sample_limit=100,
)
```

`migrate_graph()` introspects the source once, applies target **`Sanitizer`** rules, defines DDL on the target, and writes vertices and edges via **`DBWriter`**.

## Step 2 — Other target flavors

Use the same API; only the target config and `GraphEngine(target_db_flavor=...)` change:

```python
from graflo.db import TigergraphConfig, PostgresConfig

# Neo4j → TigerGraph
tg_engine = GraphEngine(target_db_flavor=DBType.TIGERGRAPH)
tg_engine.migrate_graph(Neo4jConfig.from_env(), TigergraphConfig.from_env(), recreate_schema=True)

# ArangoDB → PostgreSQL (relational graph tables)
pg_engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
pg_engine.migrate_graph(ArangoConfig.from_env(), PostgresConfig.from_env(), recreate_schema=True)
```

## Step 3 — Schema only (no data load)

To inspect or edit the inferred schema before loading:

```python
schema = engine.infer_schema_from_graph(
    Neo4jConfig.from_env(),
    target_db_flavor=DBType.ARANGO,
    sample_limit=100,
)
```

Or export schema + data in memory for small graphs:

```python
output = engine.export_graph(Neo4jConfig.from_env())
# output.graph_schema, output.data (GraphContainer)
```

## Source vs target matrix

| | As **source** (introspection) | As **target** (`migrate_graph`) |
|---|---|---|
| Neo4j | yes | yes |
| ArangoDB | yes | yes |
| GraFlo file backend | yes | yes |
| PostgreSQL | no (use SQL ingestion) | yes (relational graph) |
| TigerGraph, FalkorDB, Memgraph, NebulaGraph | no* | yes |

\*Use a file backend as an intermediate store when the live database does not support graph export yet.

List export-capable backends in code:

```python
from graflo.db.manager import ConnectionManager

ConnectionManager.graph_export_flavors()
# [DBType.NEO4J, DBType.ARANGO, DBType.GRAFLO_BACKEND]
```

## Related documentation

- [Graph export and migration](../concepts/operations/graph_export_migration.md) — full API reference and file backend layout
- [Graph export and replay](graph_export_and_replay.md) — chunked on-disk intermediate
- [Quick start — Graph export and migration](../getting_started/quickstart.md#graph-export-and-migration)
