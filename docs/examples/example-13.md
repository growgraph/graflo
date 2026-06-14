# Example 13: Graph export and migration

This example shows how to treat an existing **Neo4j** or **ArangoDB** database as a GraFlo **source**: export a self-describing **`GraFloOutput`** artifact, migrate graph→graph, or land the same logical model in **PostgreSQL** as vertex and junction edge tables.

No manifest YAML is required — `GraphEngine` introspects the source, sanitizes the schema for the target flavor, and loads data in one pass.

## Prerequisites

- Python 3.11+
- A graph database with export support as **source** (Neo4j or ArangoDB)
- Optional **target**: another supported LPG (ArangoDB, Neo4j, …) or PostgreSQL
- Connection configs via environment variables or `docker/<backend>/.env` (see [Quick Start](../getting_started/quickstart.md))

## Supported directions

| Task | Method | Source | Target |
|---|---|---|---|
| Export schema + data | `export_graph()` | Neo4j, ArangoDB | `GraFloOutput` (file) |
| Migrate graph→graph | `migrate_graph()` | Neo4j, ArangoDB | Any LPG target |
| Migrate graph→SQL | `migrate_graph()` | Neo4j, ArangoDB | PostgreSQL |
| Schema only | `infer_schema_from_graph()` | Neo4j, ArangoDB | — (`Schema`) |

## Step 1 — Connection configs

```python
from graflo.db import Neo4jConfig, ArangoConfig, PostgresConfig

# Recommended when using repo docker compose layouts
neo4j = Neo4jConfig.from_docker_env()
arango = ArangoConfig.from_docker_env()
postgres = PostgresConfig.from_docker_env()

# Or from process environment (NEO4J_URI, ARANGO_URI, POSTGRES_HOST, …)
# neo4j = Neo4jConfig.from_env()
```

## Step 2 — Export typed output

`export_graph()` introspects vertex labels/collections and edge types, samples properties for field inference, then fetches all documents and edge rows.

```python
from graflo import GraphEngine, DBType
from graflo.architecture.schema import GraFloOutput

engine = GraphEngine(target_db_flavor=DBType.ARANGO)

output = engine.export_graph(
    neo4j,
    sample_limit=100,
    data_limit=None,  # set e.g. 1000 to cap rows per type while testing
)

# Self-describing YAML: top-level keys "schema" and "data"
output.to_yaml("artifacts/neo4j-export.yaml")

# Round-trip
restored = GraFloOutput.from_yaml("artifacts/neo4j-export.yaml")
print(restored.core_schema.vertex_config.vertices)
print(list(restored.data.vertices.keys()))
```

In Python, use `output.graph_schema` (alias `output.schema`). Serialized JSON/YAML uses the key `"schema"`.

Edge keys inside `data.edges` serialize as JSON arrays, e.g. `["person","department","works_in"]`, so relation names may contain `|` without ambiguity.

## Step 3 — Migrate Neo4j → ArangoDB

`migrate_graph()` reuses a single source connection for introspection and export, sanitizes once for the target, defines DDL, and writes batches.

```python
from graflo import GraphEngine, DBType

engine = GraphEngine(target_db_flavor=DBType.ARANGO)
engine.migrate_graph(
    neo4j,
    arango,
    recreate_schema=True,
    clear_data=False,
    sample_limit=100,
)
```

Set `recreate_schema=True` to drop and recreate target collections/labels. Use `clear_data=True` when keeping schema but wiping rows first.

## Step 4 — Migrate Neo4j → PostgreSQL

PostgreSQL targets map each vertex type to a table and each edge to a junction table `{source}_{target}_{relation}_edges` with `source_id`, `target_id`, optional weight columns, and a surrogate primary key for parallel edges.

```python
from graflo import GraphEngine, DBType

pg_engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
pg_engine.migrate_graph(
    neo4j,
    postgres,
    recreate_schema=True,
)
```

Inspect results with ordinary SQL:

```sql
SELECT * FROM public.person LIMIT 5;
SELECT * FROM public.person_person_knows_edges LIMIT 5;
```

(Table names follow GraFlo vertex/edge naming helpers; exact names depend on the inferred schema.)

## Step 5 — Schema only (no data copy)

When you only need the logical model:

```python
from graflo import GraphEngine, DBType

engine = GraphEngine(target_db_flavor=DBType.ARANGO)
schema = engine.infer_schema_from_graph(neo4j, sample_limit=100)
schema.to_yaml("artifacts/neo4j-schema.yaml")
```

## Runnable script

See `examples/13-graph-export-migration/export_migrate.py` for a small CLI that runs export (default) or migration when configs are available:

```bash
# Export Neo4j → YAML (safe default)
uv run python examples/13-graph-export-migration/export_migrate.py export \
  --output artifacts/neo4j-export.yaml

# Migrate Neo4j → Arango (destructive on target when --recreate-schema)
uv run python examples/13-graph-export-migration/export_migrate.py migrate-arango \
  --recreate-schema

# Migrate Neo4j → PostgreSQL
uv run python examples/13-graph-export-migration/export_migrate.py migrate-postgres \
  --recreate-schema
```

## Related docs

- [Graph export and migration](../concepts/graph_export_migration.md) — API reference and capability notes
- [Example 5](example-5.md) — PostgreSQL as a **source** (3NF inference into a graph target)
