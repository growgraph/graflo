# Graph export and replay

Export a live graph to a chunked GraFlo file backend on disk, then replay into another database — or ingest manifest resources directly to disk.

## Prerequisites

- Python 3.11+
- Optional graph source: Neo4j or ArangoDB (for export)
- Optional connection configs via environment variables or `docker/<backend>/.env`

## When to use this

- Dry-run ingestion without a live target database
- Large-graph exports that should not stay in memory
- Replay the same export into multiple targets (ArangoDB, Neo4j, PostgreSQL)

## Step 1 — Export a graph to disk

```python
from pathlib import Path

from graflo import DBType, GraphEngine
from graflo.db import Neo4jConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig

neo4j = Neo4jConfig.from_env()
backend = GraFloBackendConfig(output_dir=Path("artifacts/neo4j-backend"))

engine = GraphEngine(target_db_flavor=DBType.ARANGO)
engine.migrate_graph(neo4j, backend, recreate_schema=True)
```

This writes `schema.yaml`, `INDEX.json`, and chunked gzip JSONL under `vertices/` and `edges/`.

## Step 2 — Replay to a target

```python
from graflo.db import ArangoConfig, PostgresConfig

arango = ArangoConfig.from_env()
engine.migrate_graph(backend, arango, recreate_schema=True)

# Or replay into PostgreSQL relational graph tables:
pg_engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
pg_engine.migrate_graph(backend, PostgresConfig.from_env(), recreate_schema=True)
```

## Step 3 — Ingest manifest resources to disk

Use `GraFloBackendConfig` as the `ingest()` target the same way as any other backend — only the config changes:

```python
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=GraFloBackendConfig(output_dir=Path("artifacts/csv-backend")),
    connection_provider=provider,
)
```

## Pre-sanitize for a future target

Set `target_flavor_hint` so `schema.yaml` is sanitized for the intended backend before it is written:

```python
backend = GraFloBackendConfig(
    output_dir=Path("artifacts/for-arango"),
    target_flavor_hint=DBType.ARANGO,
)
```

## Full runnable example

See [Example 13](../examples/example-13.md) and `examples/13-graph-export-migration/`.

## Related documentation

- [Graph export and migration](../concepts/operations/graph_export_migration.md) — API reference and layout details
- [Core components](../concepts/architecture/core_components.md) — `GraphEngine`, `GraFloOutput`, `GraphContainer`
