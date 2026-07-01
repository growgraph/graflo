# Example 13 — GraFlo file backend (export, migration, ingest)

Demonstrates `GraFloBackendConfig` as a first-class **source** and **target** in `migrate_graph()` and `ingest()`.

## Quick start (no database required)

Ingest bundled CSV resources into an on-disk backend and inspect the result:

```bash
cd examples/13-graph-export-migration
uv run python ingest.py
uv run python inspect_backend.py --backend-dir artifacts/csv-backend
```

## With live databases

Requires Neo4j (export source) and/or ArangoDB / PostgreSQL (replay targets). Connection configs load from environment variables or `docker/<backend>/.env`.

```bash
cd examples/13-graph-export-migration

# Neo4j → chunked on-disk backend (schema.yaml + INDEX.json + gzip JSONL chunks)
uv run python export.py --output-dir artifacts/neo4j-backend

# File backend → ArangoDB or PostgreSQL
uv run python migrate.py arango --from-backend artifacts/csv-backend --recreate-schema
uv run python migrate.py postgres --from-backend artifacts/neo4j-backend --recreate-schema
```

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | HR schema, ingestion model, and CSV file connectors. |
| `data/*.csv` | Sample people and department resources. |
| `ingest.py` | CSV manifest → GraFlo file backend (`define_and_ingest`). |
| `inspect_backend.py` | Print `INDEX.json` inventory for a backend directory. |
| `export.py` | Neo4j → GraFlo file backend (`migrate_graph`). |
| `migrate.py` | GraFlo file backend (or Neo4j) → ArangoDB / PostgreSQL. |
| `_common.py` | Shared config helpers and click options. |

Documentation: [Example 13](../../docs/examples/example-13.md) · [Graph export and migration](../../docs/concepts/operations/graph_export_migration.md)
