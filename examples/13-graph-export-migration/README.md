# Example 13 — GraFlo file backend (export, migration, ingest)

Demonstrates `GraFloBackendConfig` as a first-class **source** and **target** in `migrate_graph()` and `ingest()`.

Requires Neo4j (or an existing file backend) for graph-source commands, and uses bundled CSV resources for the ingest demo.

```bash
# Neo4j → chunked on-disk backend (schema.yaml + INDEX.json + gzip JSONL chunks)
uv run python examples/13-graph-export-migration/export_migrate.py export-backend

# CSV manifest → file backend via ingest()
uv run python examples/13-graph-export-migration/export_migrate.py ingest-backend

# File backend → ArangoDB or PostgreSQL
uv run python examples/13-graph-export-migration/export_migrate.py migrate-arango \
  --from-backend artifacts/neo4j-backend --recreate-schema
uv run python examples/13-graph-export-migration/export_migrate.py migrate-postgres \
  --from-backend artifacts/neo4j-backend --recreate-schema
```

Documentation: [Example 13](../../docs/examples/example-13.md) · [Graph export and migration](../../docs/concepts/graph_export_migration.md)
