# Example 13 — Graph export and migration

Demonstrates `GraphEngine.export_graph`, `migrate_graph`, and `infer_schema_from_graph` with Neo4j as source.

Requires connection configs via `docker/neo4j/.env` (and target backend `.env` for migrate commands) or equivalent environment variables.

```bash
# Export (default — writes artifacts/neo4j-export.yaml)
uv run python examples/13-graph-export-migration/export_migrate.py export

# Migrate (destructive on target when --recreate-schema)
uv run python examples/13-graph-export-migration/export_migrate.py migrate-arango --recreate-schema
uv run python examples/13-graph-export-migration/export_migrate.py migrate-postgres --recreate-schema
```

Documentation: [Example 13](../../docs/examples/example-13.md) · [Graph export and migration](../../docs/concepts/graph_export_migration.md)
