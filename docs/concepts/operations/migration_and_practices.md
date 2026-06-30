# Migration and practices

Schema migration workflow, performance levers, and authoring best practices.

## Schema migration

- **Read-only planning first** — use `migrate_schema plan --from-schema-path ... --to-schema-path ...` to generate a deterministic operation plan before any writes.
- **Risk-gated execution** — v1 executes only low-risk additive operations by default and blocks high-risk/destructive operations.
- **Backend scope** — execution adapters are currently focused on ArangoDB and Neo4j; other backends are plan-first until adapter coverage is added.
- **History and idempotency** — applied revisions are tracked in a migration manifest (`.graflo/migrations.json`) with revision + schema hash checks.
- **Operational commands** — `plan`, `apply`, `status`, and `history` are exposed through the `migrate_schema` CLI entrypoint.

### Comparing schemas

When you compare schemas, treat it like comparing two building blueprints:

- `--from-schema-path` is the **current building** blueprint.
- `--to-schema-path` is the **target building** blueprint.
- `migrate_schema plan` is the **architectural diff report** that tells you what must be added, changed, or removed to get from current to target.

Another useful analogy is `git diff`, but for graph structure:

- Additive changes (new vertex type, new edge, new property, new index) are similar to adding code in a backward-compatible way.
- Destructive changes (removing properties/types, identity shifts) are similar to breaking API changes: they often require explicit migration steps, data sweeps, or rollouts.

Practical comparison checklist:

1. Run `plan` first and review operations grouped by risk.
2. Confirm identity changes explicitly (identity shifts are high-impact).
3. Validate whether each blocked operation needs a manual script, staged rollout, or explicit high-risk approval.
4. Use `apply --dry-run` before any real apply.

Example:

```bash
uv run migrate_schema plan \
  --from-schema-path schema_v1.yaml \
  --to-schema-path schema_v2.yaml \
  --output-format json
```

How to read the output:

- `operations`: runnable operations under current risk policy (v1 defaults to low-risk subset).
- `blocked_operations`: operations intentionally withheld for safety.
- `warnings`: policy and compatibility notes you should resolve before execution.

### Migration command examples

```bash
# Plan changes between two schema versions
uv run migrate_schema plan \
  --from-schema-path schema_v1.yaml \
  --to-schema-path schema_v2.yaml

# Dry-run apply to inspect backend actions
uv run migrate_schema apply \
  --from-schema-path schema_v1.yaml \
  --to-schema-path schema_v2.yaml \
  --db-config-path db.yaml \
  --revision 0001_additive_updates \
  --dry-run

# Persist migration history after real execution
uv run migrate_schema apply \
  --from-schema-path schema_v1.yaml \
  --to-schema-path schema_v2.yaml \
  --db-config-path db.yaml \
  --revision 0001_additive_updates \
  --no-dry-run

# Inspect migration state
uv run migrate_schema status
uv run migrate_schema history
```

### Why this helps

Schema comparison gives you a predictable transition path between versions. Instead of discovering incompatibilities during ingestion, you see structural deltas in advance, gate risky steps, and execute a controlled rollout.

## Performance optimization

- **TigerGraph token caching**: Secret-based API tokens are cached per process for the ingest run (one fetch per cluster/graph/secret, not per upsert batch or `ConnectionManager` open)
- **Batch processing**: Process large datasets in configurable batches (`IngestionParams.batch_size` on `Caster` / `GraphEngine`)
- **Batch prefetch**: While one batch is cast and written, `Caster.process_data_source` can prefetch up to `IngestionParams.batch_prefetch` additional batches from `AbstractDataSource.iter_batches` (bounded memory, overlapped I/O)
- **Parallel execution**: Utilize multiple cores for faster processing (`n_cores` parameter of `Caster`)
- **Ingestion scope filters**: Limit a run to specific resources (`IngestionParams.resources`), connectors (`IngestionParams.connectors` — name or hash, same refs as `resource_connector`), and/or vertex types (`IngestionParams.vertices`). When both `resources` and `connectors` are set, only connectors bound to listed resources that also match the connector filter are ingested.
- **Efficient resource handling**: Optimized processing of both table and tree-like data

## Best practices

1. Use compound identity fields for natural keys, and **`schema.db_profile`** secondary indexes for query performance
2. Leverage blank vertices (`blank: true` on the vertex definition) for complex relationship modeling; include them in the resource pipeline when they must be populated at cast time
3. Define reusable transforms in **`ingestion_model.transforms`** and reference them from resource steps
4. Configure appropriate batch sizes based on your data volume
5. Enable parallel processing for large datasets
6. Choose the right relationship attribute based on your data format:
   - **`relation_field`** on an edge **actor** step — relation from a column/field
   - **`relation_from_key`** on an edge **actor** step — relation from JSON keys
   - **`relation`** on the logical edge — static relationship name when applicable
7. Use logical edge **`properties`** (and edge-actor payload options) for temporal or quantitative relationship attributes
   - Specify types when the target DB requires them (e.g., TigerGraph)
   - Use typed `Field` objects or dicts with a `type` key for better validation
8. Leverage key matching (`match_source`, `match_target`) on edge steps for complex matching scenarios
9. Use PostgreSQL schema inference for automatic schema generation from normalized databases (3NF) with proper PK/FK constraints
10. Use RDF/OWL schema inference (`infer_schema_from_rdf`) when ingesting data from SPARQL endpoints or `.ttl` files with a well-defined ontology
11. Specify property types for better validation and database-specific optimizations, especially when targeting TigerGraph
12. **Bidirectional edges**: choose one strategy — (a) two directed logical edges + `AddInverseEdgesOp` for portability; (b) TigerGraph `edge_specs[*].reverse_edge` for a native `WITH REVERSE_EDGE` pair with one load path; (c) `directed: false` for truly symmetric relationships (`UNDIRECTED EDGE` on TigerGraph). Do not combine `reverse_edge` with a second logical reverse edge or `AddInverseEdgesOp` on the same forward relation.
