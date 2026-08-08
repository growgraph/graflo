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

For the full concurrency model — what runs in parallel, which knob to turn for which bottleneck, and when graflo deliberately runs serially — see **[Parallelism](../ingestion/parallelism.md)**. In brief:

- **Batch pipelining** (**`IngestionParams.max_in_flight_batches`**, default 2): casting batch N+1 overlaps writing batch N. Configurations where batch order is semantic (`dynamic_edges`, blank vertices, `extra_weights`, secondary-identity endpoints, bulk load, GraFlo file backend) are serialized automatically.
- **Batch prefetch** (**`IngestionParams.batch_prefetch`**, default 2): the reader runs ahead of processing — bounded memory, overlapped source I/O. Distinct from pipelining, which overlaps cast and write.
- **Cast workers** (**`IngestionParams.n_cores`** with `cast_executor="auto"`): with `n_cores > 1`, large batches are cast across worker processes automatically — worth it for heavy per-document work (chained transforms, deep `descend` trees).
- **Concurrent writes** (**`IngestionParams.max_concurrent_db_ops`**, default 8): vertex collections and edge types within a batch are written concurrently. This is I/O-bound work, which is where concurrency actually pays.
- **Concurrent sources** (**`IngestionParams.max_concurrent_sources`**, defaults to `min(4, sources)`): how many data sources *of one resource* run at once. Resources themselves always run in declaration order.
- **TigerGraph token caching**: Secret-based API tokens are cached per process for the ingest run (one fetch per cluster/graph/secret, not per upsert batch or `ConnectionManager` open)
- **Batch processing**: Process large datasets in configurable batches (`IngestionParams.batch_size` on `Caster` / `GraphEngine`)
- **Ingestion scope filters**: Limit a run to specific resources (`IngestionParams.resources`), connectors (`IngestionParams.connectors` — name or hash, same refs as `resource_connector`), and/or vertex types (`IngestionParams.vertices`). When both `resources` and `connectors` are set, only connectors bound to listed resources that also match the connector filter are ingested.

## Best practices

1. Use compound identity fields for natural keys, and **`schema.db_profile`** secondary indexes for query performance
2. Leverage blank vertices (`blank: true` on the vertex definition) for complex relationship modeling; include them in the resource pipeline when they must be populated at cast time
3. Define reusable transforms in **`ingestion_model.transforms`** and reference them from resource steps
4. Configure appropriate batch sizes based on your data volume; with `n_cores > 1`, keep `batch_size` comfortably above `n_cores × 64` so batches qualify for worker-process casting
5. Tune for your actual bottleneck: raise `max_concurrent_db_ops` when the database is slow, set `n_cores=4..8` when transforms are heavy — see **[Parallelism](../ingestion/parallelism.md)** for the decision list
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
