# Vertex identity modes

GraFlo vertices declare how records are matched during upserts through fields on the logical **`Vertex`** model in `schema.graph.vertex_config`. Identity semantics are **not** configured in `DatabaseProfile` or `IngestionModel`.

## Three runtime modes

Each vertex resolves to one of three modes via the derived property **`Vertex.identity_mode`**:

| `identity_mode` | `blank` | `hash_identity_properties` | `identity` | Write-time behavior |
|---|---|---|---|---|
| **`natural`** | `false` | `[]` | `[f]` or `[f1, f2, ...]` | Upsert on declared fields (same code path for one or many fields) |
| **`hash`** | `false` | `[f1, f2, ...]` | `["id"]` | SHA256 of hash sources → synthetic `id`, then upsert |
| **`blank`** | `true` | `[]` | `["id"]` | Random UUID → synthetic `id`, then upsert |

Unary and composite natural keys are the **same runtime mode**. The upsert path passes `Vertex.identity` to the database as `match_keys`; width does not change the write branch.

## Schema fields

### `identity`

Logical field name(s) used for upsert matching. For `hash` and `blank` modes the normalizer sets `identity` to `["id"]` (GraFlo canonical synthetic key; ArangoDB maps to `_key` at write time).

### `hash_identity_properties`

Source field names whose values are hashed (SHA256, full hex digest) to produce the synthetic `id`. Only the listed fields are included — transient properties never enter the hash.

Example:

```yaml
- name: product
  properties: [org, product_code, name, category]
  identity: [id]
  hash_identity_properties: [org, product_code, region]
```

### `blank`

Placeholder vertices with no stable natural key; each record gets a random UUID at ingest time.

## Inference vs runtime

`IdentityInferencer` discovers keys from record samples. Its **`strategy`** is separate from runtime mode:

| Inference `strategy` | Runtime `identity_mode` |
|---|---|
| `unary` | `natural` |
| `composite` | `natural` |
| `hash_fallback` | `hash` |
| `no_viable_identity` | *(vertex unchanged)* |

### API

- **`IdentityInferenceConfig`** — `min_sample_size` (default 100), optional `max_sample_size`, bootstrap and scoring weights
- **`IdentityInferencer.infer(samples)`** — returns `IdentityInferenceResult`
- **`apply_identity_inference_to_vertices()`** — apply inference to a vertex list (immutable)
- **`infer_identities_from_snapshot()`** — infer from a `GraFloOutput` YAML snapshot

See [Example 15](../../examples/example-15.md) for a CSV → manifest → ingest walkthrough.

## Where configuration does *not* live

| Layer | Why not |
|---|---|
| **`DatabaseProfile`** | Physical indexes and storage names only |
| **`IngestionModel`** | Pipeline and connectors only; no identity semantics |

`VertexConfig.hash_identity_vertices` and `VertexConfig.vertices_by_identity_mode()` are derived lists for runtime introspection and `db_writer` branching.
