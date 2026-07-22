# Vertex identity modes

GraFlo vertices declare how records are matched during upserts through fields on the logical **`Vertex`** model in `schema.graph.vertex_config`. Identity semantics are **not** configured in `DatabaseProfile` or `IngestionModel`.

## Four runtime modes

Each vertex resolves to one of four modes via the derived property **`Vertex.identity_mode`**. Modes describe **how the upsert key is obtained** (not string encoding). They are mutually exclusive.

| `identity_mode` | Authored signal | `identity` | Key behavior |
|---|---|---|---|
| **`natural`** | default | `[f]` or `[f1, f2, …]` | Upsert on declared fields. If a field is typed **`UUID`**, validate shape when present — **do not invent**. |
| **`hash`** | non-empty `hash_identity_properties` | `["id"]` | SHA256 of hash sources → synthetic `id`, then upsert |
| **`assigned`** | `assigned: true` | `["id"]` | Intentional UUID PK: empty → `uuid4()` at **assemble** (before edge projection); writer is an idempotent safety net. **Not** blank-edge resolution. |
| **`blank`** | `blank: true` | `["id"]` | Placeholder: random UUID at write time; listed in `blank_vertices`; **does** blank-edge resolution |

Unary and composite natural keys are the **same runtime mode**. The upsert path passes `Vertex.identity` to the database as `match_keys`; width does not change the write branch.

### Blank vs assigned

Both may mint a random UUID when the synthetic `id` is empty, but they are not interchangeable:

| | `blank` | `assigned` |
|---|---|---|
| Meaning | No business identity / placeholder | Intentional UUID primary key |
| Mint timing | Writer (`_assign_blank_vertex_ids`) | Assemble (before `assemble_edges`); writer net is idempotent |
| Blank-edge resolution | Yes (`_resolve_blank_edges`) | **No** |
| Typical use | Mentions, ephemeral join stubs | Events / entities whose PK is a UUID |

## Schema fields

### `identity`

Logical field name(s) used for upsert matching. For `hash`, `blank`, and `assigned` modes the normalizer sets `identity` to `["id"]` (GraFlo canonical synthetic key; ArangoDB maps to `_key` at write time).

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

Placeholder vertices with no stable natural key; each record gets a random UUID at ingest time and may participate in blank-edge expansion.

### `assigned`

Intentional UUID primary key. Empty identity is filled with `uuid4()` so cast-time edge projections see the key. Present valid UUIDs are preserved; invalid non-empty values raise.

```yaml
- name: event
  properties:
    - { name: id, type: UUID }
    - { name: payload, type: STRING }
  identity: [id]
  assigned: true
```

Natural key that happens to be a UUID (no new mode):

```yaml
- name: user
  properties:
    - { name: external_id, type: UUID }
    - { name: email, type: STRING }
  identity: [external_id]
```

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

`VertexConfig.hash_identity_vertices`, `VertexConfig.blank_vertices`, `VertexConfig.assigned_vertices`, and `VertexConfig.vertices_by_identity_mode()` are derived lists for runtime introspection and `db_writer` / assemble branching.
