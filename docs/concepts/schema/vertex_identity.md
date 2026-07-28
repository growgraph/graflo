# Vertex identity modes

GraFlo vertices declare how records are matched during upserts through fields on the logical **`Vertex`** model in `schema.graph.vertex_config`. Upsert identity modes and **`secondary_identities`** live on the vertex; physical indexes live under `DatabaseProfile`. Soft-uniqueness *policy* for secondary lookups (`endpoints_on_ambiguous`) lives on `IngestionModel` — the key is schema, the reaction to colliding matches is ingestion.

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

## Secondary identities (edge endpoint lookup)

The four modes above answer *how a vertex is upserted*. **`secondary_identities`** answers a different question: *how an edge finds a vertex that already exists*.

An edge-only source frequently references its endpoints by a business key — an ISIN, an LEI, a source-local code — that is not the vertex's primary identity, and often carries no primary key at all. Declaring that field-set lets an edge step match on it while upserts continue to use `identity`.

```yaml
- name: instrument
  properties: [sid, isin, org, local_code]
  identity: [sid]
  secondary_identities:
    - name: by_isin
      fields: [isin]
    - [org, local_code]        # bare list; auto-named secondary_1
```

Selection is **per endpoint**, so source and target choose independently:

```yaml
resources:
  - name: links
    pipeline:
      - vertex: instrument
        lookup_only: true      # matched, never written
      - vertex: issuer
        lookup_only: true
      - from: instrument
        to: issuer
        relation: issued_by
        source_match: by_isin          # name, field list, or "secondary"
        target_match: identity         # explicit primary (also the default)
```

`source_match` / `target_match` accept a declared name, an explicit field list equal to a declared set, or `secondary` when exactly one is declared. Omitted (or `identity`) means the primary identity, so existing edge steps are unaffected. An unknown selector fails at manifest load, listing what *is* declared.

### `lookup_only`

A resource that references a vertex without owning it marks its vertex steps `lookup_only: true`. Those observations take part in edge rendering but are never upserted. Without it, rows carrying only a secondary key would be written as vertices with no primary key.

As a safety net the writer refuses to upsert any document carrying no identity value at all, since no backend can store one meaningfully.

### Soft uniqueness and ambiguity

Secondary identities are **softly** unique: the declared index is non-unique, so the database never rejects duplicates. When a lookup matches several vertices, `ingestion_model.endpoints_on_ambiguous` decides:

| Policy | Behaviour |
|---|---|
| **`all`** (default) | Attach the edge to every match; never discards data |
| `first` | Attach to one match, chosen deterministically by primary identity |
| `skip` | Write no edge for that row, and count it |
| `error` | Raise, aborting the batch |

An edge step overrides the model default with `on_ambiguous`. A row whose key matches nothing, or whose composite key is incomplete, produces no edge and is counted — a partial key is never partially matched.

### How it runs

Endpoints are resolved to their primary identity immediately before the edge write, via `Connection.resolve_vertices`. The edge write itself therefore stays an ordinary primary-key operation, which is why this works identically on backends that address endpoints by key (PostgreSQL foreign keys, NebulaGraph VIDs, TigerGraph `PRIMARY_ID`) and on those that match by property (Cypher, AQL).

Two consequences worth knowing:

- **Endpoint vertices must already exist.** Resolution reads the database, so the resource that owns a vertex has to be ingested before the edge-only resource that references it. Unmatched endpoints are counted and logged rather than raised.
- Each declared secondary identity gets a **non-unique index** automatically (see [backend indexes](backend_indexes.md)); on NebulaGraph that index is required for the lookup to run at all.

Not supported in this form: hash- or funnel-derived secondary identities, and upserting a vertex *by* its secondary identity. Both are reserved.

Runnable walkthrough: [Example 16](../../examples/example-16.md) (`examples/16-secondary-identities/`) — instruments and issuers upserted on primary keys, then an edge-only CSV linked by ISIN / LEI.

!!! note "Distinct from the SQL connector's `type_lookup`"
    `SqlConnector`'s `type_lookup` also has `source_identity` / `target_identity` fields. Those name **join columns in a lookup table** and are unrelated to schema identities.

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

## Where configuration lives

| Layer | Holds | Does *not* hold |
|---|---|---|
| **`Vertex`** | Upsert `identity` / mode fields; **`secondary_identities`** (lookup field-sets) | Write-time ambiguity policy |
| **`DatabaseProfile`** | Physical indexes and storage names (auto-indexes from secondary identities land here) | Logical identity semantics |
| **`IngestionModel`** | Pipeline steps (`lookup_only`, `source_match` / `target_match`); **`endpoints_on_ambiguous`** | Vertex key definitions |

`VertexConfig.hash_identity_vertices`, `VertexConfig.blank_vertices`, `VertexConfig.assigned_vertices`, and `VertexConfig.vertices_by_identity_mode()` are derived lists for runtime introspection and `db_writer` / assemble branching.
