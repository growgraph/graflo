# Identity inference from CSV

Infer vertex `identity` and `hash_identity_properties` from flat CSV samples, write an updated manifest, and ingest into a GraFlo file backend.

## Prerequisites

- Python 3.11+
- A manifest with vertex types but unset or placeholder identities
- CSV sample files with enough rows for heuristics (default `min_sample_size=100`)

## Step 1 — Prepare manifest and samples

Define vertex types in `manifest.yaml` with properties matching your CSV columns. Place sample CSVs under `data/`.

## Step 2 — Run inference

```bash
cd examples/15-identity-inference
uv run python infer.py
uv run python inspect_identities.py
```

`infer.py` loads the manifest and CSV samples, runs `IdentityInferencer`, and writes `artifacts/manifest-inferred.yaml`.

Expected strategies for the bundled data:

| Vertex | Strategy | `identity_mode` | `identity` |
|--------|----------|-----------------|------------|
| `product` | `composite` | `natural` | `product_code`, `org` |
| `supplier` | `unary` | `natural` | `supplier_code` |

Tune sample sizes:

```bash
uv run python infer.py --min-sample-size 100 --max-sample-size 500
```

## Step 3 — Ingest with inferred manifest

```bash
uv run python ingest.py
```

Writes a chunked GraFlo file backend under `artifacts/csv-backend/`.

## Identity modes

After inference, each vertex has a derived **`identity_mode`**:

- **`natural`** — upsert on `identity` (unary or composite)
- **`hash`** — `hash_identity_properties` hashed into synthetic `id`
- **`blank`** — random UUID

See [Vertex identity modes](../concepts/schema/vertex_identity.md) for the full model.

## Full runnable example

See [Example 15](../examples/example-15.md) and `examples/15-identity-inference/`.

## Related documentation

- [Vertex identity modes](../concepts/schema/vertex_identity.md)
- [Core components — Vertex](../concepts/architecture/core_components.md)
- [Graph export and replay](graph_export_and_replay.md) — file backend ingest pattern
