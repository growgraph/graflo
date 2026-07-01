# Example 15: Identity inference from CSV samples

This example shows how to infer vertex identity fields from flat CSV data, write an updated manifest, and ingest into a GraFlo file backend — no live graph database required.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)

## Runtime identity modes

After inference, each vertex has a derived **`identity_mode`**:

- **`natural`** — upsert on `identity` (one or more fields; unary and composite use the same path)
- **`hash`** — `hash_identity_properties` hashed into synthetic `id`
- **`blank`** — random UUID (not used in this example)

See [Vertex identity modes](../concepts/schema/vertex_identity.md) for the full model.

## Step 1 — Infer identities from CSV

```bash
cd examples/15-identity-inference
uv run python infer.py
uv run python inspect_identities.py
```

`infer.py` loads `manifest.yaml` and CSV samples from `data/`, runs `IdentityInferencer` (default `min_sample_size=100`), and writes `artifacts/manifest-inferred.yaml`.

Expected output for the bundled data:

| Vertex | Strategy | `identity_mode` | `identity` |
|--------|----------|-----------------|------------|
| `product` | `composite` | `natural` | `product_code`, `org` |
| `supplier` | `unary` | `natural` | `supplier_code` |

Tune inference:

```bash
uv run python infer.py --min-sample-size 100 --max-sample-size 500
```

## Step 2 — Ingest inferred manifest

```bash
uv run python ingest.py
```

Writes a chunked GraFlo file backend under `artifacts/csv-backend/` (same pattern as [Example 13](example-13.md)).

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | Initial catalog schema (identities filled in by `infer.py`) |
| `data/products.csv` | ~150 rows; composite key `(org, product_code)` |
| `data/suppliers.csv` | ~120 rows; unary key `supplier_code` |
| `infer.py` | Run inference and write `manifest-inferred.yaml` |
| `inspect_identities.py` | Print identity summary table |
| `ingest.py` | Ingest inferred manifest to file backend |
| `_common.py` | Sample loaders and defaults |

## Related documentation

- [Vertex identity modes](../concepts/schema/vertex_identity.md)
- [Core components — Vertex](../concepts/architecture/core_components.md)
- [Example 13 — GraFlo file backend](example-13.md)
