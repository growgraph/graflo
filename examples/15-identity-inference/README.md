# Example 15 — Identity inference from CSV samples

Demonstrates algorithmic vertex identity inference: read flat CSV samples, infer natural or hash-derived keys, write an updated manifest, then ingest to a GraFlo file backend.

No live graph database required.

## Quick start

```bash
cd examples/15-identity-inference
uv run python infer.py
    uv run python inspect_identities.py
uv run python ingest.py
```

## What this shows

| Vertex | Expected inference | Runtime `identity_mode` |
|--------|-------------------|-------------------------|
| `product` | composite on `(org, product_code)` | `natural` |
| `supplier` | unary on `supplier_code` | `natural` |

Unary and composite are the same runtime mode: upsert on `identity` (one or more fields). Hash mode uses `hash_identity_properties` when inference falls back to a synthetic `id`.

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | Catalog schema with vertices (identities inferred by `infer.py`) |
| `data/products.csv` | ~150 rows requiring a composite key |
| `data/suppliers.csv` | ~120 rows with unique `supplier_code` |
| `infer.py` | CSV → `IdentityInferencer` → `artifacts/manifest-inferred.yaml` |
| `inspect_identities.py` | Print `identity_mode`, `identity`, and hash fields |
| `ingest.py` | Ingest inferred manifest → `artifacts/csv-backend/` |
| `_common.py` | Sample loaders and inference defaults |

## Configuration

Inference tuning uses `IdentityInferenceConfig` (default `min_sample_size=100`). Override from the CLI:

```bash
uv run python infer.py --min-sample-size 100 --max-sample-size 500
```

Documentation: [Example 15](../../docs/examples/example-15.md) · [Vertex identity modes](../../docs/concepts/schema/vertex_identity.md)
