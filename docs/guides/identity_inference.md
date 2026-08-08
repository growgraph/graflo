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

## Samples from a non-CSV source

`IdentityInferencer.infer()` takes **flat records**, which is why this guide starts from CSV. To
reach a PostgreSQL table, a directory of mixed files, or a nested API response, sample the source
first and flatten through its profile:

```python
from graflo.architecture.onto_sample import profile_sample
from graflo.hq.graph_engine import GraphEngine

source = GraphEngine().sample_resources(pg_config, schema_name="public", max_docs=500)
sample = source.get("customers")
profile = profile_sample(sample)

records = profile.flat_docs(
    sample.docs
)  # nested paths become 'customer.id', 'items[].sku'
```

A sampled source also carries the **declared** `primary_key` and `foreign_keys` when it has them —
prefer those over an inferred identity, and see
[Sampling and profiling](../concepts/schema/sampling_and_profiling.md) for the caps and the caveat
on `unique`.

## Identity modes

After inference, each vertex has a derived **`identity_mode`**:

- **`natural`** — upsert on `identity` (unary or composite)
- **`hash`** — `hash_identity_properties` hashed into synthetic `id`
- **`assigned`** — intentional UUID primary key (not produced by inference)
- **`blank`** — random UUID placeholder

See [Vertex identity modes](../concepts/schema/vertex_identity.md) for the full model.

## Full runnable example

See [Example 15](../examples/example-15.md) and `examples/15-identity-inference/`.
For attaching edges by a business key that is not the upsert identity, see
[Example 16](../examples/example-16.md) (`secondary_identities`).

## Related documentation

- [Vertex identity modes](../concepts/schema/vertex_identity.md)
- [Sampling and profiling](../concepts/schema/sampling_and_profiling.md) — where samples come from
- [Example 16 — Secondary identities](../examples/example-16.md)
- [Core components — Vertex](../concepts/architecture/core_components.md)
- [Graph export and replay](graph_export_and_replay.md) — file backend ingest pattern
