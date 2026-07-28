# Example 16: Secondary identities for edge-only sources

This example shows how an edge-only CSV can attach endpoints by **business keys**
(ISIN, LEI) while vertex upserts continue to use primary identities (`sid`, `iid`).
No live graph database is required — ingest lands in a GraFlo file backend.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)

## The problem

Many feeds relate entities that already live in the graph, but they only carry
alternate identifiers — not the graph's primary keys. Declaring those field-sets
as **`secondary_identities`** puts them on the **lookup plane**: edges match on
them; upserts still use `identity`.

## Step 1 — Ingest

```bash
cd examples/16-secondary-identities
uv run python ingest.py
```

Three resources run in order:

1. **`instruments`** / **`issuers`** — ordinary vertex upserts on `sid` / `iid`
2. **`links`** — `lookup_only` vertex steps plus an edge step with
   `source_match: by_isin` and `target_match: by_lei`

Writes `artifacts/csv-backend/` (same pattern as [Example 13](example-13.md)).

## Step 2 — Inspect

```bash
uv run python inspect_graph.py
```

Expected:

| Layer | Result |
|-------|--------|
| Vertices | 3 instruments, 3 issuers — **no** extras invented from `links.csv` |
| Edges | `S1→I1`, `S2→I2`, `S3→I3` with `share` — endpoints are **primary** ids |

Resolution happens immediately before the edge write via
`Connection.resolve_vertices`, so the edge write itself stays an ordinary
primary-key operation on every backend.

## Manifest sketch

```yaml
- name: instrument
  identity: [sid]
  secondary_identities:
    - name: by_isin
      fields: [isin]

# …
resources:
  - name: links
    pipeline:
      - vertex: instrument
        lookup_only: true
      - vertex: issuer
        lookup_only: true
      - from: instrument
        to: issuer
        relation: issued_by
        source_match: by_isin
        target_match: by_lei
```

`source_match` / `target_match` also accept an explicit field list equal to a
declared set, or `secondary` when exactly one set is declared. Soft-uniqueness
policy lives on `ingestion_model.endpoints_on_ambiguous` (default `all`).

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | Secondary identities + three-resource pipeline |
| `data/instruments.csv` | Owned instrument rows |
| `data/issuers.csv` | Owned issuer rows |
| `data/links.csv` | Edge-only ISIN / LEI rows |
| `ingest.py` | Define + ingest to file backend |
| `inspect_graph.py` | Print declarations, counts, resolved edges |
| `_common.py` | Paths and workdir helper |

## Related documentation

- [Vertex identity modes — secondary identities](../concepts/schema/vertex_identity.md#secondary-identities-edge-endpoint-lookup)
- [Backend indexes](../concepts/schema/backend_indexes.md)
- [Example 15 — Identity inference](example-15.md)
- [Example 13 — GraFlo file backend](example-13.md)
