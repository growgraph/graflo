# Example 16 — Secondary identities for edge-only sources

Demonstrates **lookup-plane** identities: instruments and issuers upsert on their
primary keys (`sid`, `iid`), while an edge-only CSV relates them by business keys
(ISIN, LEI) via `secondary_identities`, `source_match` / `target_match`, and
`lookup_only` vertex steps.

No live graph database required.

## Quick start

```bash
cd examples/16-secondary-identities
uv run python ingest.py
uv run python inspect_graph.py
```

## What this shows

| Piece | Role |
|-------|------|
| `instrument.secondary_identities` / `by_isin` | Alternate field-set for **endpoint lookup only** |
| `issuer.secondary_identities` / `by_lei` | Same for the target endpoint |
| `links` resource + `lookup_only: true` | Rows carry no primary key; observations are never upserted |
| `source_match` / `target_match` | Per-endpoint selector (name, field list, or `secondary`) |
| `endpoints_on_ambiguous: all` | Soft uniqueness — attach to every match when a key collides |

Expected after ingest: **3** instruments, **3** issuers, **3** `issuedBy` edges
whose endpoints are the primary identities (`S1→I1`, `S2→I2`, `S3→I3`) — not the
ISINs/LEIs from `links.csv`.

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | Schema with secondary identities + three-resource pipeline |
| `data/instruments.csv` | Owned instrument rows (`sid` PK + `isin`) |
| `data/issuers.csv` | Owned issuer rows (`iid` PK + `lei`) |
| `data/links.csv` | Edge-only rows keyed by ISIN / LEI |
| `ingest.py` | Define + ingest → `artifacts/csv-backend/` |
| `inspect_graph.py` | Print vertices, secondary identity declarations, resolved edges |
| `_common.py` | Paths and workdir helper |

## Related documentation

- [Example 16](../../docs/examples/example-16.md)
- [Vertex identity modes — secondary identities](../../docs/concepts/schema/vertex_identity.md#secondary-identities-edge-endpoint-lookup)
- [Backend indexes](../../docs/concepts/schema/backend_indexes.md)
