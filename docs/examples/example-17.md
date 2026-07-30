# Example 17: Identity funnel across two sources

This example shows how an **identity funnel** keys the same entity from sources
that identify it differently — email in one, phone + country in another — with a
single declarative fallback order and no live lookup. No graph database is
required; ingest lands in a GraFlo file backend.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)

## The problem

A flat `hash_identity_properties` list digests one fixed field set. That works
when every source carries the same key. It fails the moment one feed has an
email address and another only a phone number: either the list is narrow enough
that half the rows have nothing to hash, or wide enough that no row has all of it.

An **`identity_funnel`** declares an ordered set of branches instead. The first
branch whose fields are all present wins, and its values are digested into `id`.

## Step 1 — See which branch keys each row

```bash
cd examples/17-identity-funnel
uv run python inspect_identities.py
```

```
source    branch  id                 evidence
crm       email   708f5d7d84b9fa70   email=ada@lovelace.io, name=Ada Lovelace, ...
crm       email   ec98fda32ff5b8e7   email=grace@hopper.mil, ...
crm       email   7f37a4e7c77494c3   email=alan@turing.uk, ...
billing   phone   b8abd25c58228b39   phone=+441632960001, country=GB, ...
billing   phone   b3dd1f0fc8a0c410   phone=+12025550142, country=US, ...
billing   email   7f37a4e7c77494c3   email=alan@turing.uk, phone=..., country=GB, ...
```

Alan Turing carries an email in **both** sources, so both rows take the `email`
branch and digest to the same `id` — one vertex, from two resources, with no join.

Ada and Grace key by `email` in the CRM and by `phone` in billing, so they land
as separate vertices. That is correct: deciding those are the same person needs
evidence the funnel does not have. See *Where the funnel stops* below.

## Step 2 — Ingest

```bash
uv run python ingest.py
```

Writes `artifacts/csv-backend/` (same pattern as [Example 13](example-13.md)).
Each source batch produces three distinct, deterministically keyed `party`
documents, and Alan's key is identical across the two.

## Manifest sketch

```yaml
- name: party
  properties: [id, email, phone, country, name, dob]
  identity: [id]
  identity_funnel:
    digest: sha256
    include_branch_id: true
    branches:
      - id: email
        when_all_present: [email]
        fields: [email]
      - id: phone
        when_all_present: [phone, country]
        fields: [phone, country]
      - id: weak
        when_all_present: [name, dob]
        fields: [name, dob]
```

`when_all_present` defaults to the branch's own `fields` and must be a subset of
them — a condition on a field the branch does not digest cannot affect the key.

## What the funnel guarantees

| Behaviour | Why |
|---|---|
| **No branch fires → no identity** | The document keeps an empty `id` and is dropped by `drop_empty_identity_docs`. Inventing a random key would create a fresh duplicate on every re-ingest. |
| **Branch id is part of the digest** | With `include_branch_id: true` (default), two branches over equal values cannot collide. Set it to `false` to reproduce a flat-hash digest byte-for-byte. |
| **Identity exists before edges are projected** | The digest runs at assemble time, the same point `assigned` mode mints UUIDs — so edge endpoints and document dedup both see a real key. |
| **Changing the funnel rekeys the graph** | Branch order, ids and field sets all feed the digest. The schema differ emits `REKEY_VERTEX` at CRITICAL risk rather than applying it quietly. |

## Where the funnel stops

The funnel is **authored policy**: it says *which key to use when*, not *which
rows are the same entity*. Concluding that `ada@lovelace.io` and
`+441632960001` denote one person requires cross-source evidence — value overlap,
column alignment, declared foreign keys. That is
**cross-resource identity discovery**, a proposal-time concern that produces a
funnel for a human to accept, and is deliberately kept out of the write path.

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | Funnel declaration + two-resource pipeline |
| `data/crm.csv` | Email-bearing rows |
| `data/billing.csv` | Phone + country rows, one with an email |
| `ingest.py` | Define + ingest to file backend |
| `inspect_identities.py` | Print the winning branch and id per source row |
| `_common.py` | Paths and workdir helper |

## Related documentation

- [Vertex identity modes — `identity_funnel`](../concepts/schema/vertex_identity.md#identity_funnel)
- [Example 15 — Identity inference](example-15.md)
- [Example 16 — Secondary identities](example-16.md)
- [Example 13 — GraFlo file backend](example-13.md)
