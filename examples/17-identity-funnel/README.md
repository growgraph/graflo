# 17 — Identity funnel

Two customer sources describe the same people, but identify them differently: the
CRM has email addresses, billing has phone + country. An **identity funnel**
declares that fallback order once, on the vertex, and every row is keyed by the
strongest evidence it actually carries.

```yaml
identity_funnel:
    digest: sha256
    include_branch_id: true
    branches:
    -   id: email
        when_all_present: [email]
        fields: [email]
    -   id: phone
        when_all_present: [phone, country]
        fields: [phone, country]
    -   id: weak
        when_all_present: [name, dob]
        fields: [name, dob]
```

The first complete branch wins. No branch complete → no identity: the document
keeps an empty `id` and is dropped, rather than being given an invented key that
would duplicate on every re-ingest.

## Run it

No live graph database required.

```bash
cd examples/17-identity-funnel
uv run python inspect_identities.py   # which branch keys each row, and to what
uv run python ingest.py               # → artifacts/csv-backend
```

## What to look for

`inspect_identities.py` prints the branch each source row resolves to:

```
source    branch  id                 evidence
crm       email   708f5d7d84b9fa70   email=ada@lovelace.io, ...
billing   phone   b8abd25c58228b39   phone=+441632960001, country=GB, ...
billing   email   7f37a4e7c77494c3   email=alan@turing.uk, ...
```

Alan Turing carries an email in **both** sources, so both rows take the `email`
branch, digest to the same `id`, and upsert onto **one** vertex — across two
separate resources, with no join and no live lookup.

Ada and Grace do not: the CRM knows their email, billing only their phone. They
key by different branches and land as separate vertices. That is correct
behaviour — deciding that `ada@lovelace.io` and `+441632960001` are the same
person needs evidence the funnel does not have. That is
**cross-resource identity discovery**, a proposal-time concern,
deliberately kept out of the write path.

## Notes

- **Branch ids are part of the key.** `include_branch_id: true` (the default)
  puts the winning branch id in the digest payload, so two branches over equal
  values cannot collide. Turning it off reproduces a flat `hash_identity_properties`
  digest exactly.
- **Changing the funnel rekeys the graph.** Branch order, ids and field sets all
  feed the digest. The schema differ reports this as `REKEY_VERTEX` at CRITICAL
  risk rather than letting it through quietly.
- **Identity is computed at assemble time**, before edges are projected and
  before documents are deduplicated on their identity fields — the same point
  where `assigned` mode mints its UUIDs.

See `docs/concepts/schema/vertex_identity.md` for the full mode table.
