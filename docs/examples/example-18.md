# Example 18: Cross-resource identity discovery

Two customer exports describe the same people, but nobody wrote down that the
CRM's `customer_email` and billing's `email_address` are the same thing. This
example **proposes** a shared identity policy from sampled documents alone — no
live database, no LLM, no configuration beyond the data.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)

## The problem

Multi-source ingestion stalls on one question: *which column in feed B means the
same thing as this column in feed A?* Answering it by hand does not scale past a
handful of resources, and answering it by column name alone is wrong often
enough to be dangerous.

## Step 1 — Discover

```bash
cd examples/18-cross-resource-identity
uv run python discover.py
```

```
strategy    : natural
identity    : ['customer_email']
confidence  : 0.83

Column alignments (how the resources were matched up):
  left                         right                          name  values  declared
  billing.country              crm.signup_country             0.67    1.00     False
  billing.email_address        crm.customer_email             0.37    1.00     False

Per-resource field maps (source -> canonical):
  billing: {'country': 'country', 'email_address': 'customer_email'}
  crm: {'signup_country': 'country', 'customer_email': 'customer_email'}

Evidence:
  shared_key_values: 150
  uniqueness_by_resource: {'crm': 1.0, 'billing': 1.0}
```

## What to look for

**Name similarity alone would have missed it.** `email_address` vs
`customer_email` scores **0.37** — under any sane name threshold. What carries
the pairing is `values: 1.00`: after normalization the two columns hold exactly
the same value set. So value overlap is a *mandatory floor* and name similarity
only contributes to the combined score; a weak name cannot veto strong value
evidence.

The fixtures deliberately include formatting drift — some CRM emails are
uppercased, some billing emails carry stray whitespace. `normalize_for_match`
absorbs that for comparison purposes only; it never rewrites ingested data.

**Uniqueness is checked per resource, not over the pool.** The 150 CRM rows and
150 billing rows describe the *same* 150 people, so the shared key deliberately
repeats across resources. Pooling and demanding global uniqueness would score
this key at 0.5 and reject it — exactly the case the module exists to serve.
`shared_key_values: 150` is the positive evidence: every entity matched.

## Step 2 — Apply (after review)

```bash
uv run python discover.py --apply
```

```yaml
name: party
properties:
- name: full_name
- name: invoice_total
- name: customer_email
identity:
- customer_email
```

`apply_proposal_to_vertex` rebuilds the vertex through validation rather than
assigning fields, so an impossible policy fails loudly — patching a LIST-typed
column as an identity raises rather than producing a subtly broken schema.

## Where fuzzy stops

> Fuzzy signals align columns. Exact equality proves keys.

Column-name similarity and value overlap decide *which columns to compare*.
Whether the field-set is really a key is settled by exact equality after
normalization, plus bootstrap resampling on each resource. Nothing probabilistic
reaches the write path — soft matching there silently fuses distinct entities,
and that damage is unbounded and hard to reverse.

## When there is no shared key

| Outcome | When |
|---|---|
| `funnel` | Each resource keys itself, but nothing spans them. One branch per resource, plus a warning that single-source rows will not converge. See [Example 17](example-17.md). |
| `hash_fallback` | No key proven; a digest over the shared fields that may collide. Carries a warning. |
| `no_viable_identity` | Fewer than two usable resources, samples below `min_sample_size`, or no columns aligned. `apply_proposal_to_vertex` refuses it. |

## Declared keys beat heuristics

When samples carry declared `primary_key` / `foreign_keys` — which
`ResourceSampler.sample_postgres` fills in from real database constraints —
those pairings are used directly and skip the heuristic. A declared foreign key
is ground truth; a name match is a guess.

## Files

| File | Purpose |
|------|---------|
| `data/crm_customers.csv` | `customer_email`, `full_name`, `signup_country` |
| `data/billing_accounts.csv` | `email_address`, `phone`, `country`, `invoice_total` |
| `discover.py` | Sample both, propose an identity, optionally patch a vertex |
| `generate_data.py` | Regenerate the fixtures (fixed seed) |

## Related documentation

- [Cross-resource identity discovery](../concepts/schema/cross_resource_identity.md)
- [Sampling and profiling](../concepts/schema/sampling_and_profiling.md)
- [Example 17 — Identity funnel](example-17.md)
- [Example 15 — Single-resource identity inference](example-15.md)
