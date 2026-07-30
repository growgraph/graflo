# 18 — Cross-resource identity discovery

Two customer exports describe the same people. The CRM calls the key column
`customer_email`; billing calls it `email_address`. Nobody wrote down that they
are the same thing, and the values do not even match literally — some are
uppercased, some have stray whitespace.

This example **proposes** a shared identity policy from sampled documents alone:
no live database, no LLM, no configuration beyond the data.

## Run it

```bash
cd examples/18-cross-resource-identity
uv run python discover.py
uv run python discover.py --apply     # also patch a vertex and print the YAML
```

Output:

```
strategy    : natural
identity    : ['customer_email']
confidence  : 0.83

Column alignments (how the resources were matched up):
  left                         right                          name  values  declared
  billing.country              crm.signup_country             0.67    1.00     False
  billing.email_address        crm.customer_email             0.37    1.00     False

Evidence:
  shared_key_values: 150
  uniqueness_by_resource: {'crm': 1.0, 'billing': 1.0}
```

## What to look for

**Name similarity alone would have missed it.** `email_address` vs
`customer_email` scores **0.37** — well under any sane name threshold. What
carries the pairing is `values: 1.00`: after normalization the two columns hold
exactly the same set of values. Two columns that share no values cannot be the
same column however alike they read, and two columns that share every value
almost certainly are. So value overlap is a mandatory floor and name similarity
only contributes to the combined score.

**Uniqueness is checked per resource, not over the pool.** The 150 CRM rows and
150 billing rows describe the *same* 150 people, so the shared key deliberately
repeats across resources — pooling them and demanding global uniqueness would
reject exactly the keys worth finding. `uniqueness_by_resource` shows 1.0 on
both sides; `shared_key_values: 150` shows every entity matched.

**Fuzzy signals never decide a merge.** Column-name similarity and value overlap
*align columns*. Whether a key is real is then proven by exact equality after
normalization, plus bootstrap resampling. The distinction matters: soft matching
in the write path silently fuses distinct entities, and the damage is unbounded.

## When there is no shared key

If each resource keys itself well but nothing spans them, the proposal comes
back as a **funnel** with one branch per resource, plus a warning that rows
described by only one source will not converge. That is the honest answer — see
[Example 17](../17-identity-funnel/README.md) for how a funnel then behaves at
ingest time.

If no columns align at all, or the sample is too small for uniqueness to mean
anything, the strategy is `no_viable_identity` and `apply_proposal_to_vertex`
refuses to apply it.

## Declared keys beat heuristics

When the sample carries declared constraints — `ResourceSample.primary_key` and
`foreign_keys`, which `ResourceSampler.sample_postgres` fills in from real
database metadata — those pairings are used directly and skip the heuristic
entirely. A declared foreign key is ground truth; a name match is a guess.

## Files

| File | Purpose |
|------|---------|
| `data/crm_customers.csv` | `customer_email`, `full_name`, `signup_country` |
| `data/billing_accounts.csv` | `email_address`, `phone`, `country`, `invoice_total` |
| `discover.py` | Sample both, propose an identity, optionally patch a vertex |
| `generate_data.py` | Regenerate the fixtures (fixed seed) |

## Related

- [Cross-resource identity discovery](../../docs/concepts/schema/cross_resource_identity.md)
- [Example 17 — Identity funnel](../17-identity-funnel/README.md)
- [Example 15 — Single-resource identity inference](../15-identity-inference/README.md)
