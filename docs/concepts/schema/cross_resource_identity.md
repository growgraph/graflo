# Cross-resource identity discovery

When several resources describe the same vertex, they rarely agree on column
names, and they may not share a key at all. **`CrossResourceIdentityInferencer`**
(`graflo/db/cross_resource_identity.py`) proposes a shared identity policy from
sampled documents — a natural key, a composite key, an
[identity funnel](vertex_identity.md#identity_funnel), or a flat hash — together
with the per-resource field maps and the evidence behind the choice.

It is deterministic inference over samples. No LLM, no live database.

```python
sample = engine.sample_resources(bindings)  # SourceSample
proposal = infer_from_source_sample(sample, vertex_name="party")

if proposal.strategy != "no_viable_identity":  # after human review
    vertex = apply_proposal_to_vertex(vertex, proposal)
```

## Proposal only

Nothing in this module runs at ingest time, and `infer` never mutates a
manifest. `apply_proposal_to_vertex` is a separate, explicit call — and it
refuses a `no_viable_identity` proposal outright.

The reason is a hard line through the design:

> **Fuzzy signals align columns. Exact equality proves keys.**

Column-name similarity and value overlap decide *which columns to compare*.
Whether the resulting field-set is actually a key is then settled by exact
equality after normalization, plus bootstrap resampling. Soft matching on the
write path silently fuses distinct entities, and that damage is unbounded and
hard to reverse — so it stays out.

## How a proposal is reached

1. **Eligibility.** Columns are screened by
   [`infer_column_type_cost`](../../guides/identity_inference.md): list and bytes columns,
   long free text, and mostly-null columns are disqualified as key material.
2. **Declared keys first.** If the samples carry `primary_key` / `foreign_keys`
   (PostgreSQL sampling fills these from real constraints), those pairings are
   ground truth and skip the heuristic entirely.
3. **Alignment.** Every remaining cross-resource column pair is scored on name
   similarity and normalized value overlap (Jaccard). Value overlap is a
   **mandatory floor** — two columns sharing no values cannot be the same
   column, however alike their names read. Name similarity contributes to a
   combined score but cannot veto strong value evidence on its own.
4. **Projection.** Aligned columns are renamed to one canonical name (the
   alphabetically first of the pair, so the result is stable across runs).
5. **Key search.** The smallest shared field tuple that is unique **within every
   resource** and survives bootstrap resampling in each. Tuples are scored, not
   columns: a pair may key the rows while neither field is unique alone.
6. **Fallbacks.** If no shared key exists, one branch per resource becomes an
   identity funnel. If that is not possible either, a flat hash over the shared
   fields, with a warning. If nothing aligns, `no_viable_identity`.

### Uniqueness is per resource, never pooled

Two resources describing the same 150 customers hold the same 150 key values.
Pooling their rows and demanding global uniqueness would score that key at 0.5
and reject it — which is precisely the case the module exists to serve. The
proposal reports `uniqueness_by_resource` for each side, plus
`shared_key_values`: how many key tuples appear in **every** resource, which is
the real evidence that the resources describe one entity.

## Configuration

| Knob | Default | Meaning |
|---|---|---|
| `min_sample_size` | 100 | Below this, uniqueness is not evidence of a key. |
| `max_sample_size` | none | Subsample cap for large snapshots. |
| `max_key_width` | 3 | Wider than this falls back to hash or funnel. |
| `min_value_jaccard` | 0.1 | Mandatory floor on value overlap. |
| `min_pair_score` | 0.5 | Floor on the combined name/value score. |
| `max_alignments` | 20 | Caps the pair search. |
| `n_boots`, `subsample_ratio` | 5, 0.8 | Bootstrap validation, shared with single-resource inference. |

## Strategies

| `strategy` | Meaning |
|---|---|
| `natural` | One shared field keys every resource. |
| `composite` | A shared tuple keys every resource. |
| `funnel` | No shared key; each resource keys itself through its own branch. Carries a warning: rows only one source describes will not converge. |
| `hash_fallback` | No key proven; a digest over the shared fields, which may collide. Carries a warning. |
| `no_viable_identity` | Fewer than two usable resources, samples too small, or nothing aligned. |

!!! note "Not the single-resource strategy literal"
    `IdentityStrategy` (single-resource, `db/identity_inference.py`) uses
    `unary` where this module uses `natural`. That literal is shipped and
    tested; `CrossResourceStrategy` is separate and the mapping happens at the
    proposal boundary, rather than renaming a published value for cosmetic
    consistency.

## Nested sources

The inferencer takes flat records. For nested documents, project them first with
`ResourceProfile.flat_docs` — paths become dotted field names (`contact.email`).
Note that `flat_docs` keeps the **first** occurrence per path and does not fan
out lists, so a document maps to exactly one flat record. That is right for
identifying the root entity and wrong for identifying list members.

## Input substrate

The input is `SourceSample.samples_by_resource` — `dict[str, list[dict]]` —
exactly what `GraphEngine.sample_resources` returns, so no adapter sits between
sampling and inference. See [Sampling and profiling](sampling_and_profiling.md).

Two properties of that structure matter here: it returns the **live** document
lists rather than copies (the inferencer treats them as read-only), and resource
names must be unique — `SourceSample` rejects duplicates, since keying by name
would otherwise discard documents silently.

## Runnable walkthrough

[Example 18](../../examples/example-18.md)
(`examples/18-cross-resource-identity/`) — a CRM and a billing export whose
shared email column scores **0.37** on name similarity and **1.00** on value
overlap.
