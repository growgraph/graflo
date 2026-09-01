# 19 — Union of manifests: canonical vocabulary + conditional equivalence

Two independent manifests describe overlapping entities. Source A speaks its
own vocabulary (`Firm`, `firm_id`); a **canonical map** translates it into the
target model (`Company`, `company_id`). Source B's `Org` is **equivalent** to
`Company` — but only conditionally: an A-record participates in the
entity-level equivalence only when its `secondary_key` starts with `abc_`.
Non-matching records are still ingested; they just must never fuse with
B-entities.

The guiding principle: **a primary identity is a property of the class.** The
merged `Company` gets ONE identity definition referencing only canonical
attributes (`match_key`, `local_key`). *How* each source populates them —
gating, normalization, namespacing — is resource knowledge, appended to the
resource pipelines as fundamental evolution ops. The source manifests stay
pure.

## The recipe, in order

```python
# 1. canonicalize A standalone — A now speaks the canonical vocabulary
canonical_a = apply_evolution(A, canonical_map_to_ops(canonical_map))

# 2. author the boundary equivalence in canonical names
op = ComposeManifestsOp(vertices=[
    VertexEquivalence(left="Company", right="Org", into="Company"),
])

# 3. cross-validate BEFORE composing — fails loudly on conflicts
validate_compose_against_canonical_map(canonical_map, op,
                                       left=canonical_a, right=B)

# 4. compose (name_conflict defaults to "error")
union = compose_manifests(canonical_a, B, op)

# 5. apply the identity alignment — emits only fundamental ops
union = apply_evolution(union, alignment_to_ops(ALIGNMENT, manifest=union,
                                                canonical_maps=[canonical_map]))
```

`alignment_to_ops` composes four fundamentals:

1. `AddVertexPropertiesOp` — declare `match_key` + `local_key` on `Company`;
2. `AddResourceTransformsOp` — per-resource derivation steps appended to the
   pipelines (the new ingestion-side fundamental op);
3. `ReplaceIdentityOp` — a priority funnel over the canonical attributes only:
   `[match_key, local_key]`, no side-specific branches;
4. `AddSecondaryIdentitiesOp` — `by_company_id` / `by_org_id` keep the retired
   side keys addressable for lookups and edge-endpoint resolution.

## How the condition works

The gate lives in a **resource transform**, not a connector filter — a
connector filter would drop the non-matching records entirely, and they must
still be ingested. `gated_normalized_key` emits the normalized shared key only
when the gate matches, and `None` otherwise:

```python
AlignmentRow(into="match_key", sources={
    "r_a": DerivationSpec(input=["secondary_key", "shared_raw"],
                          params={"prefix": "abc_", "strip_prefix": "ABC-"}),
    "r_b": DerivationSpec(input=["org_id", "shared_raw"],
                          params={"prefix": "", "strip_prefix": "ABC-"}),
})
```

`None` is an empty value to the identity digest, so the funnel's `match_key`
branch never fires for a non-gated record — it falls through to `local_key`,
which each resource fills from its own key via `tagged_key`, **namespaced**
(`a:f2` vs `b:o1`) so cross-side collisions are impossible. Source B reuses
the same derivation function with `prefix: ''` (an always-true gate), so both
sides share one normal form.

**Derivation inputs are RAW source-doc field names** (`firm_id`, not
`company_id`): property renames rewrite `vertex.from` maps so documents keep
their original keys, and transform inputs are never rewritten. Passing the
supplied canonical maps to `alignment_to_ops` makes this a loud validation
error instead of a silent empty derivation.

## Priority semantics

With several alignment rows, row order is funnel priority: a record keys by
the **highest-priority attribute it carries**. Two records fuse when their
strongest present attribute coincides. A match on a lower-priority attribute
does *not* fuse records when one of them also carries a higher-priority one —
that trade (deterministic, digest-only, no write-time lookups) is deliberate
and pinned in the test suite.

## Run it

No live graph database required.

```bash
cd examples/19-union-canonical-equivalence
uv run python build_union.py                # → artifacts/manifest_union.yaml
uv run python inspect_fusion.py             # which records fuse, and to what
uv run python build_union.py --stale-demo   # the validator failing loudly
```

`inspect_fusion.py` prints one row per emitted vertex doc:

```
resource  local_key   gate    match_key   id
r_a       a:f1        abc_7   alpha       ada61dd4…
r_a       a:f2        zz9     -           f52e9036…
r_b       b:o1        -       alpha       ada61dd4…

3 records → 2 vertices (1 fused)
```

`f1` (gate `abc_7`) and `o1` normalize to the same `match_key` and fuse; `f2`
carries the same raw shared value but fails the gate, so it keys by `a:f2` and
stays a separate vertex.

## Notes

- **Equivalence is declared, never inferred.** The canonical map, the
  `VertexEquivalence`, and the `IdentityAlignment` are author-supplied;
  validation only cross-checks the declarations against each other.
- **A merge is a stated intent.** A canonical map that collapses two classes
  requires `allow_merges: true`; a compose op that collapses two right-side
  classes onto one target requires `allow_implicit_merge=True`.
- **Changing the funnel rekeys the graph** — branch order, ids, and field sets
  all feed the digest (see example 17).

See example 17 for identity funnels on a single manifest, and example 18 for
*discovering* cross-resource identity instead of declaring it.
