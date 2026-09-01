# Example 19: Union of manifests with canonical vocabulary and conditional equivalence

Two independent manifests describe overlapping entities. Source A speaks its
own vocabulary (`Firm`, `firm_id`); a **canonical map** translates it into the
target model (`Company`, `company_id`). Source B's `Org` is **equivalent** to
`Company` — but only conditionally: an A-record participates in the
entity-level equivalence only when its `secondary_key` starts with `abc_`. Non-`abc_`
records are still ingested; they just must never fuse with B-entities.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)

## The recipe, in order

```python
from graflo.architecture.evolution import (
    CanonicalMap,
    ComposeManifestsOp,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_compose_against_canonical_map,
)

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

# 5. install the conditional-equivalence identity, post-compose
union = apply_evolution(union, [ReplaceIdentityOp(vertices={"Company":
    IdentityReplacement(to=FunnelIdentityTarget(funnel=IdentityFunnel(branches=[
        IdentityBranch(id="shared",  fields=["match_key"]),   # fuses
        IdentityBranch(id="a_local", fields=["company_id"]),  # non-abc_ A
        IdentityBranch(id="b_local", fields=["org_id"]),      # B fallback
    ])), retire="keep")})])
```

## How the condition works

The gate lives in a **resource transform**, not a connector filter — a
connector filter would drop the non-`abc_` records entirely, and they must still
be ingested. `graflo.util.transform.gated_normalized_key` emits the normalized
shared key only when the gate matches, and `None` otherwise:

```yaml
transforms:
-   name: gate_match_key
    module: graflo.util.transform
    foo: gated_normalized_key
    params: {prefix: abc_, strip_prefix: ABC-}
    input: [secondary_key, shared_raw]
    output: [match_key]
```

`None` is an empty value to the identity digest, so the funnel's `shared`
branch never fires for a non-`abc_` record — it falls through to `a_local` and
keys off its own `company_id`. Source B reuses the same function with
`prefix: ''` (an always-true gate), so both sides share one normalization code
path and cannot drift apart.

Two records that take the `shared` branch with the same normalized value
digest to the same synthetic `id` — across two resources, with no join and no
live lookup. `include_branch_id` (the default) keeps the branches
collision-free: a `shared` digest can never equal an `a_local` digest, even
over equal values.

## Why this order

- **The funnel goes in after compose.** It references both sides' local keys
  (`company_id`, `org_id`), which coexist only on the merged class —
  `replace_identity` refuses to key a vertex on properties it does not declare.
- **Validation goes in before compose.** `compose_manifests` checks that
  equivalence endpoints *exist*, but cannot know that `Firm` is a name the
  canonical map retired. `validate_compose_against_canonical_map` can, and
  fails loudly instead of composing a wrong union.

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
resource  local key   gate    match_key   id
r_a       f1          abc_7   alpha       9f56063a…
r_a       f2          zz9     -           2b69296f…
r_b       o1          -       alpha       9f56063a…

3 records → 2 vertices (1 fused)
```

`f1` (gate `abc_7`) and `o1` normalize to the same `match_key` and fuse; `f2`
carries the same raw shared value but fails the gate, so it keys locally and
stays a separate vertex.

## What to look for

- **Equivalence is declared, never inferred.** The canonical map and the
  `VertexEquivalence` are author-supplied; validation only cross-checks the
  two declarations against each other.
- **A merge is a stated intent.** A canonical map that collapses two classes
  requires `allow_merges: true`; a compose op that collapses two right-side
  classes onto one target requires `allow_implicit_merge=True` — and the
  validator warns when the collapse will turn an edge into a self-relation.
- **Changing the funnel rekeys the graph** — branch order, ids, and field sets
  all feed the digest (see [Example 17](example-17.md)).

## Files

| File | Purpose |
|------|---------|
| `manifest_a.yaml` | Source A in its own vocabulary, with the gated transform |
| `manifest_b.yaml` | Source B, same normalization with an always-true gate |
| `canonical_map.yaml` | Classes A → C and attrs A → C |
| `build_union.py` | Steps 1–5 as executable code |
| `inspect_fusion.py` | Cast both sources, print which records fuse |

## Related documentation

- [Manifest evolution](../concepts/schema/manifest_evolution.md) — compose, canonical maps, identity ops
- [Vertex identity](../concepts/schema/vertex_identity.md) — funnel semantics
- [Example 17 — Identity funnel](example-17.md)
- [Example 18 — Cross-resource identity discovery](example-18.md)
