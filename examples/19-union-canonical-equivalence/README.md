# 19 — Union of manifests: canonical vocabulary + n-ary equivalence

Two independent manifests describe overlapping entities. Source A speaks its
own vocabulary (`Firm` / `Shop`, `firm_id` / `shop_id`); a **canonical map**
translates `Firm` into the target model (`Company`, `company_id`). Source B's
`Org` and `Branch`, and A's remaining `Shop`, are declared as one **n-ary
equivalence cluster** with `Company` — one `VertexEquivalence` naming every
member on each side, collapsing onto the same composed class.

The guiding principle: **a primary identity is a property of the class.** The
merged `Company` gets ONE identity definition referencing only canonical
attributes (`match_key`, `local_key`). *How* each source populates them —
gating, normalization, namespacing — is resource knowledge, carried as
`identity_alignments` on the compose op. The source manifests stay pure.

## The recipe, in order

```python
# 1. canonicalize A standalone — Firm → Company; Shop stays for now
canonical_a = apply_evolution(A, canonical_map_to_ops(canonical_map))

# 2. author the n-ary boundary cluster in canonical names — one declaration
#    naming every member collapsing onto Company
op = ComposeManifestsOp(
    vertices=[
        VertexEquivalence(
            left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
        ),
    ],
    allow_merges=True,             # a stated intent: >1 member on a side
    identity_alignments=[ALIGNMENT],  # applied inside compose
)

# 3. validate + complete BEFORE composing — fails loudly on conflicts;
#    completes Org/Branch/Shop → Company along the cluster
validate_and_complete_canonical_map(
    op, left=canonical_a, right=B,
    canonical_maps=[("left", canonical_map)],
)

# 4. compose (name_conflict defaults to "error"); identity_alignments run here
union = compose_manifests(
    canonical_a, B, op, canonical_maps=[("left", canonical_map)]
)
```

`identity_alignments` on the compose op still emit only fundamentals:

1. `AddVertexPropertiesOp` — declare `match_key` + `local_key` on `Company`;
2. `AddResourceTransformsOp` — per-resource derivation steps appended to the
   pipelines;
3. `ReplaceIdentityOp` — a priority funnel over the canonical attributes only:
   `[match_key, local_key]`, no side-specific branches;
4. `AddSecondaryIdentitiesOp` — retired side keys stay addressable for lookups.

## Cluster consistency

A `VertexEquivalence` declaration *is* one cluster: `left` / `right` each
accept a bare class name (a 1-1 equivalence) or a list (an n-ary merge,
requiring `allow_merges=True`). The equivalence layer refuses three ways an
author could contradict themselves across declarations rather than silently
picking one:

- a class claimed by **two** declarations — e.g. `right:Org` named in both
  `{Company}~{Org, Branch}→Company` and `{Shop}~{Org}→Party` — raises
  `ClusterConflictError` ("claimed by two equivalence declarations");
- two declarations sharing one `into` — that collapses them into one composed
  class, which must be spelled as one n-ary declaration instead;
- an `into` that already names an existing, non-member class on a side —
  that would silently merge into an unrelated type.

`ClusterConflictError` is wrapped as `ComposeCanonicalConflictError` when it
surfaces through `validate_and_complete_canonical_map`.

The same declaration completes the canonical map: an unmapped member (`Org`,
`Branch`, `Shop`) inherits the cluster's `into` label (`Company`).

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
so cross-side collisions are impossible.

**Derivation inputs are RAW source-doc field names** (`firm_id`, not
`company_id`): property renames rewrite `vertex.from` maps so documents keep
their original keys, and transform inputs are never rewritten.

**Exact-name attributes fuse for free.** After boundary rename,
`merge_vertex_models` unions fields by spelling — list a `PropertyEquivalence`
only to rename or to flag identity.

## Priority semantics

With several alignment rows, row order is funnel priority: a record keys by
the **highest-priority attribute it carries**. Two records fuse when their
strongest present attribute coincides.

## Run it

No live graph database required.

```bash
cd examples/19-union-canonical-equivalence
uv run python build_union.py                         # → artifacts/manifest_union.yaml
uv run python inspect_fusion.py                      # which records fuse, and to what
uv run python build_union.py --stale-demo            # pre-canonical name → conflict
uv run python build_union.py --conflicting-cluster-demo  # overlapping declarations → conflict
```

`inspect_fusion.py` prints one row per emitted vertex doc across the four
resources feeding `Company`: five records collapse to three vertices, one
fused pair per aligned key.

## Notes

- **Equivalence is declared, never inferred.** The canonical map, the
  `VertexEquivalence` cluster, and the `IdentityAlignment` are author-supplied;
  validation only cross-checks the declarations against each other and
  completes missing canonical labels along the declared cluster.
- **A merge is a stated intent.** A canonical map that collapses two classes
  requires `allow_merges: true`, and so does a compose op whose cluster names
  more than one member on a side — `ComposeManifestsOp(allow_merges=True)`.
  A merge that would turn an edge into a self-relation, or make one pipeline
  level produce the composed class twice, needs `allow_self_relations` /
  `allow_row_fusion` on the same op: compose forwards both to the per-side
  `MergeVerticesOp` instead of bypassing the unary guards.
- **Changing the funnel rekeys the graph** — branch order, ids, and field sets
  all feed the digest (see example 17).

See example 17 for identity funnels on a single manifest, and example 18 for
*discovering* cross-resource identity instead of declaring it.
