# 21 — Identity alignment on a routed source

Source A is one view: a heterogeneous stream discriminated by `kind`, routed by
a **single `vertex_router`** nested under a `descend`. An equivalence collapses
two of its branches (`Firm`, `Shop`) onto `Company`; `person` keeps flowing
through the same router.

**The router is never split.** Splitting the resource — one per collapsing class
— would scan the view twice and duplicate a discriminator the source already
carries. Everything below exists so that one router keeps serving every
`type_field` value it serves today.

## What the merge already does to the router

Nothing here is new. `MergeVerticesOp` rewrites the router in place: `type_map`
becomes `{firm: Company, shop: Company, person: Person}` and `vertex_from_map`
keys are remapped, with the merged types' column maps **unioned** — one vertex
field reading two different columns raises rather than silently keeping the
last. And `allow_observation_fusion` is *not* needed: a router emits at most one
vertex per document, so two branches pointing at one class cannot fuse
observations. Only `allow_merges=True` is required, for naming two members on a
side.

## Two things the alignment must get right

**1. Derivations land at the router's level.** `alignment_to_ops` resolves the
level that produces `Company` and targets it with `AddResourceTransformsOp.at`.
Appended at the root they would derive nothing — an actor reads its transform
buffer at its own `LocationIndex` with no ancestor fallback, a `descend` subtree
runs *before* its own level's transforms, and a transform whose declared inputs
are missing skips silently by default. `--root-demo` forces the root and gets a
loud `AlignmentConflictError` instead.

**2. Each collapsing branch derives its own way.** `firm` rows carry `firm_ref`,
`shop` rows carry `shop_ref`. One `DerivationSpec` per resource cannot say that,
so the resource supplies a **list**:

```python
AlignmentAttribute(into="match_key", sources={
    "r_view": [
        DerivationSpec(input=["secondary_key", "firm_ref"],
                       params={"prefix": "abc_", "strip_prefix": "ABC-"}),
        DerivationSpec(input=["secondary_key", "shop_ref"],
                       params={"prefix": "abc_", "strip_prefix": "ABC-"}),
    ],
    "r_b": DerivationSpec(input=["org_id", "shared_raw"], params={"prefix": ""}),
})
```

Branch selection needs no gate here: the union view leaves the other branch's
column empty, and the derivation functions return `None` for an empty value.
`local_key` *does* gate — both branches would otherwise be namespaced the same —
so its sources read the router's own discriminator via `gate="kind"`.

## Why a list is not just two steps

Two steps writing `match_key` directly would work on a plain `vertex` step,
whose buffer extraction skips `None`. Behind a router it would not: the router
merges the transform buffer into **one observation dict**, where a later `None`
overwrites an earlier real value. So a multi-source attribute lowers to one
gated step per branch writing a scratch field, and one `coalesce_fields` step —
a single writer — reducing them:

```yaml
- transform: {call: {foo: gated_normalized_key, input: [secondary_key, firm_ref],
                     output: [_match_key__0], ...}}
- transform: {call: {foo: gated_normalized_key, input: [secondary_key, shop_ref],
                     output: [_match_key__1], ...}}
- transform: {call: {foo: coalesce_fields, strategy: all,
                     params: {fields: [_match_key__0, _match_key__1]},
                     output: [match_key]}}
```

`strategy: all` empties the missing-input guard, so a branch whose columns are
absent from a document skips without taking the coalesce down with it. Scratch
fields are not declared properties, so extraction drops them.

## Delivering through the router

A router's child `VertexActor` runs at a `LocationIndex` whose transform buffer
is empty, so derived attributes reach it only through the merged observation —
subject to `keep_fields` and `extraction_scope`. A plain `vertex` step reads the
buffer directly and is unaffected. This router sets `keep_fields`, so the
alignment emits `EnsureExtractedFieldsOp`, which adds `match_key` / `local_key`
to that list. Only the aligned class's projection is touched; the router keeps
serving `Person` exactly as before.

## Run it

No live graph database required.

```bash
cd examples/21-router-union-alignment
uv run python build_union.py          # → artifacts/manifest_union.yaml
uv run python inspect_fusion.py       # which records fuse, and to what
uv run python build_union.py --root-demo   # derive at the root → conflict
```

`inspect_fusion.py` shows five records collapsing to three vertices — one fused
pair per aligned key — and `Person`, emitted by the same router, carrying none
of the canonical attributes.

See example 19 for the canonical-map / n-ary-cluster recipe this builds on,
example 17 for identity funnels on a single manifest, and example 18 for
*discovering* cross-resource identity instead of declaring it.
