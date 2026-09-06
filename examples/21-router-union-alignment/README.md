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
marker = DerivationSpec(input=["shared_raw"], foo="affix_gated_key",
                        params={"prefix": "ABC-"})

AlignmentAttribute(into="match_key", sources={
    "r_view": [
        DerivationSpec(input=["firm_ref"], foo="affix_gated_key",
                       params={"prefix": "ABC-"}),
        DerivationSpec(input=["shop_ref"], foo="affix_gated_key",
                       params={"prefix": "ABC-"}),
    ],
    "r_b": marker,
    "r_branch": marker,
})
```

## The marker is a filter, not a cleanup

`affix_gated_key` reads **one** field, and the `ABC-` marker on that value *is*
the admission test: carry it and you are stripped and accepted as canonical key
material, omit it and you get `None`.

The marker is an affix *pair* — `prefix` and `suffix`, each defaulting to `""`,
which every string carries. So a marker can lead (`ext_42`), trail
(`42-legacy`), or bracket the key, and naming neither admits everything while
stripping nothing. This example only needs the leading half.

That `None` is a fall-through, not a drop. It is an empty value to identity
digests, so the funnel branch listing `match_key` is skipped and the record
lands on its side-local key — still ingested, just outside the cross-source
cluster. `data/view.csv` makes the difference visible: two `firm` rows carry the
same business name, one as `ABC-Alpha` and one as bare `Alpha`. Only the first
fuses with source B's `ABC-ALPHA`.

The contrast is with `gated_normalized_key`, which gates on a *sibling* field
and whose `strip_prefix` is `str.removeprefix` — a silent no-op when the prefix
is absent. Under it both spellings normalize to `alpha` and fuse, and the marker
carries no authority. Example 19 keeps that idiom, which is the right one when
participation is decided by a different column than the key itself.

Because the test lives on the value, every side runs the identical one-field
call, so the two normal forms cannot drift apart.

Branch selection needs no extra gate: the union view leaves the other branch's
column empty, and an empty value carries no marker. `local_key` *does* gate —
both branches would otherwise be namespaced the same — so its sources read the
router's own discriminator via `gate="kind"`.

## Why a list is not just two steps

Two steps writing `match_key` directly would work on a plain `vertex` step,
whose buffer extraction skips `None`. Behind a router it would not: the router
merges the transform buffer into **one observation dict**, where a later `None`
overwrites an earlier real value. So a multi-source attribute lowers to one
step per branch writing a scratch field, and one `coalesce_fields` step — a
single writer — reducing them:

```yaml
- transform: {call: {foo: affix_gated_key, input: [firm_ref],
                     output: [_match_key__0], ...}}
- transform: {call: {foo: affix_gated_key, input: [shop_ref],
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
pair per aligned key, plus the unmarked `firm` row keeping its own — and
`Person`, emitted by the same router, carrying none of the canonical attributes.

See example 19 for the canonical-map / n-ary-cluster recipe this builds on,
example 17 for identity funnels on a single manifest, and example 18 for
*discovering* cross-resource identity instead of declaring it.
