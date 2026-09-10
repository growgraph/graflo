# Example 21 — Identity alignment on a routed source

**Source:** [`examples/21-router-union-alignment/`](https://github.com/growgraph/graflo/tree/main/examples/21-router-union-alignment)

Source A is one view: a heterogeneous stream discriminated by `kind`, routed by
a **single `vertex_router`** nested under a `descend`. An equivalence collapses
two of its classes (`Firm`, `Shop`) onto `Company`; `person` keeps flowing
through the same router.

**The router is never split.** Splitting the resource — one per collapsing class
— would scan the view twice and duplicate a discriminator the source already
carries. Everything below exists so that one router keeps serving every
`type_field` value it serves today.

## The member is the unit

An equivalence names its **members** — the classes it collapses, per side:

```python
VertexEquivalence(left=["Firm", "Shop"], right=["Org", "Branch"])
```

No `into`: `canonical_map.yaml`, carried on the same op as `canonical_maps`,
renames `Firm` to `Company`, and that names the composed class. Members are
named as the manifest names them — or by their canonical name, so `Company`
would name `Firm` just as well, here and in the member keys below.

Every record that becomes `Company` was produced *as one member* by *one
resource*. B's resources each produce one member with a plain `vertex` step.
The view produces two: `Firm` for `kind: firm` rows and `Shop` for
`kind: shop` rows, through the router. Canonical attributes are derived per
member, and that is the whole idea of what follows.

## What the merge already does to the router

Nothing here is new. The per-side relabel (`CanonicalizeOp`) rewrites the
router in place: `type_map` becomes `{firm: Company, shop: Company, person:
Person}` and `vertex_from_map`
keys are remapped, with the merged types' column maps **unioned** — one vertex
field reading two different columns raises rather than silently keeping the
last. And `allow_observation_fusion` is *not* needed: a router emits at most one
vertex per document, so two branches pointing at one class cannot fuse
observations. Only `allow_merges=True` is required, for naming two members on a
side.

Notice what the rewrite loses: after it, nothing in the union says that `shop`
once meant `Shop`. That is why the alignment resolves against the sides as
handed to compose, below.

## Deriving per member

Every kind in `data/view.csv` carries the shared business key in **one**
column, `secondary_key`, each member under its own marker — `abc_` for firms,
`def_` for shops. So column presence cannot tell the members apart; which
member a document *is* must decide the derivation. `sources` is keyed by
resource, because derivation inputs are that resource's raw columns, and the
view's entry is keyed by **member**:

```python
AlignmentAttribute(
    into="match_key",
    sources={
        "r_view": SharedDerivation(
            spec=DerivationSpec(input=["secondary_key"], foo="affix_gated_key"),
            members={"Firm": {"prefix": "abc_"}, "Shop": {"prefix": "def_"}},
        ),
        "r_b": DerivationSpec(
            input=["shared_raw"], foo="affix_gated_key", params={"prefix": "abc_"}
        ),
        "r_branch": DerivationSpec(
            input=["shared_raw"], foo="affix_gated_key", params={"prefix": "def_"}
        ),
    },
)
```

`SharedDerivation` is the compact spelling of a dict keyed by member — one
call, the members that share it, and only the parameter that differs. It
expands to `{"Firm": DerivationSpec(..., params={"prefix": "abc_"}), "Shop":
...}`, which is what you write when more than a parameter varies (a different
column, a different function), or `members=["A", "B", ...]` when nothing does.

Nothing names `kind`, `firm` or `shop`. The lowering asks the pre-merge left
side how `r_view` produces `Shop` — a router over `kind`, key `shop` — and
guards the step accordingly. What lands in the pipeline:

```yaml
- transform:
    when: {field: kind, in: [firm]}
    call: {foo: affix_gated_key, input: [secondary_key], output: [match_key],
           params: {prefix: abc_}}
- transform:
    when: {field: kind, in: [shop]}
    call: {foo: affix_gated_key, input: [secondary_key], output: [match_key],
           params: {prefix: def_}}
```

A guarded step that does not fire **writes nothing** — no output, no `None` —
so each member's step is the single writer of `match_key` for its own rows and
the two cannot clobber each other. A member produced by a plain `vertex` step
gets no guard at all: the level *is* the member. The `local_key` sources are
keyed the same way, which is how the two side-local namespaces (`firm:`,
`shop:`) come out of one resource without a hand-written gate.

The gate is derived, not written, for a reason: it is exactly the test the
router applies (`type_map` lookup, exact match), read from the same manifest
the router is defined in. A hand-written `gate="kind", gate_prefix="firm"`
would restate that knowledge with a different (prefix) semantics and drift
from it silently.

## The marker is a filter, not a cleanup

`affix_gated_key` reads **one** field, and the marker on that value *is* the
admission test: carry it and you are stripped and accepted as canonical key
material, omit it and you get `None`.

The marker is an affix *pair* — `prefix` and `suffix`, each defaulting to `""`,
which every string carries. So a marker can lead (`ext_42`), trail
(`42-legacy`), or bracket the key, and naming neither admits everything while
stripping nothing. This example only needs the leading half.

The contrast is with `gated_normalized_key`, which gates on a *sibling* field
and whose `strip_prefix` is `str.removeprefix` — a silent no-op when the prefix
is absent. Under it both spellings normalize to `alpha` and fuse, and the marker
carries no authority. Example 19 keeps that idiom, which is the right one when
participation is decided by a different column than the key itself.

Because the test lives on the value, every side runs the identical one-field
call, so the two normal forms cannot drift apart.

## Falling through

`None` is a fall-through, not a drop. It is an empty value to identity digests,
so the funnel branch listing `match_key` is skipped and the record lands on its
side-local key — still ingested, just outside the cross-source cluster.
`data/view.csv` makes three cases visible:

- `f1` (`abc_alpha`) fuses with B's `o1`; `s1` (`def_beta`) fuses with `br1`.
- `f2` carries a bare `alpha` — same business name, no marker — and keys as
  `firm:f2`.
- `s2` carries `abc_alpha`, byte-for-byte the key that fused `f1`. Under a
  value-only test it would be admitted, stripped, and fused with the firm.
  Keyed by member, the shop derivation requires `def_`, so it keys as
  `shop:s2`. **The member decides, not the marker.**
- `person` rows carry `abc_9`, and nothing happens: the guards never fire for
  them, so the derivations do not run at all — not "derived and dropped".

## When the list form is enough

If each member carried its key in its **own** column (`firm_ref`, `shop_ref`),
the other column being empty already selects, and `sources["r_view"]` can be a
plain **list** of specs — no member names, no guards. That form lowers to one
scratch field per spec and a `coalesce_fields` step as the single writer,
because two unguarded steps writing `match_key` *would* clobber behind a
router. Reach for it when columns select; key by member when the member must.

## Delivering through the router

A router's child `VertexActor` runs at a `LocationIndex` whose transform buffer
is empty, so derived attributes reach it only through the merged observation —
subject to `keep_fields` and `extraction_scope`. A plain `vertex` step reads the
buffer directly and is unaffected. This router sets `keep_fields`, so the
alignment emits `EnsureExtractedFieldsOp`, which adds `match_key` / `local_key`
to that list. Only the aligned class's projection is touched; the router keeps
serving `Person` exactly as before.

Derivations also land at the router's *level*: `alignment_to_ops` resolves the
level that produces each member and targets it with
`AddResourceTransformsOp.at`. Appended at the root they would derive nothing —
an actor reads its transform buffer at its own `LocationIndex` with no ancestor
fallback, and a `descend` subtree runs *before* its own level's transforms.
`--root-demo` forces the root and gets a loud `AlignmentConflictError` instead.

## Run it

No live graph database required.

```bash
cd examples/21-router-union-alignment
uv run python build_union.py          # → artifacts/manifest_union.yaml
uv run python inspect_fusion.py       # which records fuse, and to what
uv run python build_union.py --root-demo   # derive at the root → conflict
```

`inspect_fusion.py` shows six records collapsing to four vertices — one fused
pair per member, plus the unmarked `firm` row and the mis-marked `shop` row
each keeping their own — and `Person`, emitted by the same router, carrying
none of the canonical attributes.

See [Example 19](example-19.md) for the canonical-map / n-ary-cluster recipe this builds on,
[Example 17](example-17.md) for identity funnels on a single manifest, and [Example 18](example-18.md) for
*discovering* cross-resource identity instead of declaring it.
