# Example 19: Union of manifests with canonical vocabulary and n-ary equivalence

Two independent manifests describe overlapping entities. Source A speaks its
own vocabulary (`Firm` / `Shop`, `firm_id` / `shop_id`); a **canonical map**
translates `Firm` into the target model (`Company`, `company_id`). Source B's
`Org` and `Branch`, and A's `Shop`, are declared as one **n-ary equivalence
cluster** with `Firm` — one `VertexEquivalence` naming every member on each
side, in each manifest's own vocabulary. The cluster has no `into`: the map
names the composed class.

Two declarations, one recipe. The map says *what things are called*; the
cluster says *which classes are one*. `compose_manifests` resolves both into a
single composite map per side, applies it in one step, unions by name, and
refuses when the two disagree.

The guiding principle: **a primary identity is a property of the class.** The
merged `Company` gets ONE identity definition referencing only canonical
attributes (`match_key`, `local_key`). *How* each source populates them —
gating, normalization, namespacing — is resource knowledge, carried as
`identity_alignments` on the compose op. The source manifests stay pure.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)

## The recipe

```python
from graflo.architecture.evolution import (
    AlignmentAttribute,
    CanonicalMap,
    ComposeManifestsOp,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    VertexEquivalence,
    compose_manifests,
)
```

```python
op = ComposeManifestsOp(
    vertex_equivalences=[
        # {Firm, Shop} ~ {Org, Branch}; named Company by the map
        VertexEquivalence(left=["Firm", "Shop"], right=["Org", "Branch"]),
    ],
    allow_merges=True,  # a stated intent: >1 member on a side
    canonical_maps={"left": canonical_map},  # Firm → Company, firm_id → company_id
    identity_alignments=[ALIGNMENT],  # applied inside compose
)
union = compose_manifests(A, B, op)
```

Renames compose, so this is the same function as canonicalizing A on its own
first and then declaring the cluster in canonical names — `compose_manifests`
accepts either, and a map entry the caller already applied is a no-op:

```python
canonical_a = apply_evolution(
    A, canonical_map_to_ops(canonical_map)
)  # one CanonicalizeOp
op = ComposeManifestsOp(
    vertex_equivalences=[
        VertexEquivalence(
            left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
        )
    ],
    allow_merges=True,
    canonical_maps={"left": canonical_map},
)
union = compose_manifests(canonical_a, B, op)  # identical result
```

`identity_alignments` on the compose op still emit only fundamentals:

1. `AddVertexPropertiesOp` — declare `match_key` + `local_key` on `Company`;
2. `AddResourceTransformsOp` — per-resource derivation steps appended to the
   pipelines;
3. `ReplaceIdentityOp` — a priority funnel over the canonical attributes only:
   `[match_key, local_key]`, no side-specific branches;
4. `AddSecondaryIdentitiesOp` — retired side keys stay addressable for lookups.

## How the composed name is found

For every cluster, in order: `into` when given (translated through the map
when the map maps it); else the canonical name the map gives a member; else
the one spelling every member shares; else compose refuses (*unnamed
cluster*). A member may be spelled by its own name (`Firm`) or by its
canonical name (`Company`).

## Consistency

One rule: **the map and the cluster must agree on where every name goes, and
a canonical target is a fixed point neither may re-map.** Compose refuses,
naming both declarations, on:

- a **disagreement** — the map says `Firm → Company` but the cluster names the
  composed class `Party` (`--disagreeing-map-demo`), or a cluster renames a
  class the map already established as canonical;
- a map entry that **merges a non-member into a composed class** — declare it
  in the cluster instead; the cluster's identity and property maps govern it;
- a **dangling** map entry that matches nothing on its side;
- the same rule for attributes: a canonical attribute is a fixed point, and a
  property equivalence names fields as spelled on the member.

Clusters must also not contradict each other — `ClusterConflictError`,
wrapped as `ComposeCanonicalConflictError` when it surfaces through
`validate_and_complete_canonical_map`:

- a class claimed by **two** declarations — e.g. `right:Org` named in both
  `{Firm}~{Org, Branch}` and `{Shop}~{Org}→Party` (`--conflicting-cluster-demo`);
- two declarations sharing one composed name — that collapses them into one
  composed class, which must be spelled as one n-ary declaration instead;
- a composed name that already names an existing, non-member class on a side —
  that would silently merge into an unrelated type.

## How the condition works

The gate lives in a **resource transform**, not a connector filter — a
connector filter would drop the non-matching records entirely, and they must
still be ingested. `gated_normalized_key` emits the normalized shared key only
when the gate matches, and `None` otherwise:

```python
AlignmentAttribute(
    into="match_key",
    sources={
        "r_a": DerivationSpec(
            input=["secondary_key", "shared_raw"],
            params={"prefix": "abc_", "strip_prefix": "ABC-"},
        ),
        "r_b": DerivationSpec(
            input=["org_id", "shared_raw"],
            params={"prefix": "", "strip_prefix": "ABC-"},
        ),
    },
)
```

`None` is an empty value to the identity digest, so the funnel's `match_key`
branch never fires for a non-gated record — it falls through to `local_key`,
which each resource fills from its own key via `tagged_key`, **namespaced**
so cross-side collisions are impossible.

**Derivation inputs are RAW source-doc field names** (`firm_id`, not
`company_id`): property renames rewrite `vertex.from` maps so documents keep
their original keys, and transform inputs are never rewritten.

**Exact-name attributes fuse for free.** After the boundary relabel,
`merge_vertex_models` unions fields by spelling — list a `PropertyEquivalence`
only to rename or to flag identity.

## Priority semantics

With several alignment attributes, their order is funnel priority: a record keys by
the **highest-priority attribute it carries**. Two records fuse when their
strongest present attribute coincides.

## Run it

No live graph database required.

```bash
cd examples/19-union-canonical-equivalence
uv run python build_union.py                          # → artifacts/manifest_union.yaml
uv run python inspect_fusion.py                       # which records fuse, and to what
uv run python build_union.py --disagreeing-map-demo   # map vs cluster → conflict
uv run python build_union.py --conflicting-cluster-demo  # overlapping declarations → conflict
```

`inspect_fusion.py` prints one row per emitted vertex doc across the four
resources feeding `Company`: five records collapse to three vertices, one
fused pair per aligned key.

`build_union.py` stays because it shows the recipe as Python. The same
recipe is a verb — `graflo compose` applies the op and its canonical maps
together:

```bash
graflo compose manifest_a.yaml manifest_b.yaml \
  --op boundary_op.yaml --canonical-map left=canonical_map.yaml \
  -o artifacts/manifest_union.yaml
```

`boundary_op.yaml` is the cluster written as YAML, with a declared natural
key in place of the alignment. `--canonical-map` is folded into the op
(`canonical_maps`), so the same document could carry the map itself. Drop it
and the same op is refused: the cluster has no name and nothing establishes
one.

## Notes

- **Equivalence is declared, never inferred.** The canonical map, the
  `VertexEquivalence` cluster, and the `IdentityAlignment` are author-supplied;
  compose only cross-checks the declarations against each other and completes
  the composed name from them.
- **A merge is a stated intent.** A canonical map that collapses two classes
  requires `allow_merges: true`, and so does a compose op whose cluster names
  more than one member on a side — `ComposeManifestsOp(allow_merges=True)`.
  A merge that would turn an edge into a self-relation, or make one pipeline
  level produce the composed class twice, needs `allow_self_relations` /
  `allow_observation_fusion` on the same op: compose forwards both to the
  per-side `CanonicalizeOp` instead of bypassing the unary guards.
- **Changing the funnel rekeys the graph** — branch order, ids, and field sets
  all feed the digest (see example 17).

See example 17 for identity funnels on a single manifest, and example 18 for
*discovering* cross-resource identity instead of declaring it.
