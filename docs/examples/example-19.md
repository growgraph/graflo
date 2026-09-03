# Example 19: Union of manifests with canonical vocabulary and conditional equivalence

Two independent manifests describe overlapping entities. Source A speaks its
own vocabulary (`Firm` / `Shop`, `firm_id` / `shop_id`); a **canonical map**
translates `Firm` into the target model (`Company`, `company_id`). Source B's
`Org` and `Branch`, and A's remaining `Shop`, sit in one **n-ary equivalence
cluster** with `Company` — collapsing onto the same composed class.

The guiding principle: **a primary identity is a property of the class.** The
merged `Company` gets ONE identity definition referencing only canonical
attributes (`match_key`, `local_key`). *How* each source populates them —
gating, normalization, namespacing — is resource knowledge, carried as
`identity_alignments` on the compose op. The source manifests stay pure.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)

## The recipe, in order

```python
from graflo.architecture.evolution import (
    AlignmentRow,
    CanonicalMap,
    ComposeManifestsOp,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_and_complete_canonical_map,
)

# 1. canonicalize A standalone — Firm → Company; Shop stays for now
canonical_a = apply_evolution(A, canonical_map_to_ops(canonical_map))

# 2. author the n-ary boundary cluster in canonical names
op = ComposeManifestsOp(
    vertices=[
        VertexEquivalence(left="Company", right="Org", into="Company"),
        VertexEquivalence(left="Company", right="Branch", into="Company"),
        VertexEquivalence(left="Shop", right="Org", into="Company"),
    ],
    identity_alignments=[ALIGNMENT],  # applied inside compose
)

# 3. validate + complete BEFORE composing — fails loudly on conflicts;
#    completes Org/Branch/Shop → Company along the cluster
validate_and_complete_canonical_map(
    op,
    left=canonical_a,
    right=B,
    canonical_maps=[("left", canonical_map)],
    allow_implicit_merge=True,
)

# 4. compose (name_conflict defaults to "error"); identity_alignments run here
union = compose_manifests(
    canonical_a, B, op, canonical_maps=[canonical_map]
)
```

`identity_alignments` on the compose op still emit only fundamentals:

1. `AddVertexPropertiesOp` — declare `match_key` + `local_key` on `Company`;
2. `AddResourceTransformsOp` — per-resource derivation steps appended to the
   pipelines (the ingestion-side fundamental op);
3. `ReplaceIdentityOp` — a priority funnel over the canonical attributes only:
   `[match_key, local_key]`, no side-specific branches;
4. `AddSecondaryIdentitiesOp` — retired side keys stay addressable for lookups
   and edge-endpoint resolution.

## Cluster consistency

`VertexEquivalence` edges are a bipartite graph. Connected components must
agree on one `into`. Overlapping declarations that share a node but disagree
on the target raise `ClusterConflictError` (wrapped as
`ComposeCanonicalConflictError` by the validator) instead of silently
last-write-winning in a rename dict.

The same primitive **completes** the canonical map: an unmapped peer in a
resolved cluster inherits the cluster label (`Org → Company`).

## How the condition works

The gate lives in a **resource transform**, not a connector filter — a
connector filter would drop the non-matching records entirely, and they must
still be ingested. `graflo.util.transform.gated_normalized_key` emits the
normalized shared key only when the gate matches, and `None` otherwise:

```python
ALIGNMENT = IdentityAlignment(
    vertex="Company",
    rows=[AlignmentRow(into="match_key", sources={
        "r_a": DerivationSpec(input=["secondary_key", "shared_raw"],
                              params={"prefix": "abc_", "strip_prefix": "ABC-"}),
        "r_b": DerivationSpec(input=["org_id", "shared_raw"],
                              params={"prefix": "", "strip_prefix": "ABC-"}),
    })],
    local_key=LocalKeySpec(sources={
        "r_a": LocalKeySource(field="firm_id", tag="a"),
        "r_b": LocalKeySource(field="org_id", tag="b"),
    }),
    secondary_identities={"by_company_id": ["company_id"],
                          "by_org_id": ["org_id"]},
)
```

`None` is an empty value to the identity digest, so the funnel's `match_key`
branch never fires for a non-gated record — it falls through to `local_key`,
which each resource fills from its own key via `tagged_key`, **namespaced**
so cross-side collisions are impossible.

**Derivation inputs are RAW source-doc field names** (`firm_id`, not
`company_id`): property renames rewrite `vertex.from` maps so documents keep
their original keys, and transform inputs are never rewritten. Pass
`canonical_maps` into `compose_manifests` so a canonical name used as a
derivation input fails loudly instead of silently deriving nothing.

**Exact-name attributes fuse for free.** After boundary rename,
`merge_vertex_models` unions fields by spelling — list a `PropertyEquivalence`
only to rename or to flag identity.

## Priority semantics

With several alignment rows, row order is funnel priority: a record keys by
the **highest-priority attribute it carries**. Two records fuse when their
strongest present attribute coincides. A match on a lower-priority attribute
does *not* fuse records when one of them also carries a higher-priority one —
that trade (deterministic, digest-only, no write-time lookups) is deliberate.

## Run it

No live graph database required.

```bash
cd examples/19-union-canonical-equivalence
uv run python build_union.py                         # → artifacts/manifest_union.yaml
uv run python inspect_fusion.py                      # which records fuse, and to what
uv run python build_union.py --stale-demo            # pre-canonical name → conflict
uv run python build_union.py --conflicting-cluster-demo  # disagreeing into → conflict
```

`inspect_fusion.py` prints one row per emitted vertex doc across the four
resources feeding `Company`.

## Notes

- **Equivalence is declared, never inferred.** The canonical map, the
  `VertexEquivalence` edges, and the `IdentityAlignment` are author-supplied;
  validation only cross-checks the declarations against each other and
  completes missing canonical labels along declared edges.
- **A merge is a stated intent.** A canonical map that collapses two classes
  requires `allow_merges: true`; a compose cluster that collapses several
  classes onto one target requires `allow_implicit_merge=True`.
- **Changing the funnel rekeys the graph** — branch order, ids, and field sets
  all feed the digest (see [Example 17](example-17.md)).

See [Example 17](example-17.md) for identity funnels on a single manifest, and
[Example 18](example-18.md) for *discovering* cross-resource identity instead
of declaring it. Concepts: [Manifest evolution](../concepts/schema/manifest_evolution.md).
