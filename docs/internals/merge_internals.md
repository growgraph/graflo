# Merge internals

This page is for people who change the code behind `merge_manifests` and its
preview. It records the order in which a union runs and why, how declared maps
and equivalences are resolved into one rename per side, the words the code uses
for combining things, and how identity alignments are lowered to ops. What a
user declares and sees is on [Merging manifests](../concepts/schema/merging_manifests.md).

## The order of a union

`merge_manifests` is one pass with a fixed order (`_merge_manifests` in
`graflo/architecture/evolution/merge.py`). The comments give the reason where
the position matters.

1. **Fold the declared maps** per side: the `both` map under the `left` map, and
   under the `right` map (`fold_declared_maps`, through
   `compose_canonical_maps`). Two maps that disagree on a source, or where one
   moves the other's target, are refused.
2. **Resolve the clusters** against those maps (`resolve_clusters`): the members
   in each manifest's own spelling, one merged name each, and one composite
   rename per side.
3. **Synthesize** a cluster for every name both sides still carry, as
   `name_conflict` directs, and resolve again, so synthesized clusters are
   ordinary clusters from here on.
4. **Capture each member's key and property names** before the rename. Once the
   members share one name, the schema no longer says which key came from which
   member, and the merged identity is decided by comparing exactly those.
5. **Rename the right side's resources** by `resource_renames`, then by the
   collision policy.
6. **Snapshot both sides.** Also before the rename: the rename rewrites a
   router's `type_map` values to the merged name, after which nothing says
   which router key produced which member. Identity alignment needs that.
7. **Apply the composite rename** to each side, one `CanonicalizeOp` per side.
8. **Prefix the right side's remaining collisions** (`prefix_right` only;
   `error` and `union_right` settled theirs in step 3).
9. **Union schema, ingestion and bindings by name** (`_union_schema`,
   `_concat_ingestion`, `_union_bindings`), combining each name both sides
   carry, deciding the merged identity, and checking that every member can fill
   it.
10. **Refuse a canonical split** (`_assert_no_canonical_split`): no two merged
    types may be spellings of one name.
11. **Bump the version, then apply `identity_alignments`.**
12. **Demote the members' own keys** of every re-keyed type
    (`_retire_member_keys`), against the type's final identity. This runs after
    the alignments because an alignment replaces the identity, and a key
    demoted against the identity before it could be skipped as equal to a
    primary key that no longer exists. Then **turn each resource an alignment
    derives nothing for into a reference** (`_convert_uncovered_producers`), and
    **point reference-only resources** at the demoted keys
    (`_pin_member_references`).
13. **Close each side's routers** over its own types (`router_scope="side"`,
    `_close_side_routers`). Last, so the alignments and the reference
    conversion see the routers as they were.
14. **Apply the op's `name` and `target_namespace`** (`_apply_merge_naming`),
    then `finish_init`.

## How maps and equivalences resolve

`resolve_clusters` turns the declared maps and the equivalences into one
`CanonicalizeOp` per side, applied before the union by name. The terms the code
uses:

| Term | Type | Meaning |
|---|---|---|
| declared map | `CanonicalMap`, folded into `DeclaredMaps` | a map the author wrote, in `op.canonical_maps[scope]` or passed to `merge_manifests` |
| cluster | `Cluster`, resolved from `ClusterSpec` | one equivalence, resolved: its members per side in the manifests' own spelling, and its merged name |
| cluster map | | per side, every member onto its merged name, the merged name itself included, so the op merges into it rather than refusing an occupied target |
| composite map | `SideMaps`, one `CanonicalizeOp` per side | the cluster map plus every declared entry that applies to a non-member. A relabel, not a vocabulary: two clusters may chain when one merged name is renamed by another declaration |
| fixed point | | a canonical target; no map and no cluster may move it |
| opinion | | what the declared maps say a member's canonical name is: its target, or itself when it is a fixed point |
| satisfied entry | | a declared entry whose source is absent and whose target is present on a side; taken as already applied and logged |
| dangling entry | `DanglingEntry` | a declared entry matching nothing on any side it could apply to |
| synthesized cluster | `Cluster.synthesized` | a cluster the union declares itself under `union_right` |
| completion | `Completion`, on `MergeIncompleteError` | the extension that would make an incomplete declaration consistent |

Canonicalizing a side first and then declaring clusters in canonical names is
the same function as declaring them in raw names on an op that carries the map,
because renames compose; `merge_manifests` accepts either. Canonicalizing the
union afterwards is a different function in general (it can collapse two merged
names) and is a separate `CanonicalizeOp` on the result.

One rule underlies every refusal: the maps and the equivalences must agree on
where a name goes, and a canonical target is a fixed point neither may move.

### Per name

For one name on one side, with `E` its cluster and `C` the declared entry that
names it:

| Case | Outcome |
|---|---|
| neither | unchanged |
| `C` only, target free or declared by a self entry | carried into the composite map |
| `C` only, target an unmoving non-member without a self entry | refused by the op (occupied target) |
| `E` only | onto the merged name |
| `E` and `C` agree; `E` without `into` and `C` names a member; `into` itself in the domain of `C` | onto the merged name, which `C` supplies or translates |
| `E` names a member the side does not declare | `UnknownMemberError`, naming another spelling that denotes the same concept when there is one |
| `E` with no `into`, no mapped member and no shared spelling | refused as an unnamed cluster |
| contradiction: `E` and `C` disagree; `E` moves a fixed point; a property equivalence renames a canonical attribute | `MergeCanonicalConflictError`, naming both declarations |
| ambiguity: a canonical name denotes two members; maps disagree on translating `into` | `MergeCanonicalConflictError` |
| incomplete: `C` sends a non-member onto a merged name | `MergeIncompleteError`; the completion is the cluster extended with that member |
| one-sided `both` entry | applied where it matches |
| `both` entry over a merged name | a translation of `into`, not a dangling entry |
| dangling | refused; one refusal lists every dangling entry on the side. It outranks an incomplete refusal on the same side, since a name that is absent is the more basic mistake |
| dangling, with `allow_dangling_entries` | dropped and logged |
| satisfied | no-op, logged |
| `properties` keyed by a merged or canonical type | refused; the map is keyed by the source type |
| `properties` renaming a field the member does not declare, or onto a field it keeps | refused; a property rename cannot merge two fields |
| chain or swap inside one map | refused when the `CanonicalMap` is built |

### Across the two sides

Decided after each side's composite map has been applied:

| Case | Outcome |
|---|---|
| a name both sides carry, no cluster | `error`: `MergeIncompleteError` whose completion declares the equivalence in each side's own spelling; `union_right`: a synthesized cluster; `prefix_right`: `r_<name>` |
| two spellings of one name (`OrderLine` / `order_line`) | `error`: `MergeNameConflictError`; `union_right`: a synthesized cluster under the left spelling; `prefix_right`: kept apart |
| a resource or connector name both sides carry | matched exactly only; `union_right` behaves as `error` |
| two property names that key alike | never combined; only exact spellings are |
| two declared maps chaining (`{Z: Q}` and `{X: Z}`) | refused by `compose_canonical_maps`, in either order |

A synthesized cluster goes through the same identity reconciliation as a
declared one: two same-named types whose keys disagree raise
`MergeIdentityError`, and the right side's properties are unioned rather than
dropped.

`ClusterConflictError` covers the shape of the declarations: a type claimed by
two clusters, two clusters sharing one merged name, a merged name occupying an
existing non-member type. It is raised unwrapped, before any rename, because an
op whose own declarations conflict is broken whatever the maps say.

The observation-fusion guard is judged per accumulator slot, which is what the
runtime fuses on: a vertex step stores at its `role` sub-slot when it has one
and at the bare level otherwise, and a router at its `role` (or `type_field`).
`allow_self_relations` and `allow_observation_fusion` are forwarded from the op
to the per-side `CanonicalizeOp`.

## Words for combining things

Five verbs recur in the code, in seven senses:

| Verb | Sense | Where |
|---|---|---|
| merge | join two manifests of unrelated lineage by declared equivalence (the union) | `merge_manifests`, `MergeManifestsOp` |
| union | assemble two collections by name: the outer step | `_union_transforms`, `_union_schema`, `_union_bindings` |
| merge | combine the two definitions one name has, refusing conflicts: the inner step | `merge_vertex_models`, `merge_edge_pair`, `merge_semantics` |
| merge (collapse) | send several distinct types or relations to one name | `MergeVerticesOp`, `MergeEdgesOp`, `allow_merges` |
| merge3 | reconcile two descendants of a common ancestor | `merge_three_way`, `MergeResult`, `MergeConflict` |
| fuse | two records becoming one node when cast | `allow_observation_fusion`, identity alignment |
| collapse | cluster members arriving at their merged name | `VertexEquivalence`, `CanonicalizeOp` |

Union and merge are the two levels of one operation: the union walks the names,
and the merge is what it does at a name both sides carry. `_union_transforms`
is the plain case: it folds a registry by name, keeping one copy of an identical
body and refusing two different ones. `_union_schema` is a union and the merges
it drives. `_concat_ingestion` is neither: resource name collisions are settled
earlier, so the two lists only concatenate.

Collapse and merge at a name share one implementation: `merge_vertex_models` is
called by both `MergeVerticesOp` and the union, because at the field level they
are the same work. What differs is the author's claim about the inputs, and
`allow_merges` is where it is made.

Inside `evolution/merge3.py` and `plot/merge3.py`, a bare "merge" is the
three-way merge; everywhere else it is the union. The qualifier is spelled out
where the two would otherwise look alike: the commit kind (`merge` and
`merge3`), the CLI verb (`graflo merge` and `graflo merge3`), and `MergePreview`
and `Merge3Preview`. Names in `merge3.py` that
predate the qualifier (`merge_three_way`, `find_merge_base`, `MergeResult`,
`MergeConflict`, `re_merge`, `merged_hash`) keep a bare "merge".

### History of the names

The generic model-management operators this design follows use Merge for two
models plus correspondences, and Compose for two mappings. GraFlo first spelled
them the other way round: `merge_manifests` was `compose_manifests`, and
`compose_canonical_maps` was `merge_canonical_maps`. After the swap, `compose`
names one thing in the package, `compose_canonical_maps`.

The `name_conflict` value `union_right` was spelled `fuse_right` before "fuse"
was reserved for records; the old spelling is still accepted as input.
`allow_observation_fusion` accepts `allow_row_fusion`, and several ops accept
older field names (`vertices` for `renames` on `rename_vertices`, for example);
each alias is listed in the field's description.

## How the preview stays complete

`preview_merge` is not a second implementation of the rules: each check calls
the function the union itself calls, one declaration or one map entry at a time,
so a refusal on one unit does not hide the next. That includes the schema
union: the preview runs `merge_vertex_models` and `merge_edge_pair` per cluster.

The tests hold it to one invariant: whatever the union refuses, the preview has
a finding of a matching kind. It fails closed. A refusal the preview cannot
classify fails the suite instead of being skipped, so a new rule cannot be added
without a finding kind to report it under. A refusal is classifiable by the
`check` phrase every `Refusal` in the merge and union path carries, mapped to a
finding kind in `preview.py`.

## How an identity alignment is lowered

`alignment_to_ops` turns one `IdentityAlignment` into basic ops, in this order:

1. `AddVertexPropertiesOp`: declare the canonical attributes.
2. `AddResourceTransformsOp`: the derivation steps per resource, with inline
   calls.
3. `EnsureExtractedFieldsOp`, when a producing router restricts `keep_fields`
   or `extraction_scope`.
4. `ReplaceIdentityOp`: a funnel with one branch per attribute in order, then
   the `local_key` branch, with `retire: keep`.
5. `AddSecondaryIdentitiesOp`: the alignment's `secondary_identities`. The
   union demotes each member's own key itself, afterwards.

**Derivations land at the producing level.** `alignment_to_ops` resolves the
pipeline level that produces the type and sets `AddResourceTransformsOp.at`. An
actor reads its transform buffer at its own `LocationIndex` with no fallback to
its ancestors, a `descend` subtree runs before its own level's transforms, and
a transform whose inputs are missing is skipped without an error by default. A
derivation appended at the root of a nested pipeline would therefore derive
nothing. A resource producing the type at several levels raises unless
`IdentityAlignment.at` picks one.

**Behind a router, delivery goes through the merged observation.** A router
builds its child `VertexActor` at `lindex.extend((role, 0))`, where the transform
buffer is empty, so derived attributes arrive by passthrough or `from`, subject
to `keep_fields` and `extraction_scope`. Because that observation reaches
whichever type the router picks, every derivation behind a router is guarded:
member-keyed ones per member, an unkeyed one on the discriminator values that
route onto the type (the `type_map` keys, or its own name for pass-through).
A sibling type declaring the same attribute name is then never handed the
derived value. A refusal remains only where no guard can be derived: a plain
`vertex` step producing the type beside the router, or routers reading different
discriminators at one level.

**The gate is derived from the sides.** After the per-side rename, the union
cannot say which router key produced which member, but the snapshots from step
6 of the union can, and `merge_manifests` passes them to the alignment
(`sides=`). For each resource and member, a plain `vertex` step needs no gate;
a router yields a `when` guard on its discriminator listing the keys that map to
the member, or the member's own name for a router with no entry for it.

**One writer per attribute.** A guarded step that does not fire writes nothing,
so each member's step is the only writer of the attribute for its own records,
with no scratch fields and no coalesce. The gate cannot be a function returning
`None`: behind a router, a later `None` overwrites an earlier real value in the
merged observation. That is also why the list form of `sources` lowers to one
scratch field per derivation plus a `coalesce_fields` step as the single writer.

`LocalKeySource.gate` and `gate_prefix` exist for an alignment applied outside a
union, where no side exists to derive the gate from; a member-keyed source may
not set them.

## What to read next

- [Merging manifests](../concepts/schema/merging_manifests.md): the union as a
  user declares and runs it.
- [Version control](../concepts/schema/versioning.md): the three-way merge and
  how unions are recorded.
- [Contributing](../contributing.md): how to run the tests.
