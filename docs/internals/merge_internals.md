# Merge internals

This page is for people who change the code behind `merge_manifests` and its
preview. It records the order in which a union runs and why, how the
vocabulary, the equivalences and `renames` are resolved into one rename per
side, the words the code uses
for combining things, and how a derived identity is lowered to ops. What a
user declares and sees is on [Merging manifests](../concepts/schema/merging_manifests.md).

## The order of a union

`merge_manifests` is one pass with a fixed order (`_merge_manifests` in
`graflo/architecture/evolution/merge.py`). The comments give the reason where
the position matters.

1. **Resolve the names** (`resolve_clusters`, through `build_naming` in
   `naming_graph.py`): the vocabulary, the equivalences and `renames` in one
   pass over the sides' own names. Every problem found is raised together as
   one `MergeNamingError`. The result is one cluster per group, over its closed
   member set, and one composite rename per side.
2. **Fold the vocabulary** happens inside it: the `both` map under the `left`
   map, and under the `right` map (`fold_declared_maps`, through
   `compose_canonical_maps`). Two maps that disagree on a source, or where one
   moves the other's target, are refused.
3. **Synthesize** a group for every exact name both sides still carry under
   `union_right`, and name the groups again, so synthesized groups are
   ordinary groups from here on.

4. **Capture each member's key and property names** before the rename. Once the
   members share one name, the schema no longer says which key came from which
   member, and the merged identity is decided by comparing exactly those.
5. **Rename each side's resources** by `renames.<side>.resources`, then the
   right side's by the collision policy.
6. **Snapshot both sides.** Also before the rename: the rename rewrites a
   router's `type_map` values to the merged name, after which nothing says
   which router key produced which member. A derived identity needs that.
7. **Apply the composite rename** to each side, one `CanonicalizeOp` per side,
   preceded by the `change_field_types` the op's `field_types` lower to on that
   side's own names. Every property type clash no declaration settles is
   refused before this step, naming the members that carry each type.
8. **Prefix the right side's remaining collisions** (`prefix_right` only;
   `error` and `union_right` settled theirs in step 3).
9. **Union schema, ingestion and bindings by name** (`_union_schema`,
   `_concat_ingestion`, `_union_bindings`), combining each name both sides
   carry and deciding the merged identity. A declared `identity` of property
   branches is applied here and checked: every member can fill it, and no
   branch is one no member declares. One with a derived or `local_key` branch
   is only marked: its steps need the assembled manifest.
10. **Refuse a canonical split** (`_assert_no_canonical_split`): no two merged
    types may be spellings of one name.
11. **Bump the version, then lower each derived identity**
    (`_apply_derived_identities`, through `identity_to_ops`).
12. **Demote the members' own keys** of every re-keyed type
    (`_retire_member_keys`), against the type's final identity. This runs after
    the lowering because a derived identity replaces the provisional one, and a
    key demoted against that could be skipped as equal to a primary key that no
    longer exists. Then **turn each resource that can fill no branch into a
    reference** (`_convert_uncovered_producers`): no derived branch names it and
    its members carry no property branch. Then **point reference-only
    resources** at the demoted keys (`_pin_member_references`).
13. **Close each side's routers** over its own types (`router_scope="side"`,
    `_close_side_routers`). Last, so the lowering and the reference conversion
    see the routers as they were.
14. **Apply the op's `name` and `target_namespace`** (`_apply_merge_naming`),
    then `finish_init`.

## How names resolve

`build_naming` (`graflo/architecture/evolution/naming_graph.py`) builds one
graph and checks it; it never raises, so the preview reads the same result.

- **Nodes** are the types and relations each side declares, under their own
  names. They are never renamed before the union, which is what keeps
  per-member maps and router guards meaningful.
- **Edges** come from three declarations: a vocabulary entry (`left:Press →
  Machine`), an equivalence's membership, and a `renames` entry. `union_right`
  adds edges between exact same names.
- **Groups** are the components of the merging edges: a vocabulary sending
  several sources to one target (acknowledged by its `allow_merges`), an
  equivalence, and `union_right`. A component holding an equivalence is a
  cluster. Its members are the closed set, and `declared_left` /
  `declared_right` keep the ones the equivalences name.
- **Names**: a group takes its equivalences' `into`, never translated, else the
  vocabulary's target, else the one spelling its members share. Anything else
  takes its `renames` target, else its vocabulary target, else its own name.
  The op over the vocabulary is a note (`vocabulary_override`), not a refusal.

The checks are predicates on that graph:

| Rule | Finding |
|---|---|
| every merged name is reached by one group or one ungrouped name | `occupied_into`, `shared_into`, or `incomplete` when the stray arrives by a vocabulary entry |
| a group has one name | `unnamed_cluster`, `disagreement` |
| a name is declared in one place | `cluster_overlap` (one type in two equivalences), `double_home` (a `renames` entry for a group member) |
| every declared name exists | `unknown_member`, `dangling` |
| a group has one identity | `identity_disagreement` |
| a vocabulary-joined member can fill a property-only key | `identity_coverage`, repaired by a `local_key` branch that makes the key a funnel |
| a group's merge is acknowledged | `self_relation`, `observation_fusion`; see below |
| a name both sides carry, no group | `name_collision` under `error`, synthesized under `union_right`, prefixed later under `prefix_right` |
| two spellings of one name | `near_collision` under `error` and `union_right`; kept apart under `prefix_right` |

Each finding carries the names it is about and its repairs, safest first:
`rename_away`, `set_into`, `extend_cluster` (which fuses entities),
`add_key_source`, `acknowledge`, `declare_equivalences`. `suggest_merge_op`
applies the first repair an op edit can express, repeatedly; it never applies
`add_key_source` or `acknowledge`, because each decides which records fuse.
The attribute-level checks (`check_property_fields_exist`,
`check_attribute_fixed_points`, `check_property_maps_against_manifest` in
`canonical.py`) run per group, and their refusals are collected as findings.

Every finding's message starts with its `check` phrase. A `MergeNamingError`
carries its findings, so the preview classifies it by their kinds
(`MergeOutcome.kinds`); `kind_for_check` reads only the phrases of refusals
raised after naming.

### Lowering a group

Each cluster's `declaration` is one `VertexEquivalence` over the closed sets:
the property equivalences of all its equivalences, a bare property name
expanded over the members its own equivalence lists, and the one `identity`.
When that identity derives, a member keeps its own key behind `side:Type` for
every resource that produces it and that its stepped branches do not key for
it: a resource whose entries are keyed by other members, a member-keyed local
key that skips it, or, for a member the vocabulary joins, a resource with no
entry at all. A member an equivalence lists whose resource has no entry is
left to the reference conversion of step 12, as is a resource that produces
the member through routers in several roles (`hosts_member_derivation` is
false: one level's transform buffer cannot hold a derived value per role; a
`reference_only` note). Each automatic key is an `auto_local_key` note. Downstream code iterates `cluster.members(side)` and
needs nothing else.

The composite rename per side sends every node of a multi-node component, self
entries included, onto the component's name, and every renamed singleton onto
its new name, all in one `CanonicalizeOp`. A chain or a swap therefore resolves
without an intermediate name. A recorded declaration in removed keys is
translated on read by `lift_recorded_merge_op` in `merge_commit.py`.

### Per name

| Case | Outcome |
|---|---|
| no declaration | unchanged |
| a vocabulary entry or a `renames` entry, target free | renamed |
| either, target an unrelated name kept on that side | `occupied_into` |
| a group member | onto the group's name |
| a group member also in a vocabulary group | the whole vocabulary group joins the cluster |
| a member spelled by a canonical name | every source the vocabulary sends there |
| `into` differing from the vocabulary's name | `into`, with a note |
| a vocabulary entry whose source is an `into` | dangling, with a hint to set `into` instead |
| satisfied: source absent, target present | no-op, a `satisfied` note |
| dangling | one finding per entry; dropped and logged with `allow_dangling_entries` |
| `properties` keyed by a merged name | refused; the map is keyed by the source type |
| `properties` renaming a field the member does not declare, or onto a field it keeps | refused; a property rename cannot merge two fields |
| a property equivalence renaming a canonical attribute | `property_retarget` |
| chain or swap inside one `CanonicalMap` | refused when the map is built; written with `renames` instead |

Two declared maps chaining (`{Z: Q}` and `{X: Z}`) are refused by
`compose_canonical_maps`, in either order. Resource and connector names are
matched exactly; `union_right` behaves as `error` for them. Two property names
that key alike are never combined.

A synthesized group goes through the same identity reconciliation as a declared
one: two same-named types whose keys disagree raise `MergeIdentityError`, and
the right side's properties are unioned rather than dropped.

The observation-fusion guard is judged per accumulator slot, which is what the
runtime fuses on: a vertex step stores at its `role` sub-slot when it has one
and at the bare level otherwise, and a router at its `role` (or `type_field`).
Both guards are judged per group and per side in the naming pass, on that
side's manifest before the relabel (`merge_self_relations`, `merge_fused_slots`
in `apply.py`). A group accepts what one of its equivalences lists in `allow`.
A side's vocabulary accepts, through `allow_self_relations` and
`allow_observation_fusion`, only the merges it makes itself. The per-side
`CanonicalizeOp` then sets both flags, since the judgement is already made.

## Words for combining things

Five verbs recur in the code, in seven senses:

| Verb | Sense | Where |
|---|---|---|
| merge | join two manifests of unrelated lineage by declared equivalence (the union) | `merge_manifests`, `MergeManifestsOp` |
| union | assemble two collections by name: the outer step | `_union_transforms`, `_union_schema`, `_union_bindings` |
| merge | combine the two definitions one name has, refusing conflicts: the inner step | `merge_vertex_models`, `merge_edge_pair`, `merge_semantics` |
| merge (collapse) | send several distinct types or relations to one name | `MergeVerticesOp`, `MergeEdgesOp`, `CanonicalMap.allow_merges` |
| merge3 | reconcile two descendants of a common ancestor | `merge_three_way`, `MergeResult`, `MergeConflict` |
| fuse | two records becoming one node when cast | `allow_observation_fusion`, a derived identity |
| collapse | group members arriving at their merged name | `VertexEquivalence`, `CanonicalizeOp` |

Union and merge are the two levels of one operation: the union walks the names,
and the merge is what it does at a name both sides carry. `_union_transforms`
is the plain case: it folds a registry by name, keeping one copy of an identical
body and refusing two different ones. `_union_schema` is a union and the merges
it drives. `_concat_ingestion` is neither: resource name collisions are settled
earlier, so the two lists only concatenate.

Collapse and merge at a name share one implementation: `merge_vertex_models` is
called by both `MergeVerticesOp` and the union, because at the field level they
are the same work. What differs is the author's claim about the inputs: an
equivalence's member list, or a vocabulary's `allow_merges`.

Inside `evolution/merge3.py` and `plot/merge3.py`, a bare "merge" is the
three-way merge; everywhere else it is the union. The qualifier is spelled out
where the two would otherwise look alike: the commit kind (`merge` and
`merge3`), the CLI verb (`graflo merge` and `graflo merge3`), and `MergePreview`
and `Merge3Preview`. Some names in `merge3.py` keep a bare "merge"
(`merge_three_way`, `find_merge_base`, `MergeResult`, `MergeConflict`,
`re_merge`, `merged_hash`).

### Merge, compose and aliases

The names follow the generic model-management operators: Merge takes two
models plus correspondences, and Compose takes two mappings. `compose` names
one thing in the package, `compose_canonical_maps`.

Some unary ops accept a second spelling (`vertices` for `renames` on
`rename_vertices`, `allow_row_fusion` on `merge_vertices`, for example); each
alias is listed in the field's description. `MergeManifestsOp` accepts none: a
removed key is refused with its replacement named.

## How the preview stays complete

`preview_merge` is not a second implementation of the rules. It reads the
naming graph and findings of `build_naming`, the same pass the union raises
from, and then runs the schema-union kernels itself, one group at a time
(`merge_vertex_models`, `merge_edge_pair`), so a refusal on one group does not
hide the next. A `MergeNamingError` that lists several problems marks each
matching finding as a refusal.

The tests hold it to one invariant: whatever the union refuses, the preview has
a finding of a matching kind. It fails closed. A refusal the preview cannot
classify fails the suite instead of being skipped, so a new rule cannot be added
without a finding kind to report it under. A naming finding carries its kind;
any other refusal is classified by the `check` phrase every `Refusal` in the
union path carries, mapped to a finding kind in `preview.py`.

## How a derived identity is lowered

`identity_to_ops` (`graflo/architecture/evolution/alignment.py`) turns an
`IdentityPlan` -- the equivalence's branches, its `derive` attributes, and
`derive_at` -- into basic ops, in this order:

1. `AddVertexPropertiesOp`: declare the derived attributes (`derive` entries,
   then derived branches) and the local key.
2. `AddResourceTransformsOp`: the derivation steps per resource, with inline
   calls.
3. `EnsureExtractedFieldsOp`, when a producing router restricts `keep_fields`
   or `extraction_scope`.
4. `ReplaceIdentityOp`: a funnel with one branch per declared branch, in
   declared order, with `retire: keep`. Property branches emit no steps; they
   only enter the funnel. The union demotes each member's own key itself,
   afterwards.

**Derivations land at the producing level.** `identity_to_ops` resolves the
pipeline level that produces the type and sets `AddResourceTransformsOp.at`. An
actor reads its transform buffer at its own `LocationIndex` with no fallback to
its ancestors, a `descend` subtree runs before its own level's transforms, and
a transform whose inputs are missing is skipped without an error by default. A
derivation appended at the root of a nested pipeline would therefore derive
nothing. A resource producing the type at several levels raises unless
`derive_at` picks one.

**Behind a router, delivery goes through the merged observation.** A router
builds its child `VertexActor` at `lindex.extend((role, 0))`, where the transform
buffer is empty, so derived attributes arrive by passthrough or `from`, subject
to `keep_fields` and `extraction_scope`. Because that observation reaches
whichever type the router picks, every derivation behind a router is guarded:
member-keyed ones per member, an unkeyed one on the discriminator values that
route onto the type (the `type_map` keys, or its own name for pass-through).
A sibling type declaring the same attribute name is then never handed the
derived value. A refusal remains where no guard can be derived and the spec
sets none: a plain `vertex` step producing the type beside the router. Routers
reading different discriminators at one level, the roles of an edge resource,
are refused outright, `when` or not: the level's transform buffer is shared by
every router at it, so one derived attribute cannot hold a different value per
role. Such a resource stays out of the sources and is converted into a
reference (step 12).

**The guard is derived from the sides.** After the per-side rename, the union
cannot say which router key produced which member, but the snapshots from step
6 of the union can, and `merge_manifests` passes them to the lowering
(`sides=`). For each resource and member, a plain `vertex` step needs no guard;
a router yields a `when` guard on its discriminator listing the keys that map to
the member, or the member's own name for a router with no entry for it. An
explicit `when` on an unkeyed spec replaces the derived guard, so an author's
narrower guard is never widened; a member-keyed spec may not set one.

**One writer per attribute.** A guarded step that does not fire writes nothing,
so each spec's step is the only writer of the attribute for the records its
guard admits, with no scratch fields and no coalesce. The guard cannot be a
function returning `None`: behind a router, a later `None` overwrites an earlier
real value in the merged observation.

**Why closed routers list their side's types.** `_close_side_routers` writes
every type of a router's side into its `type_map`, the unrenamed ones as
themselves, and sets `type_map_only`. The self-entries carry meaning: a closed
router skips any value its table does not name, so a pass-through router with
no table would skip every row, and the static analyses
(`step_produces_vertices`, `find_vertex_producing_levels`, the lookup-only
rewrite) read the table as the types the router produces. A router with
`vertex_types` gets self-entries only for the types it lists.

**Routers evolve through one rewrite.** Rename, merge, canonicalize and removal
all pass a router through `evolve_router`, which maps its table, list,
projections and lookups, then checks every value whose route the op can change
(table keys, listed and mapped types, the op's names) against the router before
the op. When the rewritten fields route a value differently -- a list that
cannot tell merged members apart, a removed table entry whose key names a type
-- it closes the router over the values it accepted and rewrites that instead.
A merge of types the router projects differently (each its own
`vertex_from_map` entry, or the shared `from`) cannot keep one projection per
type, so the router is split into closed routers by projection; their values
stay disjoint, so no row is routed twice.

## What to read next

- [Merging manifests](../concepts/schema/merging_manifests.md): the union as a
  user declares and runs it.
- [Version control](../concepts/schema/versioning.md): the three-way merge and
  how unions are recorded.
- [Contributing](../contributing.md): how to run the tests.
