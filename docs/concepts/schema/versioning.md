# Version control

A manifest changes over time, often in more than one hand. This page shows how
GraFlo records that history: how a manifest gets a content address, how change
sets become commits, how you go back to an earlier version, and how two lines
of change to the same manifest are reconciled. After reading it you can keep a
manifest's history in a commit store and replay, verify and merge it from the
shell or from Python.

Two operations here combine manifests, and they are different. A three-way
merge reconciles two branches of one manifest's history (`merge_three_way`,
`graflo merge3`, the [version control example (22)](../../examples/version-control/index.md));
a union combines two unrelated manifests (`merge_manifests`, `graflo merge`, the
[manifest union example (20)](../../examples/manifest-union/index.md)), and is
described on [Merging manifests](merging_manifests.md).

History moves forward, like a git log, not like a migration script with an
`upgrade()` and a `downgrade()` for every step. A reversible pair per step is
not possible here: `merge_vertices` discards which source each property came
from, `change_field_types` discards the previous type, `sanitize` overwrites the
storage names it replaces, and `project_manifest` drops material outright. A
`downgrade` that produces a different manifest from the one you started with is
worse than none. So going back means replaying from an earlier version.

Nothing here touches a database. A history records changes to the manifest; see
[Schema migration](../operations/migration_and_practices.md) for changes to a
database.

## Content addressing

Two manifests that describe the same graph hash equal:

```python
from graflo.architecture.evolution import manifest_hash

manifest_hash(a) == manifest_hash(b)  # same graph, however each was reached
```

The hash covers the schema, the ingestion model and the bindings. It leaves out
the manifest's name, version and provenance. `to_minimal_canonical_dict()`
already ignores defaults, `None` values, aliases and key order. What it keeps is
list order, and most lists in a manifest are sets written in some order, so two
identical schemas authored in different orders, or one authored and one
replayed, would hash differently. `canonical_payload` adds that normalization.

### Sorted or preserved

Which lists may be sorted is decided per field, in `LIST_ORDER`, which
classifies every list field reachable from `GraphManifest`.

The two possible mistakes are not equally bad. Sorting a list whose order
matters makes two different manifests hash equal, and nothing downstream can
detect it. Preserving the order of a list whose order does not matter only
misses a match, which is visible and harmless. So a doubtful field is
preserved, and every sorted field states why its order does not matter.

| Sorted | Preserved |
|---|---|
| vertices, edges, properties, secondary identities | resource pipelines (an ordered program) |
| resource and transform registries, bindings entries | `Vertex.identity`: a database addresses an endpoint through the first key field |
| index sets, edge specs | the columns of a compound index |
| semantic `exact_match` / `synonyms` | identity funnel branches (the first branch that fires wins) |
| selector and membership sets | transform arguments, join and projection order, filter operands |

Sorting uses each element's canonical JSON rendering rather than a key per
field, so it works for mixed element types, needs no tie-break rule, and cannot
depend on the input order. Only the hash sees the sorted order; authored YAML
keeps its own order.

A list field that is not classified raises `UnclassifiedListField` rather than
being guessed. `CANON_VERSION` is part of the hashed bytes, so a change to these
rules produces different hashes instead of reading old hashes under new rules.

## Provenance

The content address and lineage can travel with the manifest, so a shipped file
describes itself:

```yaml
metadata:
    provenance:
        content_hash: "…64 hex…"
        canon: graflo/canon@3
        parents: [a3f9c21e4b70, 9e11d02c55aa]
        commit: c4d1e9a2b3f0
        merge_recipe: "…"
```

Provenance is never part of the content hash. Content identity must not depend
on the path taken, so that two routes to the same manifest agree that they
arrived at the same place. A hash over the parents would make identity depend
on history, and equal manifests could never be recognized as equal. The commit
id is the hash that covers ancestry, as in git.

Stamping is explicit: `stamp_provenance(...)` or `graflo stamp` at a commit
point, never a side effect of `apply_evolution`. Applying the same ops twice
must not produce two files that disagree about their own lineage.

## Commits

```python
from graflo.architecture.evolution import History, build_commit, checkout

first = build_commit(base, ops, label="add commissioning date")
second = build_commit(after_first, more_ops, parents=[first.id], label="rekey")
history = History(commits=[first, second])

restored = checkout(base, history)  # replays, verifying every tree
as_of_first = checkout(base, history, first.id)
```

A `Commit` carries its ops, its parents (none for a root, one for an edit, two
or more for a merge) and the content hash before and after it. `build_commit`
applies the ops rather than trusting them, so both hashes describe a change
that actually happened, and it refuses a change set that leaves the manifest
unchanged, since such a commit would record nothing.

A commit id is derived from the ops and the order of the parents, so recording
the same change set again yields the same id rather than a duplicate under a new
name. `graflo rehash` recomputes every id when an op's serialization changes.

### Forks are recorded

Two commits may share a parent. `History` rejects what is broken (a duplicate
id, a parent that does not exist, a cycle, a first-parent edge whose hashes do
not line up) and records everything else, including several heads. Two people
changing the same version is normal, so the history records both branches:

```python
history.heads()  # more than one means it has forked
history.linearize()  # raises when there is no single path
history.topological()  # always available, deterministic on ties
```

### Merge commits store a diff from the first parent

A merge commit's `ops` are the diff from its first parent to the merged result,
not an interleaving of both sides. So replay and hash verification work the
same way for edit and merge commits, and nothing downstream needs a special
case. The record of how the merge was resolved is stored beside it as a recipe.

### Undoing

History is append-only, so undoing a change moves forward:
`build_revert_commit` (`graflo revert`) records a new commit that applies the
inverses. Inversion is exact or it fails: an op with no inverse, or one whose
inverse needs data the current manifest no longer holds, raises rather than
producing a manifest that only resembles the earlier one. When the base
manifest is available, checking out the parent commit is always exact and is
the better tool.

| Reversible | Irreversible |
|---|---|
| add and remove: vertices, edges, vertex and edge properties, indexes | `merge_vertices`, `merge_edges` |
| rename: vertices, relations, resources, properties; `canonicalize` that only renames | `change_field_types`, `canonicalize` that merges |
| `set_edge_directed`, `retarget_edges`, `add_inverse_edges`, `set_native_inverses`, `set_inverse_emission`; `declare_edge_inverses` and `retract_edge_inverses` | `sanitize`, `project_manifest` |
| `replace_identity` (with `retire: keep`), secondary identities | `add_resource_transforms`, `ensure_extracted_fields`, `merge_manifests` |

Whether an op can be undone depends on the op and on the manifest it met.
`invert_op` applies its candidate inverse and offers it only when the result
has the content hash of the state before, so an inverse is exact or absent:

- a `remove_vertices` that also removed edges, profile entries or pipeline steps
  has no inverse, because adding the vertex back restores none of them. Remove
  the edges first, as `diff_manifests` does, and each step can be undone;
- an op on a relation's properties has no inverse when the relation's edges
  disagreed about the property beforehand;
- a property rename onto a name already taken folds two properties into one,
  and renaming back cannot make them two again;
- an op the manifest refuses has no inverse, because nothing was done.

A removed property is restored as the property it was, with its type,
description and grounding.

## Merging two branches

```python
from graflo.architecture.evolution import find_merge_base, merge_three_way, take_left

base_id = find_merge_base(history, left_id, right_id)
merged, result = merge_three_way(ancestor, left, right)
if not result.clean:
    merged, result = merge_three_way(
        ancestor, left, right, resolutions=[take_left(result.conflicts[0])]
    )
```

A three-way merge is not a diff. Both sides descend from a common ancestor, so
the question is not what differs between them but what each side changed, and
whether those changes collide. When `find_merge_base` returns `None`, the two
share no ancestor, and the operation you want is a
[union](merging_manifests.md), not a three-way merge.

### Slots

Changes are reconciled per slot: the place in the manifest an op touches, such
as `vertex/machine/field/serial_number`. Changes to different slots merge on
their own, the same change on both sides merges once, and different changes to
one slot are a `MergeConflict`. A conflict carries both sides' ops and the
ancestor's state, because what the slot looked like before either change is
what a person resolving it needs to know, and a two-way diff cannot show it.

Three rules make the slot the right unit:

- **An ordered list is one slot.** A resource pipeline is an ordered program,
  and merging half of each side's edits to it produces a program neither author
  wrote.
- **A rename occupies both names.** Renaming `machine` to `equipment` while the
  other side adds a property to `machine` is a real collision, visible only if
  the rename counts as touching the old slot too.
- **An op touching several slots is atomic.** If any of them is contested, the
  whole op is held back. Applying half an op is not a merge.

Slots nest, so `vertex/machine` contains `vertex/machine/field/serial_number`,
and they are keyed on the canonical name: `work_order` and `WorkOrder` occupy
the same slot and conflict, rather than merging into two unrelated types with
the data split between them.

Edges nest under their relation: `relation/feeds` contains
`relation/feeds/edge/machine/line`. Ops address edges two ways, by relation
name (`remove_edges`, `rename_relations`, `merge_edges`) and by triple
(`set_edge_directed`, `retarget_edges`, the index and identity ops), and
nesting is what lets the two see each other: removing a relation on one side
conflicts with flipping one of its edges on the other. A relation-wide property
edit (`relation/feeds/field`) and a per-edge edit do not collide, and merge. An
edge with no relation has its own root (`edge/machine/line`), since no op
addressed by relation can reach it. A declared inverse sits under both of its
relations (`relation/feeds/inverse`), so renaming or removing a relation
conflicts with declaring or retracting its inverse on the other side; a
symmetric declaration sits under its one relation. A native inverse is a slot
of its relation (`relation/feeds/native_inverse`), as on TigerGraph, where the
reverse type belongs to the relation's edge type.

#### What an op reads

A slot is what an op writes. That alone does not tell whether two ops are
independent: `add_edges` writes an edge and depends on its endpoint types,
while a `remove_vertices` on the other side, which also removes that type's
edges, writes a different slot. Merged on written slots only, one order of the
sides would drop the new edge without a word and the other would fail to
apply. So an op also has a read set (`op_reads`):

| Op | Reads |
|---|---|
| `add_edges`, `retarget_edges`, and every op addressed by edge triple | the endpoint types (old and new, for a retarget) |
| ops addressed by relation: edge properties, `remove_edges`, `rename_relations`, `merge_edges`, the inverse ops | the types that relation connects in the base; the op names only the relation |
| `replace_identity`, `add_secondary_identities` | the fields they key on, and those fields' types |
| `add_vertex_indexes`; edge index and identity ops | the fields they index or key on |

A read is disturbed by a write at or above it, never below: an edge onto `line`
conflicts with removing or renaming `line`, and merges with a new property on
it. Two ops reading the same thing are independent, like two edges onto one
type. An op both sides made is agreement, not a dependency. The conflict is
reported at the written slot with both ops attached, and is resolved like any
other. `ops_independent(a, b, base)` is the test the merge and its tests share.

A change that no op expresses, such as one of a relation's edges gaining a
property its siblings lack, or an edited pipeline, cannot be merged at all: the
merge is built from each side's ops, so the result would lack it.
`merge_three_way` raises `MergeError` naming what is left over rather than
return a result that looks clean and is incomplete.

### Determinism

The same inputs produce the same merged manifest, the same conflicts in the same
order, and the same content hash. A resolution takes the place of the ops it
replaces rather than being appended, because op order is a precondition:
`diff_manifests` emits an identity change before the secondary-identity
addition that depends on it.

### Union and three-way merge

| | Three-way merge | Union |
|---|---|---|
| Inputs | two descendants of a common ancestor | two unrelated manifests |
| Names | expected to agree; a disagreement is a conflict | expected to differ; declared equivalences reconcile them |
| Function and command | `merge_three_way`, `graflo merge3` | `merge_manifests`, `graflo merge` |
| Order of the sides | significant: the commit's ops are the diff from the first parent | significant in six fields only; see below |

Both produce commits with several parents. The stored commit `kind` and the
CLI verb spell the three-way merge `merge3` so that the two stay apart.

#### What depends on the order of the sides

A union of B onto A and of A onto B has the same content hash for everything
the union assembles. Every list it concatenates (vertices, edges, resources,
transforms, connectors, grounding, indexes) is sorted in the canonical form, so
the order in which the two sides were walked disappears before anything is
hashed. Metadata is not hashed at all, so the combined name (`a+b`), the joined
description and the version do not change the content address either.

Six fields do depend on the order, all set when the two definitions of one type
are combined. They are the fields the canonical form preserves, because their
order carries meaning:

- `Vertex.identity`: the column order of a composite key;
- `Vertex.hash_identity_properties`: the input to the identity digest;
- `Vertex.filters`;
- `IdentityFunnel.branches`: branch order is the key's fallback order;
- secondary identities named automatically (`secondary_0`, `secondary_1`), by
  position;
- `Vertex`, `Edge` and `Field` descriptions, joined in side order.

Where a value is a claim rather than an order, the union refuses instead of
choosing a side: two declared `db_flavor` values raise, a disputed `iri` is
cleared, and conflicting storage names, field types and units raise.

### A union is recorded too

A union joins two lines of history that share no ancestor, so both inputs must
already be commits in the store: `graflo commit --root` starts the second line
rather than extending the first. The commit stores the diff from its first
parent, like any merge commit, so `checkout` and hash verification need no
special case. Its recipe records the whole declaration (equivalences with their
identities, canonical maps) and no merge base, because there is none. A recipe
whose declaration the op model no longer loads is refused as a `CommitError`
that says so. Because
the commit is a diff, every block the union changes needs an op; `set_bindings`
and `set_db_profile` cover the bindings and the database profile.

## Tracked merges

A `MergeRecipe` records how a three-way merge was resolved. It is
content-addressed, with its resolutions hashed in slot order:

```python
from graflo.architecture.evolution import build_recipe, re_merge

recipe = build_recipe(ancestor, left, right, resolutions=resolutions)
merged, result = re_merge(recipe, ancestor, advanced_left, right)
```

When the left side moves on, `re_merge` replays the recorded decisions and
reports only conflicts that are new. You keep an overlay on top of a manifest
that changes, and decide each conflict once.

A recorded resolution whose slot no longer conflicts is reported as unused and
never applied: applying an old decision to a slot nobody contests any more
would undo someone's later change without anyone noticing.

## CLI

```bash
graflo commit --from-manifest base.yaml --to-manifest target.yaml -m "add work orders"
graflo log --graph
graflo verify --base base.yaml --against target.yaml
graflo checkout <commit> --base base.yaml --output-path out.yaml
graflo merge3 <left> <right> --base base.yaml --take left
graflo revert <commit> --base base.yaml
graflo stamp manifest.yaml --commit <commit>
graflo merge A.yaml B.yaml -o AB.yaml -m "join"   # a union, recorded as a two-parent commit
```

`graflo commit` derives the ops from two manifests with `diff_manifests` and
refuses to store a change set that does not reproduce the target; `--hints`
names renames the differ cannot infer. `graflo merge3` stops at conflicts
unless `--take left` or `--take right` settles them; `--plot` draws where the
two branches met. Commits live under `.graflo/commits` by default, one YAML
file per commit (`--store` changes it). The store rebuilds the graph of commits
from the recorded parent ids, not from the file names.

These commands record and replay changes to a manifest. `graflo migrate-schema`
is different: it plans and applies changes to a database.

## Rules and limits

- A commit history changes manifests only. Applying it to a live database is
  not part of version control; `graflo migrate-schema` plans database changes
  and applies additive ones.
- A change no op expresses, such as an edited pipeline step, cannot be recorded
  from a diff or merged three ways; both refuse and name it.

## Further reading

The mechanisms on this page have prior art; the differences are stated so you
know what to compare against.

- Curino, Moon, Zaniolo: *Graceful Database Schema Evolution: the PRISM
  Workbench*, PVLDB 1(1), 2008. Schema-modification operators with an inverse
  per operator, for relational schemas. GraFlo's inverses are computed against
  the manifest before the change instead, and are refused when that manifest
  does not determine them.
- Diskin, Xiong, Czarnecki: *From State- to Delta-Based Bidirectional Model
  Transformations*, JOT 2011 / MODELS 2011. The delta-lens view, in which an
  inverse needs the delta and not only the end state; the shape of
  `invert_ops`.
- Bernstein, Melnik: *Model Management 2.0*, SIGMOD 2007; Melnik, Rahm,
  Bernstein: *Rondo*, SIGMOD 2003. Match, Compose, Diff and Merge as generic
  operators over models. There Merge takes two models plus correspondences,
  which is what `merge_manifests` does, and Compose composes two mappings, which
  is what `compose_canonical_maps` does.
- Pottinger, Bernstein: *Merging Models Based on Given Correspondences*, VLDB
  2003, and *Associativity and Commutativity in Generic Merge*, LNCS 5600, 2009.
  Their Merge is the operator this page calls a union, and those papers study
  its commutativity. GraFlo's union is commutative except in the six preserved
  fields above. The three-way merge is symmetric in its two sides (the same
  conflicts, or the same content hash), and a clean result is each side's
  change applied on top of the other; both properties are checked over
  generated inputs for the structural ops. Associativity across three branches
  is not claimed.
- Edwards, Petricek: *Baseline: Operation-Based Evolution and Versioning of
  Data*, 2025; Deshpande: *Living Databases*, 2026. Operation-based versioning of
  data, where the operations are the diff: the same design, applied to data
  rather than to manifests.

## What to read next

- [Version control example (22)](../../examples/version-control/index.md): a
  fork, a conflict, a resolution and a merge, end to end.
- [Manifest evolution](manifest_evolution.md): the ops a commit records.
- [Merging manifests](merging_manifests.md): combining two unrelated manifests.
