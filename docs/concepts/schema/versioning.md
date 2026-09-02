# Version control for world models

A schema is a world model, and manifests are its provenance. This page covers
what GraFlo records about a manifest's history: how a manifest gets a content
address, how change sets become commits, and how two lines of change are
reconciled.

The model is a **git log, not an Alembic script**. Alembic's core abstraction is
a reversible `upgrade()` / `downgrade()` pair, and GraFlo cannot honour that:
`merge_vertices` discards which source each property came from,
`change_field_types` discards the previous type, `sanitize` and
`project_manifest` drop material outright. A `downgrade` that quietly produces a
*different* manifest is worse than none. So history moves forward, and going
back means replaying from the base.

Nothing here touches a database. A history is a fact about the **contract**; see
[Migration and practices](../operations/migration_and_practices.md) for the
database-facing plane.

## Content addressing

Two manifests that describe the same world model must hash equal.

```python
from graflo.architecture.evolution import manifest_hash

manifest_hash(a) == manifest_hash(b)   # same model, however each was reached
```

`to_minimal_canonical_dict()` already normalizes defaults, `None`, aliases and
key order. What it does not normalize is **list order** — and most lists in the
contract are declaration order over a set, so two identical schemas authored in
different order, or one authored and one replayed, hashed differently.
`canonical_payload` adds exactly that normalization.

### Sorted or preserved

Which lists may be sorted is a per-field decision, recorded in `LIST_ORDER`
(`architecture/evolution/canonicalize.py`) as a total classification of every
sequence field reachable from `GraphManifest`.

**The two mistakes are not symmetric.** Marking an order-significant list
`SORTED` makes two *different* models hash equal, and nothing downstream can
detect it. Marking an order-insignificant one `PRESERVED` is a missed dedup —
visible and harmless. So doubt resolves to `PRESERVED`, and every `SORTED` entry
carries the reason order does not matter there.

| Sorted | Preserved |
|---|---|
| vertices, edges, properties, secondary identities | resource pipelines (an ordered program) |
| resource and transform registries, bindings entries | `Vertex.identity` — a backend addresses an endpoint through the *first* identity field |
| index sets, edge specs | compound-index columns |
| semantic `exact_match` / `synonyms` | identity funnel branches (first firing branch wins) |
| selector and membership sets | transform argument tuples, join and projection order, filter operands |

Sorting is by each element's canonical JSON rendering rather than a per-field
key: total over heterogeneous unions, no tie-break rule, and no way for the
result to depend on input order. Nothing reads that order — it is hash-side
only, and authored YAML keeps its declaration order.

Reaching an unclassified sequence raises `UnclassifiedListField` rather than
guessing, and `CANON_VERSION` is mixed into the hashed bytes so a future change
to these rules produces different hashes by construction instead of silently
reinterpreting old ones.

## Provenance

The content address and lineage travel *with* the artifact, so a shipped
manifest is self-describing outside any registry.

```yaml
metadata:
    provenance:
        content_hash: "…64 hex…"
        canon: graflo/canon@2
        parents: [a3f9c21e4b70, 9e11d02c55aa]
        commit: c4d1e9a2b3f0
        merge_recipe: "…"
```

**Provenance is never part of the content hash.** That exclusion is the
definition, not an optimization: content identity must be *path independent*, so
two routes reaching the same world model agree that they did. A hash covering
the parents would make identity depend on history, and dedup could never fire.
The role of a hash-that-covers-ancestry is played by the commit id, exactly as
in git.

Stamping is explicit — `stamp_provenance(...)` at a commit point, never
something `apply_evolution` does. Applying the same ops twice must not produce
two artifacts that disagree about their own lineage.

## Commits

```python
from graflo.architecture.evolution import build_commit, History, checkout

first = build_commit(base, ops, label="add email")
second = build_commit(after_first, more_ops, parents=[first.id], label="rekey")
history = History(commits=[first, second])

restored = checkout(base, history)            # replays, verifying every tree
as_of_first = checkout(base, history, first.id)
```

A `Commit` carries its ops, its **parents** (empty for a root, one for an edit,
two or more for a merge or compose) and the content hash before and after it.
`build_commit` applies the ops rather than trusting them, so both trees describe
a transition that actually happened, and it refuses a change set that leaves the
manifest unchanged — a commit that moves nothing is a lie about history.

Commit ids are content-derived from the ops **and** the parent order, so
regenerating the same change set yields the same id rather than a duplicate
under a new name.

### Forks are recorded facts

Two commits may share a parent. `History` validates what is genuinely broken —
duplicate ids, a parent that does not exist, a cycle, a first-parent edge whose
trees do not line up — and represents everything else, including multiple heads.

Two people evolving the same version is a thing that happens. A history that
refuses to represent it is not a record of what happened.

```python
history.heads()        # more than one means it has forked
history.linearize()    # raises when there is no single path
history.topological()  # always available, deterministic on ties
```

### Merge commits are materialized

A merge commit's `ops` are the diff from its **first parent** to the merged
result — not an interleaving of both sides. That single decision keeps
everything else simple: first-parent replay and hash verification work
identically for edit and merge commits, so nothing downstream needs a special
case. The declarative record of *how* the merge was resolved rides alongside as
a recipe.

### Undoing

History is append-only, so undoing a change moves forward:
`build_revert_commit` records a new commit applying the inverses. Inversion is
exact or it fails — an op with no total inverse, or one whose inverse needs data
the current manifest no longer holds, raises rather than producing a manifest
that merely resembles the earlier state. When the base is available, checking
out the parent commit is always exact and is the better tool.

| Reversible | Irreversible |
|---|---|
| add ↔ remove: vertices, edges, vertex/edge properties, indexes | `merge_vertices`, `merge_edges` |
| rename: vertices, relations, resources, properties | `change_field_types` |
| `set_edge_directed`, `add_inverse_edges`, `retarget_edges` | `sanitize`, `project_manifest` |
| `replace_identity` (with `retire: keep`), secondary identities | `compose_manifests` (binary) |

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

Merging is not diffing. Both sides descend from a common ancestor, so the
question is never "what is different" but "what did each side *change*, and do
those changes collide". `find_merge_base` returning `None` means the two share
no ancestor — which is the signal that the operation wanted is **compose**, not
merge.

### Slots

Reconciliation happens per **slot** — the addressable location an op touches,
such as `vertex/person/field/age`. Disjoint slots merge automatically; the same
change on both sides merges once; different changes to one slot are a
`MergeConflict` carrying both sides' ops *and the ancestor's state*, because
"what did this look like before either change" is the question a resolver needs
answered and the one a two-way diff cannot express.

Three things make the slot the right unit:

- An **order-significant sequence is one slot**. A resource pipeline is an
  ordered program, and half-merging two edits to a program produces something
  neither author wrote.
- A **rename occupies both names**. Renaming `person` → `customer` while the
  other side adds a field to `person` is a genuine collision, invisible unless
  the rename is understood to touch the old slot too.
- An op touching several slots is **atomic**: if any one is contested, the whole
  op is held back. Applying half an op is not a merge.

Slots nest, so `vertex/person` contains `vertex/person/field/age`, and they are
keyed on the **canonical** name — `order_line` and `OrderLine` occupy the same
slot and conflict, rather than merging into two unrelated types with the data
split between them.

### Determinism

The same inputs produce the same merged manifest, the same conflicts in the same
order, and the same content hash. Resolutions take the **place** of the ops they
replace rather than being appended, because op order is a precondition:
`diff_manifests` emits an identity change before the secondary-identity add that
depends on it.

### Merge is not compose

| | Merge | Compose |
|---|---|---|
| Inputs | two descendants of a common ancestor | unrelated lineages |
| Names | expected to agree; disagreement is a **conflict** | expected to disagree; a **declared equivalence** reconciles them |
| Reached by | `merge_three_way` | `compose_manifests` |

Both produce multi-parent commits. They are not the same operation.

## Tracked merges

A `MergeRecipe` records how a merge was resolved, content-addressed with its
resolutions hashed in slot order.

```python
from graflo.architecture.evolution import build_recipe, re_merge

recipe = build_recipe(ancestor, left, right, resolutions=resolutions)
merged, result = re_merge(recipe, ancestor, advanced_left, right)
```

When the left side advances, `re_merge` replays the recorded decisions and
surfaces only genuinely new conflicts. That is what keeps an overlay
maintainable rather than a fork someone re-litigates every release.

A recorded resolution whose slot no longer conflicts is **reported as unused,
never force-applied** — re-applying a stale decision to a slot nobody contested
is how a re-merge quietly reverts someone's work.

## CLI

```bash
graflo commit --from-manifest base.yaml --to-manifest target.yaml -m "add order"
graflo log --graph
graflo verify --base base.yaml --against target.yaml
graflo checkout <commit> --base base.yaml --output-path out.yaml
graflo merge <left> <right> --base base.yaml --take left
graflo revert <commit> --base base.yaml
graflo stamp manifest.yaml --commit <commit>
```

Commits live under `.graflo/commits` by default, one YAML per commit. The store
rebuilds the DAG from the recorded parent ids, not from the filenames.

Distinct from `graflo migrate-schema`, which plans and executes changes against
a *database*. These verbs record and replay changes to the manifest.

## Not in scope

Applying a commit history to a **live database**. `migrate` remains the
DB-facing plane, and extending it beyond additive DDL is tracked separately.
Commits describe the contract.

## See also

- [Manifest evolution](manifest_evolution.md) — the op vocabulary a commit records
- [Example 20](../../examples/example-20.md) — fork, conflict, resolve, merge, end to end
- [Example 19](../../examples/example-19.md) — composing unrelated manifests instead
