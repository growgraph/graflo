# Manifest evolution

A manifest changes over its life: types are renamed, properties added, keys
replaced, relations given a reverse name. This page shows how to make such a
change as a list of operations that you can review, store, replay and often
undo, instead of editing YAML by hand. After reading it you will know which
operations exist, how to apply them from Python or the shell, how to undo them,
and how to derive them from two versions of a manifest.

## What an operation is

An operation, or op, is one typed change to a manifest, such as "rename the
vertex type `Asset` to `Machine`". It names what changes, and GraFlo carries the
change through every place that refers to it: the schema, the resources that
feed the type, the database profile and the bindings. An ordered list of ops is
a change set.

Ops are written in YAML or built in Python. This change set renames a type and
adds a typed property to it:

```yaml
- op: rename_vertices
  renames: {Asset: Machine}
- op: add_vertex_properties
  additions:
    Machine:
    - {name: commissioned_on, type: DATETIME}
```

Every op is checked when it is applied. An op that names a type the manifest
does not declare, or that would leave the manifest inconsistent, is refused
with a message rather than applied halfway.

Ops change the manifest only. A database already loaded with the old manifest
keeps its data as it was; see
[Evolving a manifest](../../guides/evolving_a_manifest.md#what-happens-to-the-data)
for what to do about it.

## Operations

Each table lists the op name as written in YAML (the Python class is the same
name in CamelCase with `Op` appended, such as `RenameVerticesOp`), what it
does, and whether it can be [undone](#undoing-ops).

### Vertex types

| Op | What it does | Undo |
|---|---|---|
| `add_vertices` | Adds vertex types, given as full definitions. Refuses a name that exists. | yes |
| `remove_vertices` | Removes vertex types, the edges that touch them, and the pipeline steps, profile entries and bindings that refer only to them. A vertex router keeps routing the surviving types. | only when nothing else was removed with it; remove the edges first |
| `rename_vertices` | Renames vertex types everywhere they are named. The map must not send two names to one. | yes |
| `merge_vertices` | Merges `sources` into one type `into`: edges are redirected, and properties are combined by name. Refuses two different types or units for one property. | no |

### Properties

| Op | What it does | Undo |
|---|---|---|
| `add_vertex_properties` | Adds properties to vertex types, as bare names or with `type` and grounding. | yes |
| `remove_vertex_properties` | Removes properties and the indexes, `from` mappings and `keep_fields` entries that name them. Refuses a property the type does not declare. | yes; the property comes back with its type and grounding |
| `rename_vertex_properties` | Renames properties per type. Resources keep reading the old column name, through a `from` mapping. Refuses a new name the type already declares, unless the map sends both names there on purpose (`{a: c, b: c}`). | yes, unless it folded two properties into one |
| `add_edge_properties` | Adds properties to every edge of a relation. | yes |
| `remove_edge_properties` | Removes properties from every edge of a relation. | yes, when the relation's edges agreed on the property |
| `rename_edge_properties` | Renames properties of a relation, in the schema, profile and edge steps. | yes |
| `change_field_types` | Sets the type of vertex or edge properties. Checked against the target database, and refuses to make a key property a `LIST`. | no |

### Edges and relations

| Op | What it does | Undo |
|---|---|---|
| `add_edges` | Adds edges between existing vertex types. | yes |
| `remove_edges` | Removes edges by relation (`relations`, every pair of types) or by exact `(source, target, relation)` (`edges`). | yes |
| `rename_relations` | Renames relations everywhere they are named. | yes |
| `merge_edges` | Merges several relation names into one. | no |
| `retarget_edges` | Changes which vertex types an edge connects, keeping its properties, keys, direction and database settings. | yes |
| `set_edge_directed` | Sets `directed` on selected edges. | yes |

### Inverse relations

A declared inverse is a second name for reading a relation backwards, such as
`has_work_order` for `targets`. See
[Inverse relations](#inverse-relations-in-detail) below.

| Op | What it does | Undo |
|---|---|---|
| `declare_edge_inverses` | Declares inverse pairs (`inverses`) and relations that are their own inverse (`symmetric`). Stores nothing. | yes |
| `retract_edge_inverses` | Withdraws declarations. Refused while the database maintains the pair or a step still writes it. | yes |
| `add_inverse_edges` | Stores declared pairs as edges in the reverse direction, written from the same records (materialized). | yes |
| `set_native_inverses` | Has TigerGraph maintain declared pairs (`enabled: false` withdraws). | yes |
| `set_inverse_emission` | Sets or clears `emit_inverse` on edge steps addressed by position. | yes |

### Identity

| Op | What it does | Undo |
|---|---|---|
| `replace_identity` | Replaces how a vertex type is keyed: new fields or a new mode. Decides what becomes of the old key and of the edge steps that match on it. See [Evolving a manifest](../../guides/evolving_a_manifest.md#4-key-machines-by-serial-number). | only with `retire: keep` |
| `add_secondary_identities` | Adds lookup keys to vertex types; each gets an index. | yes |
| `remove_secondary_identities` | Removes lookup keys. Refused while an edge step matches on one. | yes |
| `replace_edge_identities` | Replaces the uniqueness keys of edges. | yes |

### Database profile

| Op | What it does | Undo |
|---|---|---|
| `add_vertex_indexes`, `remove_vertex_indexes` | Adds or removes secondary indexes on vertex types. Indexes that come from secondary identities are removed with `remove_secondary_identities`. | yes |
| `add_edge_indexes`, `remove_edge_indexes` | Adds or removes indexes on edges. | yes |
| `set_db_profile` | Replaces the whole database profile: target database, namespace, storage names, defaults and indexes. | yes |
| `sanitize` | Records storage names that a target database accepts, for names it reserves or cannot store. The logical names are unchanged. | no |

### Resources and bindings

| Op | What it does | Undo |
|---|---|---|
| `add_resources` | Adds resources, with any named transforms their steps use. | yes, unless it registered a transform the manifest did not hold |
| `remove_resources` | Removes resources and the bindings entries that wire them. | yes, unless bindings wired them |
| `rename_resources` | Renames resources and every bindings reference to them. | yes |
| `add_resource_transforms` | Appends transform steps to resources, at the root or at a nested level (`at`). | no |
| `ensure_extracted_fields` | Makes a vertex router keep named fields that its `keep_fields` or `extraction_scope` would drop. | no |
| `set_bindings` | Replaces the whole bindings block; `null` removes it. | yes |

### Grounding and descriptions

These ops change what a type means to a reader. Nothing reads them when records
are cast or written.

| Op | What it does | Undo |
|---|---|---|
| `set_vertex_semantics` | Grounds vertex types in an external vocabulary; `null` clears. | yes |
| `set_edge_semantics` | Grounds edges. | yes |
| `set_field_semantics` | Grounds properties of vertices or edges, including their `unit`. | yes |
| `set_vertex_descriptions` | Sets or clears the description of vertex types. | yes |

### Whole manifest

| Op | What it does | Undo |
|---|---|---|
| `canonicalize` | Renames types, properties and relations by one map, all at once. A group of names sent to one target merges them and needs `allow_merges`. | when it only renames |
| `project_manifest` | Keeps a part of the manifest: named vertex types and edges, or the neighborhood of some types. See [Keeping part of a manifest](#keeping-part-of-a-manifest). | no |

`merge_manifests` is the one op that takes two manifests. It is applied with
`merge_manifests` or `graflo merge`, not with `apply_evolution`; see
[Merging manifests](merging_manifests.md).

## Applying ops

In Python, `apply_evolution` applies a change set and returns a new manifest;
the input is not changed:

```python
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.evolution import apply_evolution, manifest_hash, ops_from_yaml

manifest = GraphManifest.from_config(FileHandle.load("maintenance.yaml"))
ops = ops_from_yaml("changes.yaml")

changed = apply_evolution(manifest, ops)
print(manifest_hash(manifest)[:12], "->", manifest_hash(changed)[:12])
```

`apply_evolution` validates the result as loading a manifest does, and bumps the
minor part of `schema.metadata.version`. Pass `bump_version=False` to keep the
version, or `finish_init=False` to skip the validation.

Ops are also Python classes, which is convenient when a program builds them:

```python
from graflo.architecture.evolution import AddVertexPropertiesOp, RenameVerticesOp

ops = [
    RenameVerticesOp(renames={"Asset": "Machine"}),
    AddVertexPropertiesOp(
        additions={"Machine": [{"name": "commissioned_on", "type": "DATETIME"}]}
    ),
]
```

`ops_to_yaml_str(ops)` writes a change set as YAML, and `ops_from_yaml` reads a
file path or a YAML string back.

`manifest_hash` is the manifest's content hash. It covers the schema, the
ingestion model and the bindings but not the name or version, so two manifests
that describe the same graph hash equal.

Many ops rewrite resources as well as the schema. Applied to a manifest without
an `ingestion_model`, they change only the schema, and the resources you keep
elsewhere would still use the old names. `ops_reaching_ingestion(ops)` lists
the ops in a change set for which that matters.

From the shell, there is no command that applies a hand-written change set.
These commands produce or replay one:

| Command | What it does with ops |
|---|---|
| `graflo commit --from-manifest A --to-manifest B` | derives the ops from A to B and records them; see [Deriving ops from two manifests](#deriving-ops-from-two-manifests) |
| `graflo checkout --base A` | replays recorded ops to rebuild a manifest |
| `graflo inverses realize\|repair\|switch\|withdraw ... --emit-ops FILE -o OUT` | plans ops for inverse relations, writes them, and applies them |
| `graflo lift --spec FILE --emit-ops FILE -o OUT` | plans the ops that add state and provenance types |

## Undoing ops

`invert_ops` computes the ops that undo a change set, from the manifest as it
was before the change:

```python
from graflo.architecture.evolution import invert_ops

inverses, blockers = invert_ops(ops, manifest=manifest)
if not blockers:
    restored = apply_evolution(changed, inverses)
```

An inverse is exact or absent: GraFlo applies each candidate inverse and keeps
it only when it restores the earlier manifest's content hash. When `blockers` is
not empty, some op could not be undone, and the inverses do not restore the
manifest. Rebuild it from the earlier version instead, with
[`checkout`](versioning.md#commits).

These ops never have an inverse, because the information needed to go back is
gone once they run:

| Op | Why it cannot be undone |
|---|---|
| `merge_vertices`, `merge_edges`, `canonicalize` that merges | which source each property and key came from is lost |
| `change_field_types` | the previous type is overwritten |
| `sanitize` | the storage names it replaces are not recorded |
| `project_manifest` | it drops everything outside the part kept |
| `add_resource_transforms`, `ensure_extracted_fields` | no op removes the steps or fields they add |
| `merge_manifests` | it has two inputs, so there is no single earlier manifest |

Others can be undone only from some manifests, as the tables above say: a
`remove_vertices` that also removed edges, or a `replace_identity` that demoted
the old key, has no inverse. To keep a change undoable, split it: remove the
edges first, then the vertex type.

## Deriving ops from two manifests

When you have two versions of a manifest, such as a file edited by hand,
`diff_manifests` derives the ops that turn one into the other.
`diff_manifests_verified` also applies them and checks that the result has the
target's content hash:

```python
from graflo.architecture.evolution import RenameHints, diff_manifests_verified

ops, warnings = diff_manifests_verified(
    before, after, hints=RenameHints(vertices={"Asset": "Machine"})
)
```

The differ does not guess renames. A type `Asset` that disappears while a type
`Machine` appears could be a rename or a removal plus an addition, and guessing
wrong turns a change that keeps data into one that drops it. Without the hint
above, the diff removes `Asset` and adds `Machine`. `RenameHints` takes
`vertices`, `relations`, `resources`, `vertex_properties` and `edge_properties`.

`warnings` lists what the ops could not express. A pipeline edit other than
appended transform steps has no op, for example; when that happens, replaying
the ops does not reproduce the target, and the last warning says so. A change
set with warnings is incomplete, and `graflo commit` refuses to record it.

The ops come in an order in which each one's preconditions hold: renames first,
then additions, then changes, then removals.

## Inverse relations in detail

A relation can often be read from both ends: `WorkOrder targets Machine` and
`Machine has_work_order WorkOrder`. Declaring the pair gives the reverse
reading a name and stores nothing; reads of `has_work_order` follow `targets`
edges backwards, which most databases do at no extra cost. Storing the reverse
reading is a separate choice, made per pair, in one of two ways:

- **materialized**: the reverse edges are declared edges, written from the same
  records by edge steps that set `emit_inverse: true`. Works on every database.
- **native**: TigerGraph maintains the reverse edge type itself
  (`db_profile.native_inverses`).

[Core components](../architecture/core_components.md#directed-undirected-and-bidirectional-edges)
shows how each is declared in YAML. This section covers changing them.

### Planning the change

Five planners look at a manifest and return the ops for a change, together with
the pairs they skipped and why. They apply nothing, so the op list is what you
review:

```python
from graflo.architecture.evolution import apply_evolution, plan_realize_inverses

plan = plan_realize_inverses(manifest, strategy="materialized")
plan.ops  # [AddInverseEdgesOp(op='add_inverse_edges', relations=['targets'])]
plan.skipped  # pairs left alone, each with a code and a reason
manifest = apply_evolution(manifest, plan.ops)
```

| Planner | Emits | Use it to |
|---|---|---|
| `plan_realize_inverses(manifest, strategy=...)` | `set_native_inverses` or `add_inverse_edges` | store declared pairs: `native`, `materialized`, or `auto`, which chooses from what a reverse read costs on the target database and often stores nothing |
| `plan_repair_inverses(manifest)` | whatever fills each gap the audit reports | make every resource feed a stored inverse; conflicts are left to you |
| `plan_switch_realization(manifest, relations, to=...)` | a withdrawal, then a realization | move pairs from native to materialized or back |
| `plan_withdraw_realization(manifest, relations)` | `set_native_inverses` off, or `remove_edges` | stop storing the reverse edges and keep the declaration |
| `plan_declare_symmetric(manifest, relations)` | `set_edge_directed`, then `declare_edge_inverses` | declare relations that read the same both ways, such as `adjacent_to` |

Each plan is tried on a copy of the manifest before it is returned, so an op the
manifest would refuse is reported as skipped rather than handed to you.
Planners emit the basic ops rather than one combined op so that a recorded
change replays without the planner and each step can be undone on its own.

From the shell, `graflo inverses` runs the same planners:

```bash
graflo inverses audit machines.yaml
graflo inverses realize machines.yaml --strategy materialized \
    -o machines_both.yaml --emit-ops realize.ops.yaml
graflo inverses repair merged.yaml -o fixed.yaml --emit-ops fix.ops.yaml
graflo inverses switch machines.yaml --to native -r targets -o out.yaml
graflo inverses withdraw machines.yaml -r targets -o out.yaml
```

Without `-o` a command prints the plan only; `--dry-run` writes nothing.

### Auditing what you have

`audit_inverses` reports, for each declared pair, how it is stored and how each
resource that writes the forward relation feeds the reverse one:

```python
from graflo.architecture.profile import audit_inverses

for line in audit_inverses(manifest).to_lines():
    print(line)
```

```text
has_work_order <-> targets: materialized (2/2 mirrored)
    fed in work_orders: emit_inverse
```

A pair is `declared` when nothing stores it, `native` or `materialized` when
something does, and `partial` or `conflicting` when something is wrong. Findings
come in three severities:

| Severity | Meaning | Examples |
|---|---|---|
| `repairable` | One place states something another omits. Filling it in cannot change the meaning. | a resource writes the forward relation but not its stored inverse (`unfed_inverse`); the reverse edge exists for some pairs of types only; one direction declares a property the other lacks |
| `conflict` | Two places disagree, and only you know which is right. | a pair whose two relations run the same way; the two directions declare different keys or property types; a step writes an inverse no edge declares |
| `note` | Nothing is wrong. | a pair that is only declared, on a database that cannot read an edge from its target |

`plan_repair_inverses` fills in `repairable` findings and never touches a
`conflict`: where one place is silent the manifest has one answer, and where two
places disagree it has two. The same audit runs as the `inverses`
[conformance profile](world_model_profile.md) (`graflo check --profile
inverses`), where conflicts fail and omissions warn. `graflo inverses audit`
exits `1` when it finds a conflict.

### Rules the schema enforces

A manifest that breaks one of these rules is refused when it is loaded:

| Rule | Refused when |
|---|---|
| One inverse per relation | a relation would get two inverses, such as `a-b` and `b-c`, or it is both paired and symmetric |
| No self-pair | a pair names one relation twice; declare it `symmetric` instead |
| No dangling declaration | a pair or symmetric name names no edge |
| One direction per relation | edges of one relation disagree on `directed` |
| Pairs are directed | a paired relation is on an undirected edge |
| Symmetric is undirected | a symmetric relation is on a directed edge |
| Native needs a pair | a relation in `native_inverses` has no declared pair, or is symmetric |
| One way to store a pair | a relation is native and declared edges also carry its inverse, or both sides of a pair are native |
| One edge type | `relation_name` overrides store a native relation under several names |
| TigerGraph only | `native_inverses` is set for another database |
| One type namespace | an inverse name equals a vertex type name (TigerGraph type names are global) |
| A flag that can write | a step that names one edge sets `emit_inverse`, but its relation has no stored inverse |

Ops are refused by the same rules, which is why making a relation symmetric
takes two ops in a fixed order (`set_edge_directed`, then
`declare_edge_inverses`).

Stored reverse edges are ordinary edges once they exist: a later op on the
forward edge does not reach them. That is the drift the audit reports.

## Keeping part of a manifest

`project_manifest` keeps a part of a manifest and removes everything else,
including the pipeline steps, profile entries and bindings that referred to it:

```python
from graflo.architecture.evolution import ProjectManifestOp, apply_evolution

around_machines = apply_evolution(
    manifest, [ProjectManifestOp(keep_vertices=["Machine"], depth=1)]
)
```

With `keep_vertices` alone, the listed types are kept together with the edges
among them, and a listed type left with no edge is dropped. `keep_edges` lists
exact `(source, target, relation)` edges to keep. `keep_resources` narrows the
resources too.

`depth` turns `keep_vertices` into starting points and keeps every type within
that many edges of one of them, with every edge among the kept types:

| Setting | Effect |
|---|---|
| `depth: 0` (default) | `keep_vertices` is the literal list. |
| `depth` with `keep_edges` | the walk follows only the listed edges |
| `direction` | `any` (default), `out` or `in`; an undirected edge is followed both ways |
| `keep_inverse_edges: true` | with `keep_edges`, also keeps the stored reverse of each kept edge |

## Further reading

- Bonifati, Furniss, Green, Harmer, Oshurko, Voigt: *Schema Validation and
  Evolution for Graph Databases*, ER 2019. Property-graph schema evolution as
  graph rewriting; the closest earlier set of operations for property-graph
  schemas.
- Hausler, Klettke, Störl: *A language for graph database evolution and its
  implementation in Neo4j*, ER Forum 2023. An evolution language bound to one
  database; the ops on this page are independent of the database.
- Bonifati: *Versatile Property Graph Transformations*, PVLDB 18(12), 2025.
  Declarative graph-to-graph transformations; a comparison point for
  `project_manifest`.
- Bernstein: *Applying Model Management to Classical Meta Data Problems*, CIDR
  2003. The generic operators Match, Compose, Diff, Merge and ModelGen, of which
  `diff_manifests` and `merge_manifests` are instances for manifests.

## What to read next

- [Evolving a manifest](../../guides/evolving_a_manifest.md): a change set
  written, applied, undone and recorded, step by step.
- [Merging manifests](merging_manifests.md): combining two manifests into one.
- [Version control](versioning.md): recording change sets as commits.
