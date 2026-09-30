# Evolving a manifest

Your manifest has to change: a type needs a better name, a property is added or
dropped, a type must be identified by a different key. This guide makes such a
change as a change set of operations (ops), applies it, checks what it did,
undoes it, records it, and shows what the change means for data already loaded
with the old manifest. The ops themselves are listed on
[Manifest evolution](../concepts/schema/manifest_evolution.md).

## What you need

- GraFlo installed (`pip install graflo`). No database is needed.
- The starting manifest below, saved as `maintenance.yaml`. It describes a
  maintenance system: assets, and work orders raised against them. Work orders
  name their asset by `asset_id`, and only look assets up
  (`lookup_only: true`); they do not create them.

```yaml
schema:
  metadata:
    name: maintenance
    version: 1.0.0
  graph:
    vertex_config:
      vertices:
      - name: Asset
        properties: [asset_id, serial_number, name, location_code]
        identity: [asset_id]
      - name: WorkOrder
        properties: [work_order_id, opened_at]
        identity: [work_order_id]
    edge_config:
      edges:
      - {source: WorkOrder, target: Asset, relation: targets}
ingestion_model:
  resources:
  - name: assets
    pipeline:
    - vertex: Asset
  - name: work_orders
    pipeline:
    - vertex: WorkOrder
    - vertex: Asset
      lookup_only: true
    - edge: {source: WorkOrder, target: Asset, relation: targets}
```

The plant wants four changes: call assets machines, record when each
machine was commissioned, identify machines by serial number (the key other
systems share) while work orders keep finding them by asset id, and drop the
obsolete location code. It also wants to go from a machine to its work orders.

## Steps

Run the snippets in one Python session, in order; each one uses the result of
the one before.

### 1. Load the manifest

```python
import yaml
from suthing import FileHandle

from graflo import GraphManifest

manifest = GraphManifest.from_config(FileHandle.load("maintenance.yaml"))
manifest.finish_init()


def show_pipeline(m, resource):
    """Print one resource's pipeline as YAML."""
    found = next(r for r in m.ingestion_model.resources if r.name == resource)
    print(
        yaml.safe_dump(found.to_minimal_canonical_dict()["pipeline"], sort_keys=False)
    )
```

`finish_init` validates the manifest, as every GraFlo command does when it
loads one. `show_pipeline` is only for looking at the results.

### 2. Rename a type

```python
from graflo.architecture.evolution import RenameVerticesOp, apply_evolution

renamed = apply_evolution(manifest, [RenameVerticesOp(renames={"Asset": "Machine"})])
print(renamed.graph_schema.metadata.version)
show_pipeline(renamed, "work_orders")
```

```text
1.1.0
- vertex: WorkOrder
- vertex: Machine
  lookup_only: true
- edge:
    source: WorkOrder
    target: Machine
    relation: targets
```

`apply_evolution` returned a new manifest and left `manifest` unchanged. The
rename reached the edge and both resources, and the schema version went from
`1.0.0` to `1.1.0`. Pass `bump_version=False` to keep the version.

### 3. Add a property

```python
from graflo.architecture.evolution import AddVertexPropertiesOp

added = apply_evolution(
    renamed,
    [
        AddVertexPropertiesOp(
            additions={"Machine": [{"name": "commissioned_on", "type": "DATETIME"}]}
        )
    ],
)
machine = added.graph_schema.core_schema.vertex_config["Machine"]
print([(p.name, p.type) for p in machine.properties])
```

```text
[('asset_id', None), ('serial_number', None), ('name', None), ('location_code', None), ('commissioned_on', 'DATETIME')]
```

A property can be added as a bare name or, as here, with its type.

### 4. Key machines by serial number

Replacing a key is the change with the most consequences, so the op asks what
becomes of the old key and of the edge steps that match on it:

```python
from graflo.architecture.evolution import ReplaceIdentityOp

rekeyed = apply_evolution(
    added,
    [
        ReplaceIdentityOp(
            replacements={
                "Machine": {
                    "to": {"mode": "natural", "identity": ["serial_number"]},
                    "retire": "demote",
                    "retire_as": "by_asset_id",
                    "endpoints": "pin_to_retired",
                }
            }
        )
    ],
)
machine = rekeyed.graph_schema.core_schema.vertex_config["Machine"]
print(machine.identity, [(s.name, s.fields) for s in machine.secondary_identities])
show_pipeline(rekeyed, "work_orders")
```

```text
['serial_number'] [('by_asset_id', ['asset_id'])]
- vertex: WorkOrder
- vertex: Machine
  lookup_only: true
- edge:
    source: WorkOrder
    target: Machine
    relation: targets
    target_match: by_asset_id
```

Machines become one vertex per serial number. The old key survives as a
[secondary identity](../concepts/glossary.md#secondary-identity) named
`by_asset_id`: a lookup key that does not decide which records become one
vertex, but lets a source that knows only the asset id find the machine. Work
orders carry only an asset id, so `endpoints: pin_to_retired` pointed their edge
step at that key (`target_match: by_asset_id`).

The choices the op offers:

| Setting | Values |
|---|---|
| `to.mode` | `natural` (with `identity: [...]`), `hash` (with `hash_from: [...]`), `funnel` (with `funnel:`), `assigned`, `blank`; see [Vertex identity](../concepts/schema/vertex_identity.md) |
| `retire` | `demote` (default): the old key becomes a secondary identity; `keep`: its fields stay as plain properties; `drop`: its fields are removed |
| `retire_as` | the name of the demoted key; default `retired_identity` |
| `endpoints` | `follow_new` (default): edge steps match on the new key; `pin_to_retired`: they match on the demoted key, for sources that carry only the old one |

The new key's fields must already be declared on the type; otherwise the op
refuses and tells you to add them first with `add_vertex_properties`. A `blank`
type cannot have secondary identities, so demoting into one is refused; use
`keep` or `drop`. When the old key was generated (`hash`, `assigned` or
`blank`), there is nothing a source could look up by, so `demote` acts as
`keep` and logs a warning.

### 5. Remove a property

```python
from graflo.architecture.evolution import RemoveVertexPropertiesOp

trimmed = apply_evolution(
    rekeyed, [RemoveVertexPropertiesOp(removals={"Machine": ["location_code"]})]
)
print(
    [
        p.name
        for p in trimmed.graph_schema.core_schema.vertex_config["Machine"].properties
    ]
)
```

```text
['asset_id', 'serial_number', 'name', 'commissioned_on']
```

Indexes, `from` mappings and `keep_fields` entries that named the property are
removed with it. A property the type does not declare is refused, so a typo
fails instead of doing nothing.

### 6. Read the relation both ways

Declare `has_work_order` as the inverse of `targets`, then audit the pair:

```python
from graflo.architecture.evolution import DeclareEdgeInversesOp
from graflo.architecture.profile import audit_inverses

machines = apply_evolution(
    trimmed, [DeclareEdgeInversesOp(inverses={"targets": "has_work_order"})]
)
print("\n".join(audit_inverses(machines).to_lines()))
```

```text
has_work_order <-> targets: declared
```

The declaration stores nothing: a read of `has_work_order` follows `targets`
edges backwards, which most databases do at no extra cost. If a consumer needs
the reverse edges stored, a planner returns the ops that store them:

```python
from graflo.architecture.evolution import plan_realize_inverses

plan = plan_realize_inverses(machines, strategy="materialized")
print(plan.ops)
both_ways = apply_evolution(machines, plan.ops)
print("\n".join(audit_inverses(both_ways).to_lines()))
```

```text
[AddInverseEdgesOp(op='add_inverse_edges', relations=['targets'])]
has_work_order <-> targets: materialized (2/2 mirrored)
    fed in work_orders: emit_inverse
```

The rest of this guide continues with `machines`, where the pair is declared
only. [Inverse relations in detail](../concepts/schema/manifest_evolution.md#inverse-relations-in-detail)
covers the other ways to store a pair and the rules they follow.

### 7. Write the change set down

Steps 2 to 6 applied one op at a time. Written as one change set, the same
changes are a file you can review, keep with the manifest and apply again.
Save this as `changes.yaml`:

```yaml
- op: rename_vertices
  renames: {Asset: Machine}
- op: add_vertex_properties
  additions:
    Machine:
    - {name: commissioned_on, type: DATETIME}
- op: replace_identity
  replacements:
    Machine:
      to: {mode: natural, identity: [serial_number]}
      retire: demote
      retire_as: by_asset_id
      endpoints: pin_to_retired
- op: remove_vertex_properties
  removals: {Machine: [location_code]}
- op: declare_edge_inverses
  inverses: {targets: has_work_order}
```

```python
from graflo.architecture.evolution import manifest_hash, ops_from_yaml

ops = ops_from_yaml("changes.yaml")
changed = apply_evolution(manifest, ops)
print(manifest_hash(changed) == manifest_hash(machines))
changed.to_yaml("machines.yaml")
```

```text
True
```

`manifest_hash` is the manifest's content hash; it ignores the name and version,
so equal hashes mean the same graph. `ops_to_yaml_str(ops)` goes the other way,
from ops built in Python to YAML.

### 8. Undo the change

```python
from graflo.architecture.evolution import invert_ops

inverses, blockers = invert_ops(ops, manifest=manifest)
print(blockers)
```

```text
['replace_identity: no inverse could be derived']
```

Four of the five ops can be undone. The key replacement cannot: demoting the
old key changed the secondary identities and the edge steps too, and putting
the old key back would not restore them. When `blockers` is not empty, the
inverses do not restore the original, so do not apply them. Rebuild the old
manifest from its recorded history instead (next step), or split the change so
that the part you may want to undo is a change set of its own.

### 9. Record the change

A commit stores a change set with the content hash before and after it, so the
history can be replayed and checked later:

```python
from graflo.architecture.evolution import FileCommitStore, build_commit

commit = build_commit(manifest, ops, label="key machines by serial number")
print(FileCommitStore(".graflo/commits").append(commit))
```

```text
.graflo/commits/0000_b4c026b926d2_key_machines_by_serial_number.yaml
```

Then, from the shell:

```bash
graflo log
graflo verify --base maintenance.yaml --against machines.yaml
graflo checkout --base maintenance.yaml --output-path restored.yaml
```

```text
commit     kind     rev?  ops  label
b4c026b9   edit     True  5    key machines by serial number (head)
history replays cleanly (1 commit(s), 1 head(s))
matches machines.yaml
manifest hash: e8d32eca1102
written: restored.yaml
```

`verify` replays the history from `maintenance.yaml` and checks every recorded
hash; `checkout` rebuilds the manifest at any commit, which is the reliable way
back.

`graflo commit --from-manifest maintenance.yaml --to-manifest machines.yaml`
records a change between two files instead, deriving the ops itself. It refuses
this change: no op expresses the `target_match` that step 4 added to the work
orders pipeline, so the derived ops would not reproduce `machines.yaml`. Record
a change set you wrote with `build_commit`, as above; use `graflo commit` for
changes made by editing the file. See
[Deriving ops from two manifests](../concepts/schema/manifest_evolution.md#deriving-ops-from-two-manifests).

## What you should see

`machines.yaml` declares `Machine`, keyed on `serial_number`, with the lookup key
`by_asset_id`, the typed property `commissioned_on`, no `location_code`, and the
pair `targets` / `has_work_order`. Its work orders pipeline matches machines by
`by_asset_id`. The commit store holds one commit, and `graflo verify` reports
that the history replays cleanly.

## What happens to the data

Ops change the manifest, never a database. A database loaded with
`maintenance.yaml` still holds `Asset` nodes keyed by `asset_id`. You have two
ways forward:

- **Load again.** Ingest the sources with `machines.yaml` into a new database or
  namespace, then switch readers over. This is the only way a rename or a new
  key reaches stored data.
- **Migrate the schema in place**, for additions only. `graflo migrate-schema`
  compares two manifests at the database level and rates each change by risk:

```bash
graflo migrate-schema plan --from-schema-path maintenance.yaml --to-schema-path machines.yaml
```

```text
Migration Plan
================
Operations: 3
Blocked: 2

Runnable operations:
- ADD_VERTEX vertex:Machine [LOW]
- ADD_EDGE edge:('WorkOrder', 'Machine', 'targets') [LOW]
- ADD_VERTEX_INDEX vertex:Machine:index:(('asset_id',), False, 'persistent', False) [LOW]

Blocked operations:
- REMOVE_EDGE edge:('WorkOrder', 'Asset', 'targets') [HIGH]
- REMOVE_VERTEX vertex:Asset [HIGH]

Warnings:
- High-risk operations are blocked by default. Re-run with explicit allow flag in future guarded workflow.
```

At the database level the rename is a new type plus a removed one, and removals
are blocked by default: applying the plan would create an empty `Machine` type,
not move the assets into it. So a change set like this one is followed by a new
load. [Schema migration](../concepts/operations/migration_and_practices.md)
describes the plan, the risk levels and which databases it can apply to.

## What to read next

- [Manifest evolution](../concepts/schema/manifest_evolution.md): every op, and
  which ones can be undone.
- [Version control](../concepts/schema/versioning.md): branches, three-way
  merges and reverts.
- [Edge inverses example (19)](../examples/edge-inverses/index.md): a relation
  read and stored both ways.
