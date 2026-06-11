# Manifest evolution

GraFlo provides **contract-level** operations that transform a validated `GraphManifest` into a new manifest: logical vertices and edges, ingestion resources, optional bindings wiring, and the database profile are updated together. This is **not** an in-database migration of existing graph data; the intended workflow is to publish the new manifest and **reingest** from sources.

## Identity and validation

- **Stable hash**: use `manifest_hash` from `graflo.migrate.io` (see [`graflo.migrate.io`](../reference/migrate/io.md)) to compare the composed `schema`, `ingestion_model`, and `bindings` blocks before and after an evolution.
- **Validation**: `apply_evolution` in `graflo.architecture.evolution` returns a deep copy and runs `GraphManifest.finish_init()` by default so the same cross-block checks apply as when loading YAML. API reference: [`graflo.architecture.contract.manifest`](../reference/architecture/contract/manifest.md).

## Operations

| Operation | Summary |
|-----------|---------|
| **Remove vertices** | Drops named vertex types, removes incident edges, prunes ingestion resources that reference removed types (including `vertex_router` `type_map` / `vertex_from_map` via structured pipeline scan), trims `merge_collections`, filters `resource_connector` rows, and updates `db_profile`. Fails if ingestion would be left with no resources. |
| **Merge vertices** | Merges one or more source vertex types into a target name (`into`). If `into` already exists, sources are merged into it; otherwise a new vertex type is built from all sources. Endpoints on edges are rewritten and duplicate `(source, target, relation)` edge kinds are merged. Resource pipelines, `infer_edge_only` / `infer_edge_except`, and `extra_weights` are rewritten; `db_profile` logical keys follow the merge. Conflicting field types or default-value maps raise an error. |
| **Rename vertices** | Renames logical vertex type names across schema, edge endpoints, ingestion pipelines/selectors, and bindings resource references. |
| **Rename relations** | Renames logical edge `relation` values across schema, ingestion selectors/pipelines, and `db_profile` edge metadata. |
| **Rename resources** | Renames ingestion resource names and all bindings references (`connectors[].resource_name`, `resource_connector[].resource`). |
| **Remove edges** | Removes edge types by relation name from schema, `db_profile.edge_specs`, `default_property_values.edges`, and ingestion relation selectors. |
| **Merge edges** | Canonicalizes multiple relation names into one relation, then merges duplicate edge identities and deduplicates edge/profile defaults. |
| **Rename vertex fields** | Per-vertex `{old_field: new_field}` maps: updates schema field names, identities, `db_profile` index specs, and ingestion (`vertex` `from`, `transform.rename` targets) so documents still use the **source** column names where a reverse map is injected. |
| **Remove vertex fields** | Removes vertex properties, prunes vertex/edge index references, and rewrites ingestion references (`from`, `keep_fields`, `vertex_weights`). |
| **Add vertex fields** | Adds properties to existing vertices for schema enrichment and migration planning. |
| **Rename edge fields** | Per-relation edge property renames across schema edge properties/identities, `db_profile` edge indexes/defaults, and edge actor `properties` payloads. |
| **Remove edge fields** | Removes per-relation edge properties, prunes edge index/default references, and rewrites edge actor `properties`. |
| **Add edge fields** | Adds properties to existing relations for edge-schema enrichment. |
| **Add inverse edges** | For each **directed** forward relation `R -> R_inv`, appends inverse schema edges and mirrors ingestion (`pipeline` EdgeActor steps including dynamic endpoints, `relation_field`, redefined `relation_map`, nested `descend`), `infer_edge_only` / `infer_edge_except`, `extra_weights`, and `db_profile`. Skips `directed: false`, TigerGraph `edge_specs[*].reverse_edge`, and existing inverse triples. |
| **Project manifest** | Keeps a logical subgraph by vertex names and/or edge triples `(source, target, relation)`. Prunes isolated vertex types from `keep_vertices` when they have no surviving edges (`connectivity: induced_prune`). Cascades to schema, `db_profile`, ingestion (pipeline steps, infer selectors, `extra_weights`), and bindings. Optional `keep_resources` filters ingestion resources. Inverse edges are not auto-kept. Fails if ingestion would be left empty. |
| **Sanitize** | Target-`DBType` policy: reserved-word-safe names on `DatabaseProfile`, reserved vertex field renames, and (for TigerGraph) consistent identity tuples per edge relation. This is the same work **`graflo.hq.sanitizer.Sanitizer`** applies by building a single **`SanitizeOp`**. |

## API

```python
from graflo.architecture.evolution import (
    AddInverseEdgesOp,
    EdgeSelector,
    MergeEdgesOp,
    MergeVerticesOp,
    ProjectManifestOp,
    RenameRelationsOp,
    RemoveVerticesOp,
    SanitizeOp,
    apply_evolution,
    apply_sanitize,
)
from graflo.migrate.io import manifest_hash
from graflo.onto import DBType

b = apply_evolution(
    a,
    [
        RemoveVerticesOp(op="remove_vertices", names=["legacy_vertex"]),
        MergeVerticesOp(op="merge_vertices", sources=["user", "person"], into="party"),
        RenameRelationsOp(op="rename_relations", relations={"works_at": "employed_by"}),
        MergeEdgesOp(op="merge_edges", sources=["employee_of"], into="employed_by"),
        AddInverseEdgesOp(
            op="add_inverse_edges",
            relations={"employed_by": "employs"},
        ),
    ],
    bump_version=True,  # default: increment schema metadata MINOR (see bump_semver_minor)
)

assert manifest_hash(a) != manifest_hash(b)

# Or sanitize an existing GraphManifest (same op `Sanitizer` uses internally):
apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))
```

- **`bump_version`**: when `True` or `"minor"` (default), increments the numeric `MAJOR.MINOR.PATCH` prefix of `schema.metadata.version` if present (prerelease suffix preserved). Pass `bump_version=False` to leave the version string unchanged.
- **Imports**: `graflo.architecture.evolution` re-exports the ops and apply helpers; lower-level functions such as `apply_remove_vertices`, `apply_merge_vertices`, `apply_rename_relations`, `apply_add_inverse_edges`, `apply_rename_vertex_properties`, and `apply_sanitize` mutate a manifest in place (used mainly internally and by `Sanitizer`).

## Tutorial: relation and property evolution

Use these recipes when converging ontologies or normalizing an existing manifest.

### 1) Rename relation labels (same semantics, new vocabulary)

```python
from graflo.architecture.evolution import RenameRelationsOp, apply_evolution

renamed = apply_evolution(
    manifest,
    [RenameRelationsOp(relations={"works_at": "employed_by"})],
    bump_version=False,
)
```

### 2) Merge relation labels (canonicalization)

Use this when multiple labels represent the same concept:
`works_for`, `employee_of`, `employed_by` -> `employed_by`.

```python
from graflo.architecture.evolution import MergeEdgesOp, apply_evolution

canonical = apply_evolution(
    manifest,
    [
        MergeEdgesOp(
            sources=["works_for", "employee_of"],
            into="employed_by",
        )
    ],
    bump_version=False,
)
```

### 3) Evolve relation payload fields

```python
from graflo.architecture.evolution import (
    AddEdgePropertiesOp,
    RemoveEdgePropertiesOp,
    RenameEdgePropertiesOp,
    apply_evolution,
)

updated = apply_evolution(
    manifest,
    [
        RenameEdgePropertiesOp(
            renames={"employed_by": {"since": "started_at"}},
        ),
        RemoveEdgePropertiesOp(
            removals={"employed_by": ["deprecated_score"]},
        ),
        AddEdgePropertiesOp(
            additions={"employed_by": ["confidence"]},
        ),
    ],
    bump_version=False,
)
```

### 4) Add new vertex fields for enrichment

```python
from graflo.architecture.evolution import AddVertexPropertiesOp, apply_evolution

enriched = apply_evolution(
    manifest,
    [AddVertexPropertiesOp(additions={"person": ["canonical_id", "normalized_name"]})],
    bump_version=False,
)
```

### 5) Add inverse edge relations (bidirectional modeling)

Use this when a forward relation already exists in schema and ingestion (for example `person --works_at--> company`) and you want the reverse kind without hand-authoring every mirror (`company --employs--> person`).

```python
from graflo.architecture.evolution import AddInverseEdgesOp, apply_evolution

bidirectional = apply_evolution(
    manifest,
    [
        AddInverseEdgesOp(
            relations={"works_at": "employs"},
        )
    ],
    bump_version=False,
)
```

For each **directed** schema edge whose `relation` is a key in the map, the op appends an inverse edge with swapped endpoints and the mapped relation name, copying properties, identities, and `directed: true`. The op **does not** run when:

- the forward edge has **`directed: false`** (use one undirected logical edge or TigerGraph `UNDIRECTED EDGE` instead), or
- the forward edge’s TigerGraph **`edge_specs[*].reverse_edge`** is already set (TigerGraph owns the paired reverse type via `WITH REVERSE_EDGE`).

**What gets mirrored in ingestion**

| Location | Inverse behavior |
|----------|------------------|
| Static `pipeline` edge step (`from`/`to`/`relation`) | Duplicate step with swapped endpoints and inverse `relation` |
| Dynamic edge step (`source_role`/`target_role`, mixed static+dynamic) | Duplicate step with swapped roles/static sides; `match_source`/`match_target` swapped |
| `relation_field` | Same field name on the inverse step |
| `relation_map` on the step | Redefined: same raw keys map to inverse canonical names (`EMPLOYED_BY: employed_by` forward → `EMPLOYED_BY: employs` after `employed_by -> employs`) |
| `links` | Each link item inverted independently |
| Nested `descend` pipelines | Recursively mirrored |
| `infer_edge_only` / `infer_edge_except` / `extra_weights` | Static triple specs appended when missing |

**Dynamic EdgeActor example** (after `AddInverseEdgesOp(relations={"employed_by": "employs"})`):

Forward step:

```yaml
- edge:
    source_role: source
    target_role: target
    relation_field: relation_type
    relation_map:
      EMPLOYED_BY: employed_by
```

Appended inverse step:

```yaml
- edge:
    source_role: target
    target_role: source
    relation_field: relation_type
    relation_map:
      EMPLOYED_BY: employs
```

**Choosing a bidirectional strategy** (see also [Core components — Edge](core_components.md#directed-undirected-and-bidirectional-edges)):

| Goal | Approach |
|------|----------|
| Portable across DBs | Two logical directed edges + `AddInverseEdgesOp` |
| TigerGraph-native pair, single load path | One logical edge + `db_profile.edge_specs[*].reverse_edge` |
| Truly symmetric (friends, co-authors) | One logical edge with `directed: false` → `UNDIRECTED EDGE` on TigerGraph |

### 6) Project to a subgraph slice

Use when you need a smaller manifest that retains only specific vertex types and edge triples (for example agent experiments or publishing a focused contract):

```python
from graflo.architecture.evolution import EdgeSelector, ProjectManifestOp, apply_evolution

slice = apply_evolution(
    manifest,
    [
        ProjectManifestOp(
            keep_vertices=["person", "company"],
            keep_edges=[
                EdgeSelector(source="person", target="company", relation="works_at"),
            ],
        )
    ],
    bump_version=False,
)
```

With `keep_vertices` only, vertex types listed but not incident to any surviving edge are dropped (`connectivity: induced_prune`). List inverse edge triples explicitly in `keep_edges` when you need them; they are not inferred automatically.

### Choosing `RenameRelationsOp` vs `MergeEdgesOp`

- Use `RenameRelationsOp` when there is a one-to-one label replacement.
- Use `MergeEdgesOp` when multiple relation labels should collapse into one canonical relation.
- Use `AddInverseEdgesOp` when forward and reverse relations should coexist with different labels (not a rename of the same edge kind).
- `RenameRelationsOp` and `MergeEdgesOp` propagate to schema, `DatabaseProfile` (`edge_specs`, defaults/indexes), and ingestion selectors/resources. `AddInverseEdgesOp` also propagates to `db_profile` and does not rename existing relations; it only adds missing inverse edges and ingestion mirrors.

## Scope notes

- **Transforms**: bodies of named transforms are not rewritten when vertex *field* names change during a merge; that remains an authoring concern. Use **`RenameVertexPropertiesOp`** / **`SanitizeOp`** when you need coordinated field rewrites at the manifest boundary.
- **Bindings**: connector definitions are unchanged; only `resource_connector` rows pointing at dropped resources are removed after a remove operation.

## See also

- [Creating a Manifest](../getting_started/creating_manifest.md) — manifest structure
- [Concepts overview](index.md) — `GraphManifest` role in the pipeline
