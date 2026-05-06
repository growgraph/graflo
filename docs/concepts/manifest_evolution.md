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
| **Rename vertex fields** | Per-vertex `{old_field: new_field}` maps: updates schema field names, identities, `db_profile` index specs, and ingestion (`vertex` `from`, `transform.rename` targets) so documents still use the **source** column names where a reverse map is injected. |
| **Sanitize** | Target-`DBType` policy: reserved-word-safe names on `DatabaseProfile`, reserved vertex field renames, and (for TigerGraph) consistent identity tuples per edge relation. This is the same work **`graflo.hq.sanitizer.Sanitizer`** applies by building a single **`SanitizeOp`**. |

## API

```python
from graflo.architecture.evolution import (
    MergeVerticesOp,
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
    ],
    bump_version=True,  # default: increment schema metadata MINOR (see bump_semver_minor)
)

assert manifest_hash(a) != manifest_hash(b)

# Or sanitize an existing GraphManifest (same op `Sanitizer` uses internally):
apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))
```

- **`bump_version`**: when `True` or `"minor"` (default), increments the numeric `MAJOR.MINOR.PATCH` prefix of `schema.metadata.version` if present (prerelease suffix preserved). Pass `bump_version=False` to leave the version string unchanged.
- **Imports**: `graflo.architecture.evolution` re-exports the ops and apply helpers; lower-level functions such as `apply_remove_vertices`, `apply_merge_vertices`, `apply_sanitize`, and `apply_rename_vertex_fields` mutate a manifest in place (used mainly internally and by `Sanitizer`).

## Scope notes

- **Transforms**: bodies of named transforms are not rewritten when vertex *field* names change during a merge; that remains an authoring concern. Use **`RenameVertexFieldsOp`** / **`SanitizeOp`** when you need coordinated field rewrites at the manifest boundary.
- **Bindings**: connector definitions are unchanged; only `resource_connector` rows pointing at dropped resources are removed after a remove operation.

## See also

- [Creating a Manifest](../getting_started/creating_manifest.md) — manifest structure
- [Concepts overview](index.md) — `GraphManifest` role in the pipeline
