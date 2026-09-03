# Manifest evolution

GraFlo provides **contract-level** operations that transform a validated `GraphManifest` into a new manifest: logical vertices and edges, ingestion resources, optional bindings wiring, and the database profile are updated together. This is **not** an in-database migration of existing graph data; the intended workflow is to publish the new manifest and **reingest** from sources.

## Identity and validation

- **Stable hash**: use `manifest_hash` from `graflo.migrate.io` (see [`graflo.migrate.io`](../../reference/migrate/io.md)) to compare the composed `schema`, `ingestion_model`, and `bindings` blocks before and after an evolution.
- **Validation**: `apply_evolution` in `graflo.architecture.evolution` returns a deep copy and runs `GraphManifest.finish_init()` by default so the same cross-block checks apply as when loading YAML. API reference: [`graflo.architecture.contract.manifest`](../../reference/architecture/contract/manifest.md).

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
| **Replace identity** | Per-vertex identity policy swap covering both field-set and **mode** changes (`natural` / `hash` / `assigned` / `blank`). `retire` decides what becomes of the old field-set — `demote` (default) turns it into a secondary identity, `keep` leaves it as plain properties, `drop` removes it. `endpoints` decides whether edge steps follow the new identity (`follow_new`, default) or stay pinned to the demoted one (`pin_to_retired`). Drops `db_profile` indexes that encoded the retired identity. See [Replacing a vertex identity](#replacing-a-vertex-identity). |
| **Add / remove secondary identities** | Declares or withdraws alternate lookup keys on existing vertices. Each field-set's non-unique index is *derived* by `Schema.finish_init`, so adding one needs no index authoring; removing one drops the derived index explicitly. Removal is rejected while an edge step still selects the field-set. |
| **Replace edge identities** | Replaces `Edge.identities` (uniqueness keys) per `(source, target, relation)`. No retire policy — edge identities have no lookup plane. Non-endpoint tokens are merged into edge `properties` by `Edge.finish_init`. |
| **Add vertices / add edges** | Introduces new logical vertex types and edge relations unarily — the counterpart to what `ComposeManifestsOp` could previously only do binarily. Rejects existing names/triples and unknown endpoints. |
| **Retarget edges** | Changes which vertex types an edge connects, preserving its properties, `identities`, `directed` flag, and `db_profile` physical spec — all of which a remove-plus-add would lose. Rewrites the `EdgeId` in `edge_config`, `edge_specs`, and pipeline edge steps, keyed on the full triple so a different relation between the same types is untouched. |
| **Change field types** | Sets `Field.type` / `item_type` on vertex or edge properties. Validated against the profile's `db_flavor` via `graflo.db.field_type_support`, so an unsupported LIST target fails at op time rather than at define time. Refuses to make an identity field a LIST. |
| **Add / remove vertex & edge indexes** | Authors `db_profile.vertex_indexes` and `edge_specs[].indexes` directly. Indexes derived from `secondary_identities` cannot be removed this way — they would be re-registered by the next `finish_init`, so the op points at **remove secondary identities** instead. |
| **Set edge directed** | Sets `Edge.directed` on selected triples. Load-bearing for replay: `directed` decides what **add inverse edges** may duplicate. |
| **Sanitize** | Target-`DBType` policy: reserved-word-safe names on `DatabaseProfile`, reserved vertex field renames, and (for TigerGraph) consistent identity tuples per edge relation. This is the same work **`graflo.hq.sanitizer.Sanitizer`** applies by building a single **`SanitizeOp`**. |
| **Add resource transforms** | Appends transform steps to named resources' pipelines (root level; actor type-priority ordering runs them before vertex extraction at that level) and optionally registers named transforms (loud on same-name/different-body, mirroring compose). The only op whose primary effect is ingestion; requires `ingestion_model` and raises otherwise. Steps may reference the registry via `call.use` or carry a fully inline `call` (collision-free). Irreversible. |
| **Compose manifests** | Binary union of two full `GraphManifest`s (schema **and** resources/bindings) via `ComposeManifestsOp` + `compose_manifests(left, right, op)`. Consumes **explicit** equivalence maps only (no semantic inference): n-ary vertex clusters (`left` / `right` each name one or more classes collapsing onto one `into`), property alignment, optional composed `identity`, optional `identity_alignments`, relation equivalences, resource renames / `name_conflict`. Distinct from unary `MergeVerticesOp`. Rejected by unary `apply_evolution`. |

## Compose two manifests

GraFlo stays deterministic: an external tool (or a human) may *propose* equivalences; core only *applies* them.

```python
from graflo.architecture.evolution import (
    ComposeManifestsOp,
    PropertyEquivalence,
    RelationEquivalence,
    VertexEquivalence,
    compose_manifests,
)

composed = compose_manifests(
    left,
    right,
    ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left="Client",
                right="Customer",
                into="Person",
                properties=[
                    PropertyEquivalence(
                        left="client_id", right="customer_id", into="id"
                    ),
                    PropertyEquivalence(
                        left="email", right="email_addr", into="email", identity=True
                    ),
                ],
                identity=["email"],  # optional explicit key; else merge + flags
            )
        ],
        relations=[RelationEquivalence(left="places", right="billed", into="activity")],
        resource_renames={},  # right resource name -> composed name
        name_conflict="error",  # or "prefix_right" / "fuse_right"
    ),
)
```

`left` / `right` also take a **list**, which is how an n-ary cluster is
spelled — one declaration naming every member that collapses onto `into`:

```python
ComposeManifestsOp(
    vertices=[
        VertexEquivalence(
            left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
        )
    ],
    allow_merges=True,          # >1 member on a side is a stated intent
    allow_self_relations=False,  # forwarded to the per-side MergeVerticesOp
    allow_row_fusion=False,
)
```

Empty `vertices` / `relations` yields a **disjoint union** (both resource sets and bindings retained), subject to collision policy.

A `VertexEquivalence` declaration *is* one cluster. `ClusterConflictError` (raised before any rename) covers the three ways declarations can contradict each other: a class **claimed by two** declarations, two declarations **sharing one `into`** (that collapse is one n-ary cluster and must be spelled as one), and an `into` that already names an **existing non-member class** on a side (which would silently merge into an unrelated type). Properties with the **same spelling** on both sides after alignment fuse for free; list a `PropertyEquivalence` only to rename, to map per member (`left={"Company": "company_key", "Shop": "shop_key"}`), or to flag identity.

Compose refuses to guess the composed **identity** too: when members disagree on their identity field-set after alignment and nothing resolves it, `ComposeIdentityError` names each member's key. Resolve it with `identity` on the cluster (a natural key, an `IdentityFunnel`, or a `SideIdentity` shorthand lowered to one funnel), a `PropertyEquivalence(identity=True)` flag, or an `identity_alignments` entry. A declared `identity` demotes each member's retired key to a lookup-only secondary identity unless the cluster sets `retire="keep"`.

## Canonical maps

When one side of a compose is first translated into a target vocabulary, the translation and the compose op are two declarations that can silently contradict each other — most dangerously via a *stale name*: an equivalence written against a class or attribute the translation retired still passes compose's existence checks and composes into the wrong union. `CanonicalMap` makes the translation a single source of truth serving both moments, and is **completed** along the declared cluster (an unmapped member inherits the cluster's `into` label). It maps `vertices`, `properties` **and** `relations`.

```python
from graflo.architecture.evolution import (
    CanonicalMap,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_and_complete_canonical_map,
)

cm = CanonicalMap(
    vertices={"Firm": "Company"},
    properties={"Firm": {"firm_id": "company_id"}},
)
canonical_left = apply_evolution(left, canonical_map_to_ops(cm))

# Multi-side maps are allowed: canonical_maps=[("left", cm), ("right", cm_b)]
side_maps = validate_and_complete_canonical_map(
    op,
    left=canonical_left,
    right=right,
    canonical_maps=[("left", cm)],
)
# side_maps.left / .right are the per-side maps the clusters lower to,
# e.g. side_maps.right.vertices == {"Org": "Company"}
composed = compose_manifests(
    canonical_left, right, op, canonical_maps=[("left", cm)]
)
```

`canonical_map_to_ops` emits property renames first (keyed by the *source* class names), then class merges (only with `allow_merges=True` — collapsing two classes is a stated intent, not a rename) and renames, then the same pair for relations. It is the **one lowering** both moments share: compose lowers each declared cluster to a per-side `CanonicalMap` and applies it through this function, rather than re-implementing rename/merge resolution of its own.

`merge_canonical_maps(base, extension)` is the single conflict primitive underneath: a partial-function union where every source maps to one target and a **target of `base` is a fixed point** the extension may not re-map. Two author maps for one side reconcile through it, and so does an author map against the lowered cluster map.

`validate_and_complete_canonical_map` indexes the declared clusters and raises `ComposeCanonicalConflictError` (wrapping `ClusterConflictError` for a cluster contradiction) on: a stale pre-canonical class or relation name referenced by an equivalence; an `into` that re-targets a class the canonical map already fixed; a property equivalence that uses a retired attribute name, re-targets an attribute the map already routed, renames an undeclared property, or renames onto one that already exists. On success it returns `SideMaps` — the per-side maps the clusters lower to, ready for `canonical_map_to_ops`. It deliberately re-checks nothing compose already raises on (collisions, incompatible types, divergent funnels).

Self-relations are no longer merely warned about: compose forwards `allow_self_relations` / `allow_row_fusion` from the op to the per-side `MergeVerticesOp`, so the unary guards fire unless the author acknowledges them.

### Identity alignment

When the composed class should deduplicate entities across its sources, the identity question splits along a principle: **a primary identity is a property of the class**, so the class declares one identity over canonical attributes only — while *how* each source populates those attributes is resource knowledge, expressed as pipeline steps. `IdentityAlignment` states both halves declaratively; put it on `ComposeManifestsOp.identity_alignments` so `compose_manifests` applies it after the schema/resource union (each entry's `vertex` must be a declared cluster's `into` label). Under the hood `alignment_to_ops` still emits only fundamentals:

```python
from graflo.architecture.evolution import (
    AlignmentRow, ComposeManifestsOp, DerivationSpec, IdentityAlignment,
    LocalKeySource, LocalKeySpec, VertexEquivalence,
    compose_manifests,
)

alignment = IdentityAlignment(
    vertex="Company",
    rows=[AlignmentRow(into="match_key", sources={     # priority order
        "r_a": DerivationSpec(input=["secondary_key", "shared_raw"],
                              params={"prefix": "abc_", "strip_prefix": "ABC-"}),
        "r_b": DerivationSpec(input=["org_id", "shared_raw"],
                              params={"prefix": ""}),
    })],
    local_key=LocalKeySpec(sources={                   # fallback, namespaced
        "r_a": LocalKeySource(field="firm_id", tag="a"),
        "r_b": LocalKeySource(field="org_id", tag="b"),
    }),
    secondary_identities={"by_company_id": ["company_id"],
                          "by_org_id": ["org_id"]},
)
op = ComposeManifestsOp(
    vertices=[
        VertexEquivalence(left="Company", right="Org", into="Company"),
    ],
    identity_alignments=[alignment],
)
union = compose_manifests(canonical_left, right, op, canonical_maps=[("left", cm)])
```

The emitted op sequence: `AddVertexPropertiesOp` (declare the canonical attributes), `AddResourceTransformsOp` (per-resource derivation steps, inline calls), `ReplaceIdentityOp` (a priority funnel — one branch per row in order, then the `local_key` fallback; `retire: keep`), and `AddSecondaryIdentitiesOp` (the retired side keys as lookup-only secondaries). Row order is funnel priority: a record keys by the highest-priority attribute it carries, so two records fuse when their strongest present attribute coincides — a match on a lower-priority attribute does not fuse records when one side also carries a stronger one. The `local_key` values are namespaced per resource (`a:f2` vs `b:o1`), so non-aligned records stay ingested without cross-source collisions.

Two rules the validator enforces (`validate_alignment`, raising `AlignmentConflictError`): derivation inputs are **raw source-doc field names** — property renames rewrite `vertex.from` maps so documents keep their original keys, and transform inputs are never rewritten (pass `canonical_maps` to catch canonical names used by mistake); and alignment targets must not collide with the class's current primary-identity fields.

Worked end-to-end in [Example 19](../../examples/example-19.md).

## API

```python
from graflo.architecture.evolution import (
    AddInverseEdgesOp,
    ComposeManifestsOp,
    EdgeSelector,
    MergeEdgesOp,
    MergeVerticesOp,
    ProjectManifestOp,
    RenameRelationsOp,
    RemoveVerticesOp,
    SanitizeOp,
    apply_evolution,
    apply_sanitize,
    compose_manifests,
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
- **Imports**: `graflo.architecture.evolution` re-exports the ops and apply helpers; lower-level functions such as `apply_remove_vertices`, `apply_merge_vertices`, `apply_rename_relations`, `apply_add_inverse_edges`, `apply_rename_vertex_properties`, and `apply_sanitize` mutate a manifest in place (used mainly internally and by `Sanitizer`). Cross-manifest compose uses `compose_manifests` (not unary `apply_evolution`).

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

**Choosing a bidirectional strategy** (see also [Core components — Edge](../architecture/core_components.md#directed-undirected-and-bidirectional-edges)):

| Goal | Approach |
|------|----------|
| Portable across DBs | Two logical directed edges + `AddInverseEdgesOp` |
| TigerGraph-native pair, single load path | One logical edge + `db_profile.edge_specs[*].reverse_edge` |
| Truly symmetric (friends, co-authors) | One logical edge with `directed: false` → `UNDIRECTED EDGE` on TigerGraph |

### 6) Project to a subgraph slice

Use when you need a smaller manifest that retains only specific vertex types and edge triples (for example agent experiments or publishing a focused contract):

```python
from graflo.architecture.evolution import (
    EdgeSelector,
    ProjectManifestOp,
    apply_evolution,
)

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

## Replacing a vertex identity

`ReplaceIdentityOp` is the one operation that touches how a vertex is *keyed*, so it
carries an explicit policy for the identity being retired.

```python
from graflo.architecture.evolution import ReplaceIdentityOp, apply_evolution

evolved = apply_evolution(
    manifest,
    [
        ReplaceIdentityOp(
            vertices={
                "party": {
                    "to": {"mode": "natural", "identity": ["party_uid"]},
                    "retire": "demote",  # default
                    "retire_as": "by_legacy",
                    "endpoints": "follow_new",  # default
                }
            }
        )
    ],
)
```

After this, `party` upserts on `party_uid`, and the old `legacy_id` key survives as a
secondary identity named `by_legacy` — usable by any edge step that names it, and
automatically indexed. Edge steps that were matching on the primary identity now match
on `party_uid`; pass `endpoints: "pin_to_retired"` to keep them on `by_legacy` instead.

The `to` block reaches every identity mode:

| `to.mode` | Required | Result |
|---|---|---|
| `natural` | `identity: [...]` | Named properties are the key |
| `hash` | `hash_from: [...]` | Deterministic synthetic `id` digested from those fields |
| `assigned` | — | Intentional UUID primary key |
| `blank` | — | Auto-generated placeholder ID |

Things worth knowing before you reach for it:

- **Demotion is downgraded to `keep`** when the old identity was synthetic (`hash`,
  `assigned`, `blank`) or already equals the new one — demoting a generated `id` would
  create a lookup key no source carries. The op logs a warning when it does this.
- **`mode: blank` cannot demote at all.** A blank vertex may not declare secondary
  identities, so the op raises and points at `keep` / `drop`.
- **New identity fields must already be declared.** `Vertex.set_identity` would happily
  synthesise them as untyped fields, which makes an empty column the primary key; the op
  refuses and points at `AddVertexPropertiesOp`.
- **A no-op replacement does not bump the schema version.**

This is a contract-level change only. To see what it implies for a populated database,
diff the schemas — a mode change or a non-widening key swap emits `CHANGE_VERTEX_IDENTITY`
**and** `REKEY_VERTEX`, both CRITICAL and blocked by `MigrationPlanner` unless high risk is
explicitly allowed.

## Deriving a change set

Individual ops rewrite a manifest. Recording *sequences* of them — so a
manifest's history can be stored, replayed and verified — is
[version control](versioning.md), which has its own page.


```python
from graflo.architecture.evolution import diff_manifests_verified

ops, warnings = diff_manifests_verified(base, target)
```

`diff_manifests` is the only producer of `ManifestOp` values — the `migrate`-plane
`SchemaDiff` emits *description records* that cannot be applied, and never looks at
`ingestion_model` or `bindings`. The `_verified` variant additionally checks the

> **replay invariant:** `manifest_hash(apply_evolution(base, ops)) == manifest_hash(target)`

and reports the residual when it does not hold, rather than letting a partial change set
pass as complete.

**Renames need hints.** A dropped `mail` plus an added `email` is structurally identical to
a rename, and guessing would turn a data-preserving rename into a destructive drop. Supply
`RenameHints` when the intent is known:

```python
ops, _ = diff_manifests(
    base, target, hints=RenameHints(vertex_properties={"party": {"mail": "email"}})
)
```


### Where this goes next

`diff_manifests` produces the change set; a **commit** records it with the
content hash before and after, so replay is verified rather than assumed, and
commits form a **DAG** rather than a line. Merging two branches of that DAG,
resolving conflicts, and replaying a recorded resolution are all covered in
[Version control](versioning.md).

## Scope notes

- **Transforms**: bodies of named transforms are not rewritten when vertex *field* names change during a merge; that remains an authoring concern. Use **`RenameVertexPropertiesOp`** / **`SanitizeOp`** when you need coordinated field rewrites at the manifest boundary.
- **Identity ops are contract-level too**: `ReplaceIdentityOp` rewrites the manifest, it does not re-key stored vertices. Propagating identity changes to a live database is not yet supported.
- **Bindings**: connector definitions are unchanged; only `resource_connector` rows pointing at dropped resources are removed after a remove operation.

## See also

- [Creating a Manifest](../../getting_started/creating_manifest.md) — manifest structure
- [Concepts overview](../index.md) — `GraphManifest` role in the pipeline
