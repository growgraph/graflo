# Manifest evolution

GraFlo provides **contract-level** operations that transform a validated `GraphManifest` into a new manifest: logi./ru  cal vertices and edges, ingestion resources, optional bindings wiring, and the database profile are updated together. This is **not** an in-database migration of existing graph data; the intended workflow is to publish the new manifest and **reingest** from sources.

## Identity and validation

- **Stable hash**: use `manifest_hash` from `graflo.migrate.io` (see [`graflo.migrate.io`](../../reference/migrate/io.md)) to compare the merged `schema`, `ingestion_model`, and `bindings` blocks before and after an evolution.
- **Validation**: `apply_evolution` in `graflo.architecture.evolution` returns a deep copy and runs `GraphManifest.finish_init()` by default so the same cross-block checks apply as when loading YAML. API reference: [`graflo.architecture.contract.manifest`](../../reference/architecture/contract/manifest.md).

## Operations

| Operation | Summary |
|-----------|---------|
| **Remove vertices** | Drops named vertex types, removes incident edges, and trims ingestion **step-wise**: `vertex` / `edge` steps naming a removed type go, a `vertex_router` loses only its `type_map` / `vertex_from_map` entries for it and keeps routing the rest (a pass-through router needs no edit — a discriminator value naming an undeclared class is skipped at ingestion), a `descend` stays while anything survives under it. A resource is dropped only when it no longer produces or references any surviving type. Also trims `merge_collections`, `infer_edge_*` / `extra_weights` entries on removed endpoints, filters `resource_connector` rows, and updates `db_profile`. Fails if ingestion would be left with no resources. |
| **Merge vertices** | Merges one or more source vertex types into a target name (`into`). If `into` already exists, sources are merged into it; otherwise a new vertex type is built from all sources. Endpoints on edges are rewritten and duplicate `(source, target, relation)` edge kinds are merged. Resource pipelines, `infer_edge_only` / `infer_edge_except`, and `extra_weights` are rewritten; `db_profile` logical keys follow the merge. Properties fuse by exact name: `type` and `item_type` are compared as a unit, descriptions and grounding are unioned, and a conflicting type, LIST element type, or `unit` raises — naming every conflicting property at once, and naming the merge target rather than the source spellings. Conflicting default-value maps raise too. |
| **Rename vertices** | `renames: {old: new}` (injective). Renames logical vertex type names across schema, edge endpoints, ingestion pipelines/selectors, and bindings resource references. |
| **Rename relations** | `renames: {old: new}` (injective). Renames logical edge `relation` values across schema, ingestion selectors/pipelines, and `db_profile` edge metadata. |
| **Rename resources** | `renames: {old: new}` (injective). Renames ingestion resource names and all bindings references (`connectors[].resource_name`, `resource_connector[].resource`). |
| **Remove edges** | Removes edges from schema, `db_profile.edge_specs`, `default_property_values.edges`, and ingestion selectors. Two addressing forms, combinable: `relations` removes a relation on every endpoint pair; `edges` (`(source, target, relation)` triples) removes exactly those pairs, and is the only way to remove an edge with no relation set. |
| **Merge edges** | Canonicalizes multiple relation names into one relation, then merges duplicate edge identities and deduplicates edge/profile defaults. |
| **Rename vertex fields** | Per-vertex `{old_field: new_field}` maps: updates schema field names, identities, `db_profile` index specs, and ingestion (`vertex` `from`, `transform.rename` targets) so documents still use the **source** column names where a reverse map is injected. |
| **Remove vertex fields** | Removes vertex properties, prunes vertex/edge index references, and rewrites ingestion references (`from`, `keep_fields`, `vertex_weights`). |
| **Add vertex fields** | Adds properties to existing vertices for schema enrichment and migration planning. |
| **Rename edge fields** | Per-relation edge property renames across schema edge properties/identities, `db_profile` edge indexes/defaults, and edge actor `properties` payloads. |
| **Remove edge fields** | Removes per-relation edge properties, prunes edge index/default references, and rewrites edge actor `properties`. |
| **Add edge fields** | Adds properties to existing relations. An entry is a bare name (untyped) or a full `Field` carrying type and grounding, as for vertices. Rejects an unknown relation. |
| **Declare / retract edge inverses** | `declare_edge_inverses` (`inverses: {R: R_inv}`, `symmetric: [R, ...]`) records pairs in `edge_config.inverses` and self-inverse relations in `edge_config.symmetric`; `retract_edge_inverses` (`relations: [...]`, either side of a pair, or a symmetric name) withdraws them. Logical only — creates no edge. A pair is unordered: `{a: b}`, `{b: a}` and `{a: b, b: a}` declare the same thing, and restating a declaration is a no-op. A declaration giving a relation a second inverse is refused. Retracting is refused while a native inverse still realizes the pair, or while an edge step naming exactly one edge of it still sets `emit_inverse`. Inverses of each other; the differ emits both. See [Inverse and symmetric relations](#5-inverse-and-symmetric-relations). |
| **Add inverse edges** | **Materializes** declared pairs as explicit logical edges, portable to every backend. `relations: [R, ...]` names paired relations (either side of a pair); omitted, it realizes every declared pair, both sides, skipping relations whose inverse is native. It never declares: an undeclared relation is refused, and so is a symmetric one (its edges are undirected). For each directed `(S, T, R)` adds `(T, S, R_inv)` unless it exists, with a physical spec copied minus `relation_name`, and sets `emit_inverse` on the edge steps that write `R` — no step is generated, so every way a step can name its relation is covered. A resource that already writes the inverse with a step of its own is left alone. A named relation whose inverse is already native is refused. Reversible: the inverse removes exactly the created edges, and removing an edge clears the flags that fed it. |
| **Set inverse emission** | `steps: {resource: [{at, step, link}]}` + `enabled` set or clear `emit_inverse` on edge steps addressed by position — the ingestion half of a materialized inverse, as a primitive. Enabling is refused on a step that names exactly one edge whose relation has no declared pair, is symmetric, or has no declared inverse edge. Its own inverse, over the steps that actually change. One merge slot per resource. |
| **Set native inverses** | Realizes declared pairs **physically**: `relations` + `enabled` add to or remove from `db_profile.native_inverses`, so TigerGraph maintains the pair (`WITH REVERSE_EDGE` on the relation's edge type, named after the declared inverse). Refused without a declared pair, for a symmetric relation, where explicit edges carry the inverse name, on both sides of one pair, when `relation_name` overrides split the relation over several edge types, and on non-TigerGraph profiles. |
| **Project manifest** | Keeps a logical subgraph by vertex names and/or edge triples `(source, target, relation)`. Prunes isolated vertex types from `keep_vertices` when they have no surviving edges (`connectivity: induced_prune`). Cascades to schema, `db_profile`, ingestion (pipeline steps, infer selectors, `extra_weights`), and bindings through the same removal as **Remove vertices**, so a `vertex_router` keeps routing the kept types. Optional `keep_resources` filters ingestion resources. Optional `depth` expands `keep_vertices` into seeds for an n-hop neighbourhood walk (`direction` orients it), yielding the induced subgraph on the hop ball. Inverse edges are not kept by default; `keep_inverse_edges: true` keeps the declared mirror of every edge in `keep_edges`, so a materialized pair survives as a pair. Fails if ingestion would be left empty. |
| **Replace identity** | `replacements: {vertex: {to, retire, ...}}`. Per-vertex identity policy swap covering both field-set and **mode** changes (`natural` / `hash` / `assigned` / `blank`). `retire` decides what becomes of the old field-set — `demote` (default) turns it into a secondary identity, `keep` leaves it as plain properties, `drop` removes it. `endpoints` decides whether edge steps follow the new identity (`follow_new`, default) or stay pinned to the demoted one (`pin_to_retired`). Drops `db_profile` indexes that encoded the retired identity. See [Replacing a vertex identity](#replacing-a-vertex-identity). |
| **Add / remove secondary identities** | Declares or withdraws alternate lookup keys on existing vertices. Each field-set's non-unique index is *derived* by `Schema.finish_init`, so adding one needs no index authoring; removing one drops the derived index explicitly. Removal is rejected while an edge step still selects the field-set. |
| **Replace edge identities** | Replaces `Edge.identities` (uniqueness keys) per `(source, target, relation)`. No retire policy — edge identities have no lookup plane. Non-endpoint tokens are merged into edge `properties` by `Edge.finish_init`. |
| **Add vertices / add edges** | Introduces new logical vertex types and edge relations unarily — the counterpart to what `MergeManifestsOp` could previously only do binarily. Rejects existing names/triples and unknown endpoints. |
| **Retarget edges** | Changes which vertex types an edge connects, preserving its properties, `identities`, `directed` flag, and `db_profile` physical spec — all of which a remove-plus-add would lose. Rewrites the `EdgeId` in `edge_config`, `edge_specs`, and pipeline edge steps, keyed on the full triple so a different relation between the same types is untouched. |
| **Change field types** | Sets `Field.type` / `item_type` on vertex or edge properties. Validated against the profile's `db_flavor` via `graflo.db.field_type_support`, so an unsupported LIST target fails at op time rather than at define time. Refuses to make an identity field a LIST. |
| **Add / remove vertex & edge indexes** | Authors `db_profile.vertex_indexes` and `edge_specs[].indexes` directly. Indexes derived from `secondary_identities` cannot be removed this way — they would be re-registered by the next `finish_init`, so the op points at **remove secondary identities** instead. |
| **Set edge directed** | Sets `Edge.directed` on selected triples. Load-bearing for replay: `directed` decides what **add inverse edges** may duplicate, an undirected edge may not carry a declared pair, and an edge naming a symmetric relation must be undirected — so the differ retracts declarations before flipping `directed`, and declares after. |
| **Set db profile** | Replaces the whole `db_profile`. The four index ops reach `vertex_indexes` and `edge_specs[].indexes`; nothing reached `db_flavor`, `target_namespace`, `vertex_storage_names`, `default_property_values` or the rest of a spec — and those are content-hashed, so a change set that moved one could not replay. Carries the indexes too and is emitted *instead of* the index ops, never alongside them. |
| **Set bindings** | Replaces the whole `bindings` block; `null` removes it. The block previously had no op at all, so any diff touching it was inexpressible and a merge that unioned two registries could not be recorded. Wholesale by design: in a three-way merge the block is one slot, so two independent bindings edits conflict — granular connector ops would refine that without changing this op's meaning. |
| **Sanitize** | Target-`DBType` policy: reserved-word-safe names on `DatabaseProfile`, reserved vertex field renames, and (for TigerGraph) consistent identity tuples per edge relation. This is the same work **`graflo.hq.sanitizer.Sanitizer`** applies by building a single **`SanitizeOp`**. |
| **Ensure extracted fields** | Widens a producing step's projection so named fields survive extraction (`keep_fields` gains them; under `extraction_scope: mapped_only` so does `vertex_from_map[<class>]`, seeded from the router-level `from`). Only `vertex_router` steps need it — a plain `vertex` step reads the transform buffer directly, bypassing both knobs. A no-op on an unrestricted step. Requires `ingestion_model`. |
| **Add resource transforms** | Appends transform steps to a named level of named resources' pipelines (`at`, as `descend` step indices; root by default — actor type-priority ordering runs them before vertex extraction at that level) and optionally registers named transforms (loud on same-name/different-body, mirroring merge). The only op whose primary effect is ingestion; requires `ingestion_model` and raises otherwise. Steps may reference the registry via `call.use` or carry a fully inline `call` (collision-free). Irreversible. |
| **Add / remove resources** | `add_resources` takes full `ResourceConfig` definitions (creating `ingestion_model` if absent; an existing name is rejected) plus optional named `transforms` their steps reference via `call.use`, unioned into the registry exactly as `add_resource_transforms` does; `remove_resources` takes names and prunes the `resource_connector` entries that wired them. Inverses of each other — a removal that pruned a binding has none, and neither does an addition that registered a transform the manifest did not already hold, since no op withdraws one. The differ emits both, carrying the transforms new resources need. |
| **Set vertex / edge / field semantics** | Ground an existing element in an external vocabulary: `set_vertex_semantics` (`{vertex: Semantics \| null}`), `set_edge_semantics` (edge triples + one `Semantics`), `set_field_semantics` (per-target `FieldSemantics`, the only model carrying `unit`; a target is a vertex property `{vertex, field}` or an edge property `{source, target, relation, field}`). `null` clears, which is what makes each invertible. Never consulted at execution time. The differ emits all three. |
| **Set vertex descriptions** | `set_vertex_descriptions` (`{vertex: text \| null}`) sets or clears the description of existing types — what a merge that folds two types together writes when it joins what each side said. `null` clears, so it is invertible. Never consulted at execution time. The differ emits it. |
| **Canonicalize** | Applies a whole **vocabulary map** — classes, per-class attributes (keyed by the *source* class), and relations — in one step over the original schema. Attribute renames run first, then classes and relations simultaneously, so a chain (`{X: Z, Z: Q}`) and a swap resolve without an intermediate state and no op order can leak into the result. The fibers of the map are exactly the groups that merge, so a group of more than one name needs `allow_merges`; a target that already exists and does not move must be declared a member of its own group with a self entry (`Company: Company`), or the op refuses rather than merging into it silently. Reversible when it only renames. This is what a `CanonicalMap` lowers to, and the per-side step of merge. See [Canonical maps](#canonical-maps). |
| **Merge manifests** | Binary merge of two full `GraphManifest`s (schema **and** resources/bindings) via `MergeManifestsOp` + `merge_manifests(left, right, op)`. Consumes **explicit** equivalence maps only (no semantic inference): n-ary vertex clusters (`vertex_equivalences`: `left` / `right` each name one or more classes collapsing onto one `into`), property alignment, optional merged `identity`, optional `identity_alignments`, relation equivalences (`relation_equivalences`), resource renames, and `canonical_maps` (scoped `left` / `right` / `both`), which name the merged classes so `into` may be omitted. A name both sides carry that no equivalence covers is what `name_conflict` decides — refused with the declarations to add (`error`), unioned by name through a synthesized equivalence (`union_right`), or kept apart (`prefix_right`). Distinct from unary `MergeVerticesOp`. Rejected by unary `apply_evolution`. |

## Merge two manifests

GraFlo stays deterministic: an external tool (or a human) may *propose* equivalences; core only *applies* them.

```python
from graflo.architecture.evolution import (
    MergeManifestsOp,
    PropertyEquivalence,
    RelationEquivalence,
    VertexEquivalence,
    merge_manifests,
)

merged = merge_manifests(
    left,
    right,
    MergeManifestsOp(
        vertex_equivalences=[
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
        relation_equivalences=[
            RelationEquivalence(left="places", right="billed", into="activity")
        ],
        resource_renames={},  # right resource name -> merged name
        name_conflict="error",  # or "prefix_right" / "union_right"
    ),
)
```

`left` / `right` also take a **list**, which is how an n-ary cluster is
spelled — one declaration naming every member that collapses onto `into`:

```python
MergeManifestsOp(
    vertex_equivalences=[
        VertexEquivalence(
            left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
        )
    ],
    allow_merges=True,  # >1 member on a side is a stated intent
    allow_self_relations=False,  # forwarded to the per-side CanonicalizeOp
    allow_observation_fusion=False,
)
```

Name-disjoint sides need no equivalence at all: with `vertex_equivalences` / `relation_equivalences` empty they merge into a **disjoint union**, both resource sets and bindings retained. A name *both* sides carry is not a disjoint union and is never silently treated as one — it is what `name_conflict` decides (see the [case table](#where-a-map-and-the-equivalences-can-stand)).

A `VertexEquivalence` declaration *is* one cluster. `into` is optional — see [Canonical maps](#canonical-maps) for how a merged name is found. `ClusterConflictError` (raised before any rename) covers the three ways declarations can contradict each other: a class **claimed by two** declarations, two declarations **sharing one merged name** (that collapse is one n-ary cluster and must be spelled as one), and a merged name that already names an **existing non-member class** on a side (which would silently merge into an unrelated type). Properties with the **same spelling** on both sides after alignment fuse for free; list a `PropertyEquivalence` only to rename, to map per member (`left={"Company": "company_key", "Shop": "shop_key"}`), or to flag identity.

That fusion is a **union**, and it compares more than a name. `type` and `item_type` travel together, so `LIST<STRING>` and `LIST<INT>` are a conflict rather than a shared `LIST`; descriptions from both sides are kept; grounding blocks union their `exact_match` and `synonyms`, and a disputed `iri` clears rather than electing one side's concept. Two disagreements refuse the merge instead of resolving it: two declared **types** for one property, and two declared **units** — a property that is `m/s` on one side and `km/h` on the other would hold numerically incomparable values once fused. Edge properties fold by exactly the same rule, since it is one kernel. Neither is widened automatically, because the merged type would be one neither author wrote; retype one side with `change_field_types` first. Every conflicting property is named in one error rather than one per run.

Merge refuses to guess the merged **identity** too: when members disagree on their identity field-set after alignment and nothing resolves it, `MergeIdentityError` names each member's key. Resolve it with `identity` on the cluster (a natural key, an `IdentityFunnel`, or a `SideIdentity` shorthand lowered to one funnel), a `PropertyEquivalence(identity=True)` flag, or an `identity_alignments` entry. A declared `identity` demotes each member's retired key to a lookup-only secondary identity unless the cluster sets `retire="keep"`.

### What merge does, in order

`merge_manifests` is one pass with a fixed order. Two steps are where they are for a reason, noted below.

1. **Fold the declared maps** per side — `both` under `left`, `both` under `right` (`compose_canonical_maps`), refusing two maps that disagree on a source or move each other's targets.
2. **Resolve the clusters** against those maps: members in each manifest's own spelling, one merged name each, and one composite relabel per side.
3. **Synthesize** a cluster for every name both sides still carry, as `name_conflict` directs, then re-resolve so the synthesized ones are ordinary clusters from here on.
4. **Capture each member's identity key and property names** — *before* the relabel, because once the members are collapsed onto one name the schema no longer records which key came from which member, and the merged identity is decided by comparing exactly those.
5. **Rename the right side's resources** per `resource_renames`, then per the collision policy.
6. **Snapshot both sides as they now stand** — also *before* the relabel: the relabel rewrites a `vertex_router`'s `type_map` values to the merged name, after which nothing says which router key produced which member. Identity alignment needs that, so it reads these snapshots.
7. **Apply the composite relabel** to each side, one `CanonicalizeOp` per side.
8. **Prefix the right side's remaining vertex and relation collisions** (`prefix_right` only — `error` and `union_right` settled theirs in step 3).
9. **Union schema, ingestion and bindings by name**, merging each name both sides carry, deciding the merged identity, and demoting retired keys to secondary identities.
10. **Assert no canonical split** on the result: no two merged types may denote one concept under different spellings.
11. **Bump the version, apply `identity_alignments`, `finish_init`.**

### Words for combining things

Five verbs recur across seven senses, and they are not synonyms.

| verb | sense | where |
|---|---|---|
| **merge** | join two manifests of *unrelated lineage* by declared equivalence | `merge_manifests`, `MergeManifestsOp` |
| **union** | assemble two collections **by name** — the outer step | `_union_transforms`, `_union_schema`, `_union_bindings` |
| **merge** | combine the definitions **one name** has on both sides, refusing conflicts — the inner step | `merge_vertex_models`, `merge_edge_pair`, `merge_semantics` |
| **merge** (collapse) | send *several distinct* classes or relations to one name | `MergeVerticesOp`, `MergeEdgesOp`, `allow_merges` |
| **merge3** | reconcile two descendants of a **common ancestor** — a different operation entirely | `merge_three_way`, `MergeResult`, `MergeConflict` |
| **fuse** | two *records* becoming one node at ingestion | `allow_observation_fusion`, identity alignment |
| **collapse** | cluster members arriving at their merged name | `VertexEquivalence`, `CanonicalizeOp` |

Union and merge are not competing words: they are the two levels of one operation. The union walks the names; the merge is what it does at a name both sides carry.

`_union_transforms` is the plain case — it folds a registry by name, keeping one copy of an identical body and refusing two different ones. `_union_schema` is a union *and* the merges it drives, which is why it reads as the whole loop rather than the outer step alone. `_concat_ingestion` is neither: resource name collisions are settled earlier in the pipeline, so by the time the lists meet there is nothing left to fold and it concatenates.

Collapse and merge-at-a-name share one implementation — `merge_vertex_models` is called both by `MergeVerticesOp` and by the union — because at the field level they are the same work: union the properties, reconcile the identity, refuse a conflicting type or unit. What differs is the author's claim about the inputs, and `allow_merges` is where that claim is made: several *distinct* classes becoming one is a stated intent, while two views of one class needs no acknowledgement.

`fuse` is about records, not types. The one exception is the `name_conflict="union_right"` policy, which was spelled `fuse_right` before this distinction was drawn and still parses under that name.

`merge3` is the outlier: it reconciles change sets, not schemas, and expects names to *agree*. See [Merge is not merge3](versioning.md#merge-is-not-merge3).

#### Reading a bare `merge`

`merge3` is a qualifier, not a separate word. The three-way operation is still
called a merge in prose, and the names it carried before this distinction was
drawn — `merge_three_way`, `find_merge_base`, `MergeResult`, `MergeConflict`,
`re_merge`, `merged_hash` — keep a bare `merge`. The rule for reading one:

> Inside `evolution/merge3.py`, `plot/merge3.py` and the version-level lineage
> routes (`POST /{scope}/{uuid}/merge` and `/re-merge`), a bare **merge** is the
> three-way. Everywhere else it is the model operation.

The qualifier is spelled out wherever the two would otherwise be
indistinguishable: the commit kind (`merge` against `merge3`), the CLI verb
(`graflo merge` against `graflo merge3`), `MergePreview` against
`Merge3Preview`, and `record_merge` against `record_merge3`.

**`join`** is not an eighth sense. It is used in prose as the superordinate for merge — "joins two lineages" — and never names an operation; nothing is called `join_*` except SQL and URL joins, which are unrelated.

#### These names match the literature

The generic-model-management operators GraFlo's design draws on (see [the
references](versioning.md#further-reading)) use two of these words, and GraFlo
spells both the same way they do:

| GraFlo | that literature | signature |
|---|---|---|
| `merge_manifests(left, right, op)` | **Merge** | two models plus correspondences → a model |
| `compose_canonical_maps(base, extension)` | **Compose** | two mappings → a mapping |

GraFlo's *merge* is that literature's Merge, and composing two canonical maps is
its Compose. The two were spelled the other way round until they were swapped:
what is now `merge_manifests` was `compose_manifests`, and what is now
`compose_canonical_maps` was `merge_canonical_maps`.

After the swap, `compose` names exactly two things in the codebase, and both are
that literature's Compose or a sibling of it: `compose_canonical_maps`, and the
`COMPOSES` relation the registry writes for artifact containment
(`POST /registry/manifests/compose`).

### Either side may carry no schema

A manifest needs only one block, so an overlay carrying just an `ingestion_model` and/or `bindings` — a new source wired onto an existing type vocabulary — is a valid merge input. The merged schema is the other side's, copied verbatim: physical profile, target namespace, secondary indexes and schema version all survive. With neither side carrying one, the merged manifest has no schema block and the version bump is a no-op.

Verbatim rather than "merged with an empty schema" on purpose: filling a missing side with an empty `Schema` would merge against a profile nobody wrote, and drop that side's namespace and version — a wrong answer with nothing raising.

When both sides *are* present the profile fold elects neither. `db_flavor` and `target_namespace` are single-valued and decide what DDL is emitted against which backend, so two **declared** values raise — unless the op sets `target_namespace`, which supersedes both and is validated against the merged flavor; a side that never declared one yields to the side that did. The merged schema's `metadata.name` folds to a flat, deduplicated `left+right` (or the op's `name`); it is a label, and the namespace is derived from it per flavor when unset (see the graph namespace guide). The merged version bumps from the higher side's. Declaration is read from what a side actually wrote, not from the value — `db_flavor` defaults to Arango, so a value-based fold could not tell a side that chose Arango from one that never spoke, and an undeclared left would silently retarget a right that named its backend. `default_property_values` union, refusing two defaults for one property. Vertex indexes union on their full definition: two entries over one field-set that disagree on `unique`, `type` or `sparse` raise rather than keeping the left's, since the next schema resolution collapses them on field-set alone and the survivor would depend on ordering.

### From the shell

```bash
graflo merge LEFT.yaml RIGHT.yaml --op OP.yaml -o OUT.yaml \
  [--canonical-map SIDE=PATH]... [--name-conflict error|prefix_right|union_right] \
  [--bump-version minor|none] [--strict-references] [--dry-run] [--check-profile NAME] \
  [--store DIR] [-m LABEL]
```

`-m LABEL` also **records the merge** as a two-parent commit in `--store`
(default `.graflo/commits`). Both inputs are resolved to commits by content
address — a manifest is its hash and a commit records the tree it produced — so
a side that is in no history is refused by name rather than half-recorded. The
merged manifest is stamped with its parents, its recipe pointer and the commit
that produced it before it is written, so the file carries its own lineage.
Without `-m` the verb writes only the manifest, as before. See
[Version control](versioning.md#merge-is-recorded-too).

The verb applies the op and its canonical maps together: `--canonical-map SIDE=PATH` (`SIDE` one of `left`, `right`, `both`) is folded into the op's `canonical_maps`, so the same document may carry the maps itself. Omitting `--op` merges a disjoint union. `--name-conflict` overrides the op's policy: `error` refuses a name both sides carry, `union_right` unions by name, `prefix_right` keeps them apart under `r_` names. Exit `0` merged, `1` merge refused, `2` the command could not run.

A refusal names what to declare, and an *incomplete* one prints the declarations themselves — `MergeIncompleteError` carries a completion, which the verb renders as YAML on stderr below the message, ready to paste into the op:

```yaml
completion:
  kind: declare_equivalences
  vertex_equivalences:
  -   left: Deal
      right: Deal
      into: Deal
```

## Canonical maps

A `CanonicalMap` is a translation of a source vocabulary into canonical names — a partial function on names, identity where unmapped, over `vertices`, `properties` (keyed by *source* class) **and** `relations`. Two sources sharing a target is a merge and must be acknowledged with `allow_merges`. It is a **vocabulary**, so it is idempotent: a canonical name is a fixed point nothing maps away from, and a chain (`{X: Z, Z: Q}`) or a swap is refused at construction — that shape is a relabel, which `CanonicalizeOp` expresses directly. It has two uses that are one mechanism.

**On its own**, `canonical_map_to_ops(cm)` lowers it to a single `CanonicalizeOp`, which applies the whole map in one step over the original schema: attribute renames first (keyed by the source class), then classes and relations simultaneously. The op is more general than the map — a chain and a swap resolve without an intermediate state, the fibers of the map are exactly the groups that merge, and a target that already exists and does not move must be declared a member of its own group with a self entry (`Company: Company`), or the op refuses rather than merging into it silently. No op order can leak into the result.

**On a merge op**, `MergeManifestsOp.canonical_maps` names the merged classes. An equivalence cluster says *which* classes are one; the map says *what they are called*, so `into` is optional. The merged name of a cluster is `into` (translated through the map when the map maps it), else the canonical name the map gives a member, else the one spelling every member shares. A member may be spelled by its own name or by its canonical name. Maps are scoped: `left` / `right` apply to that manifest's own names; `both` to either side's names and to merged names — an entry that matches one side only applies there and is simply inapplicable on the other.

```python
from graflo.architecture.evolution import (
    CanonicalMap,
    MergeManifestsOp,
    VertexEquivalence,
    merge_manifests,
)

op = MergeManifestsOp(
    # {Firm} ~ {Org}: no `into` — the map names it Company
    vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")],
    canonical_maps={
        "left": CanonicalMap(
            vertices={"Firm": "Company"},
            properties={"Firm": {"firm_id": "company_id"}},
        )
    },
)
merged = merge_manifests(left, right, op)
```

### Previewing

`merge_manifests` raises at the first refusal — right for a function that
returns a manifest, but it means three bad declarations take three runs to
find. `preview_merge(left, right, op)` walks the same declarations and
reports **every** problem at once, as data:

```python
from graflo.architecture.evolution.preview import preview_merge

preview = preview_merge(left, right, op)
for finding in preview.blocking:
    print(finding.severity, finding.kind, finding.nodes, finding.message)
```

A `MergePreview` carries the declaration graph — each side's classes and
attributes, the clusters over them, the canonical names the maps establish —
plus `findings` at three severities: `refusal` is the one merge raised,
`possible` is everything the preview found on its own, `note` an acknowledged
heuristic such as a satisfied entry. Every finding names the nodes it is
about, and an incomplete one carries the same `Completion` the exception
does. Pass `attempt=False` to describe the declarations without merging.

It is not a second implementation of the rules: each check calls the function
merge itself calls, one declaration or one map entry at a time, so a refusal
on one unit does not hide the next. That extends to the schema union — the
preview runs `merge_vertex_models` and `merge_edge_pair` per cluster, so a type
clash, a unit clash, two identity modes that exclude each other, a divergent
funnel, a secondary identity claimed twice and an edge whose two declarations
disagree on `type`/`by` are all found by the code that decides them.

The invariant the tests hold it to is that whatever merge refuses, the
preview has a finding of a matching kind for. It **fails closed**: a refusal
the preview cannot classify fails the suite rather than skipping the case, so a
new rule cannot be added without a finding kind to report it under. What makes
a refusal classifiable is a `check` phrase on the exception — every refusal in
the merge and union path carries one.

From the shell, `graflo merge --plot conflicts.svg --preview-json
conflicts.json` writes both — **including when merge refuses**, which is the
case they are for. `--dry-run` prints the findings table on its own.

### Checking a map before merging

A canonical map is authored against one schema long before it meets another, and
a map of hundreds of entries is not debugged through a merge. `graflo
canonical-check MAP --left manifest.yaml` classifies every entry against that
one manifest — no op, no other side — and names each one that matches nothing,
with a near-miss candidate where another spelling denotes the same concept. It
exits 1 when entries dangle and 2 when it could not run, so it gates in CI.

Give it both `--left` and `--right` and it runs the full preview instead. Give
it `--trim OUT.yaml` and it writes the map narrowed to what the manifest
declares, so pruning a map is a change with a diff rather than something
discovered at merge time. In Python the same two are
`dangling_entries(cm, manifest)` and `trim_canonical_map(cm, manifest)`.

### Vocabulary

| term | type | meaning |
|---|---|---|
| **declared map** | `CanonicalMap`, folded into `DeclaredMaps` | a map the author wrote — `op.canonical_maps[scope]` or a `canonical_maps=` pair handed to merge |
| **cluster** | `Cluster` (resolved from `ClusterSpec`) | one equivalence declaration, resolved: its members per side in the manifests' own spelling, and its **merged name** |
| **cluster map** | — | per side, every member onto its merged name (the merged name itself included, so the op merges into it rather than refusing an occupied target) |
| **composite map** | `SideMaps`, one `CanonicalizeOp` per side | the cluster map plus every declared entry that applies to a non-member: what merge applies to that side before the union by name. A relabel, not a vocabulary — two clusters may chain when one merged name is renamed away by another declaration |
| **fixed point** | — | a canonical target; no map and no cluster may move it |
| **opinion** | — | what the declared maps say a member's canonical name is: the target it maps to, or itself when it is a fixed point |
| **satisfied entry** | — | a declared entry whose source is absent and target present on a side: taken as already applied by the caller (a heuristic — it is logged) |
| **dangling entry** | — | a declared entry matching nothing on any side it could apply to: a typo, refused. Every one on a side is named by a single refusal, with a near-miss candidate where one exists; `allow_dangling_entries` drops them instead |
| **synthesized cluster** | `Cluster.synthesized` | a cluster merge declares itself under `name_conflict="union_right"` for a name both sides carry, or two spellings of one name |
| **completion** | `Completion`, on `MergeIncompleteError` | the extension that would make an incomplete declaration consistent, as `VertexEquivalence` / `RelationEquivalence` documents |

Identity is **nominal**: a class is the same class across two manifests only by name or by declared equivalence. Nothing structural fingerprints it — content hashes address a manifest, not a class — and merge never infers a match.

### Where a map and the equivalences can stand

Renames merge, so canonicalizing a side on its own first and then declaring the cluster in canonical names is the same function as declaring it in raw names on an op that carries the map: `merge_manifests` accepts either. Canonicalizing the *union* afterwards is a different function in general (it can collapse two merged names) and is just a unary `CanonicalizeOp` on the result. `resolve_clusters` — what merge runs; `validate_and_complete_canonical_map` returns just its per-side relabels — folds the clusters and the maps into **one composite `CanonicalizeOp` per side**, applied before the schema/resource union.

One rule underlies every refusal: **the map and the equivalences must agree on where a name goes, and a canonical target is a fixed point neither may re-map.** Every case below is an instance of it.

**Per name.** For one name on one side, with `E` its cluster and `C` the declared entry that names it:

| case | outcome |
|---|---|
| neither | unchanged |
| `C` only, target free or self-declared | carried into the composite |
| `C` only, target an unmoving non-member without a self entry | refused by the op (occupied target) |
| `E` only | onto the merged name |
| `E` and `C` agree; `E` without `into` and `C` names a member; `into` itself in `dom(C)` | onto the merged name, which `C` supplies or translates |
| `E` names a member the side does not declare | `ValueError` naming the member — and, when another spelling on that side denotes the same concept, naming that too ("author the equivalence in the manifest's own spelling") |
| `E` with no `into`, no mapped member and no spelling its members share | refused as an **unnamed cluster**: give it `into`, or map a member |
| **contradiction** — `E` and `C` disagree; `E` moves a fixed point; a property equivalence renames a canonical attribute | `MergeCanonicalConflictError`, naming both declarations |
| **ambiguity** — a canonical name denotes two members; maps disagree on translating `into` | `MergeCanonicalConflictError` |
| **incomplete** — `C` sends a non-member onto a merged name | `MergeIncompleteError`; the completion is the cluster extended with that member, whose identity and property maps then govern it |
| one-sided `both` entry | applied where it matches |
| `both` entry over a merged name | translation of `into`, not a dangling entry |
| dangling | refused — one refusal lists every dangling entry on that side, each with a near-miss candidate where another spelling denotes the same concept. It outranks an `incomplete` on the same side: a name that is not there at all is the more basic mistake |
| dangling, with `allow_dangling_entries` on the map or the op | dropped and logged, for a shared vocabulary deliberately broader than this manifest. Off by default — a misspelt class has the same shape, and dropping it silently renames less than the author asked |
| satisfied | no-op, logged |
| `properties` keyed by a merged or canonical class | refused — the attribute map is keyed by the source class |
| `properties` renaming a field the member does not declare, or onto a field it keeps | refused — a property rename cannot merge two fields; align them with `PropertyEquivalence` on both sides |
| chain or swap inside one map | refused at `CanonicalMap` construction |

**Across the two sides.** These are facts about a *pair* of names, decided after each side's composite map has been applied:

| case | outcome |
|---|---|
| a name both sides carry, no cluster | `error`: `MergeIncompleteError`, whose completion declares the equivalence — naming the members in each side's **own** spelling, merged onto the shared name; `union_right`: a synthesized cluster; `prefix_right`: kept apart as `r_n` |
| two spellings of one name (`OrderLine` / `order_line`) | `error`: `MergeNameConflictError`; `union_right`: a synthesized cluster under the left spelling; `prefix_right`: kept apart |
| a **resource** or **connector** name both sides carry | matched **exactly only** — these are addresses, not concepts, so two that key alike split nothing and `union_right` behaves as `error`. Resolve with `resource_renames` or `prefix_right` |
| two **property** names that key alike (`customer_email` / `customerEmail`) | never fused: a property name binds to a key in the source document, so the two are fed by different columns. Only exact spellings fuse |
| two declared maps chaining (`{Z: Q}` and `{X: Z}`) | refused by `compose_canonical_maps`, in either order |

A synthesized cluster is a real cluster: the union it produces goes through the same identity reconciliation as a declared one, so two same-named classes whose keys disagree raise `MergeIdentityError` instead of merging to a key no record carries, and the right side's properties are unioned rather than dropped.

The cluster-shape checks (`ClusterConflictError`, wrapped by `validate_and_complete_canonical_map`) stay: a class claimed by two declarations, two declarations sharing one merged name, a merged name occupying an existing non-member class. `compose_canonical_maps(base, extension)` is the union two declared maps for one scope reconcile through — every source maps to one target, and a target of either map is a fixed point the other may not move. Merge deliberately re-checks nothing it already raises on (incompatible types, divergent funnels).

Self-relations and observation fusion are not merely warned about: merge forwards `allow_self_relations` / `allow_observation_fusion` from the op to the per-side `CanonicalizeOp`, so the merge guards fire unless the author acknowledges them. The fusion guard is judged per **accumulator slot**, which is what the runtime fuses on: a vertex step stores at its `role` sub-slot when it has one and at the bare level otherwise, and a router at its `role` (or `type_field`). Two members produced at one level by steps with distinct `role`s — the client/server or buyer/seller pattern, addressed from the edge by `source_role` / `target_role` — never share a slot, are not fusion, and need no flag. The flag is for two members produced by *bare* steps (or steps of one `role`) at one level, where a single document yields both and the merged observations fold into one node.

### Identity alignment

When the merged class should deduplicate entities across its sources, the identity question splits along a principle: **a primary identity is a property of the class**, so the class declares one identity over canonical attributes only — while *how* each source populates those attributes is resource knowledge, expressed as pipeline steps. `IdentityAlignment` states both halves declaratively; put it on `MergeManifestsOp.identity_alignments` so `merge_manifests` applies it after the schema/resource union (each entry's `vertex` must be a declared cluster's merged name). Under the hood `alignment_to_ops` still emits only fundamentals:

```python
from graflo.architecture.evolution import (
    AlignmentAttribute,
    MergeManifestsOp,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    VertexEquivalence,
    merge_manifests,
)

alignment = IdentityAlignment(
    vertex="Company",
    attributes=[
        AlignmentAttribute(
            name="match_key",
            sources={  # priority order
                "r_a": DerivationSpec(
                    input=["secondary_key", "shared_raw"],
                    params={"prefix": "abc_", "strip_prefix": "ABC-"},
                ),
                "r_b": DerivationSpec(
                    input=["org_id", "shared_raw"], params={"prefix": ""}
                ),
            },
        )
    ],
    local_key=LocalKeySpec(
        sources={  # fallback, namespaced
            "r_a": LocalKeySource(field="firm_id", tag="a"),
            "r_b": LocalKeySource(field="org_id", tag="b"),
        }
    ),
    secondary_identities={"by_company_id": ["company_id"], "by_org_id": ["org_id"]},
)
op = MergeManifestsOp(
    vertex_equivalences=[
        VertexEquivalence(left="Company", right="Org", into="Company"),
    ],
    identity_alignments=[alignment],
)
union = merge_manifests(canonical_left, right, op, canonical_maps=[("left", cm)])
```

The emitted op sequence: `AddVertexPropertiesOp` (declare the canonical attributes), `AddResourceTransformsOp` (per-resource derivation steps, inline calls), `ReplaceIdentityOp` (a priority funnel — one branch per attribute in order, then the `local_key` fallback; `retire: keep`), and `AddSecondaryIdentitiesOp` (the retired side keys as lookup-only secondaries). Attribute order is funnel priority: a record keys by the highest-priority attribute it carries, so two records fuse when their strongest present attribute coincides — a match on a lower-priority attribute does not fuse records when one side also carries a stronger one. The `local_key` values are namespaced per resource (`a:f2` vs `b:o1`), so non-aligned records stay ingested without cross-source collisions. The tag is required so that opting out is a statement: `tag=None` keeps the raw value as the local key, no separator — the author's claim that the values are already unique across every source of the class (UUIDs, IRIs, ids the source itself prefixes), where a namespace would only be noise. It is stored as the empty tag `""`, the neutral element, which is what survives serialization.

Two idioms decide *which* records participate, and `DerivationSpec.foo` selects between them. `gated_normalized_key` gates on a **sibling** field — participation is decided by one column, the key material comes from another — and its `strip_prefix` is `str.removeprefix`, a best-effort cleanup that is a silent no-op when the prefix is absent. `affix_gated_key` puts the test on the **value itself**: it reads one field, and a marker on that value is the admission test, so `company_A42` is stripped and accepted while a bare `A42` yields `None`. The marker is an affix pair — `prefix` and `suffix`, each defaulting to `""`, which every string carries — so it can lead, trail, or bracket the key, both halves are required when both are named, and naming neither admits everything while stripping nothing. Both return `None` to decline, which is a fall-through rather than a drop — an empty value skips the funnel branch listing that attribute, and the record keys on its `local_key` instead, ingested but outside the cluster. Reach for the marker form when the key column is self-describing, and for the gated form when a different column decides.

Rules the validator enforces (`validate_alignment`, raising `AlignmentConflictError`): derivation inputs are **raw source-doc field names** — property renames rewrite `vertex.from` maps so documents keep their original keys, and transform inputs are never rewritten (pass `canonical_maps` to catch canonical names used by mistake); alignment targets must not collide with the class's current primary-identity fields; and every referenced resource must produce the aligned class at exactly one resolvable pipeline level.

### The member is the unit of derivation

A cluster names its **members** — the classes it collapses, per side: `VertexEquivalence(left=["Company", "Shop"], right=["Org", "Branch"], into="Company")`. Every record that becomes `Company` was produced *as one member* by *one resource*: by a `vertex: Shop` step, or by a `vertex_router` key whose value was `Shop`. Canonical attributes are therefore derived **per member**. `sources` is keyed by resource because derivation inputs are that resource's raw column names; a resource that produces one member needs nothing more, and a resource that produces several says how, with one of three shapes:

| `sources[resource]` | When | Lowers to |
|---|---|---|
| one `DerivationSpec` | the resource produces one member, or its members share the key column *and* the marker convention | one step writing the attribute; behind a router, guarded on the values that route onto the class |
| a **list** of specs | several members, each carrying its key in its **own column** — the other column is empty, which already selects | one scratch field per spec + a `coalesce_fields` step as the single writer, every one of them guarded behind a router |
| a **dict keyed by member class** | several members, and *which member a document is* must decide — they share a column, or each has its own marker | one guarded step per member, each the single writer for its own documents |
| a `SharedDerivation` | the dict above, when the call is the same for every member and only a parameter differs (or nothing does) | expands to the dict; lowers the same way |

A member is keyed by its own name on its side or by its canonical name — `Firm` or `Company` when a map renames one to the other — as in the equivalence itself. The list form needs no member names because column presence selects; the dict form is the general one, and `SharedDerivation` spells it once for the common case — one call, the members sharing it, and per member only what varies:

```python
AlignmentAttribute(
    name="match_key",
    sources={
        "r_view": SharedDerivation(  # the member decides
            spec=DerivationSpec(input=["secondary_key"], foo="affix_gated_key"),
            members={"Company": {"prefix": "abc_"}, "Shop": {"prefix": "def_"}},
        ),
        "r_b": DerivationSpec(
            input=["shared_raw"], foo="affix_gated_key", params={"prefix": "abc_"}
        ),  # one member: no key
    },
)
# twenty members, nothing varying:  SharedDerivation(spec=..., members=[...20 names...])
# a different column or function per member: write the {member: spec} dict
local_key = LocalKeySpec(
    sources={
        "r_view": {
            "Company": LocalKeySource(field="firm_id", tag="firm"),
            "Shop": LocalKeySource(field="shop_id", tag="shop"),
        },
        "r_b": LocalKeySource(field="org_id", tag="b"),
    }
)
```

**The gate is derived, never written.** The merge has already rewritten the router's `type_map` values to the canonical name, so the union cannot say which key produced which member — but the pre-merge *sides* can, and `merge_manifests` hands them to the alignment. For each `(resource, member)` the lowering reads how the side produces it: a plain `vertex` step needs no gate (the level *is* the member); a router yields a `when` guard on its discriminator, exact match, listing the keys that mapped to the member:

```yaml
- transform:
    when: {field: kind, in: [shop]}
    call: {foo: affix_gated_key, input: [secondary_key], output: [match_key], params: {prefix: def_}}
```

A router with no entry for the member — no `type_map` at all, or a table that does not name it — routes the raw discriminator value as the class name, so the guard is the member's **own name**: that is the value which reaches it. When a level must be chosen, a step naming the class explicitly outranks such pass-through; a resource whose routers pass through at several levels picks one with `at`. Renames keep a pass-through router routing: a canonical map or a merge that renames a class writes `{old: new}` into every router's `type_map` on that side, so a raw value that used to name the class still lands on it.

A guarded step that does not fire **writes nothing**, so each member's step is the single writer of the attribute for its own documents and nothing clobbers — no scratch fields, no coalesce. Documents of other members never run it: a `person` row through the same router is not "derived and dropped", it is never derived. This is also why the gate cannot be a function returning `None`: behind a router a later `None` overwrites an earlier real value.

Rules the validator adds for member keys: the resource must produce the member on its side (the error lists what it does produce); the member must belong to the aligned cluster on that side; all members a resource keys must resolve to one pipeline level; and member-keyed sources cannot be lowered without the sides (call through `merge_manifests`, or pass `sides=` to `alignment_to_ops`). A resource that routes several members onto the class but derives with a single un-keyed spec is warned about — right when the members share a column and a marker, wrong otherwise — and so is a member dict that covers only some of the members the resource produces. `LocalKeySource.gate` / `gate_prefix` remain for an alignment applied outside a merge, where no side exists to derive the gate from; a member-keyed source may not set them.

### Routed sources

When the aligned class is produced by a `vertex_router` — one heterogeneous stream whose branches the equivalence collapses — two more things follow, and the router is never split to accommodate them.

**Derivations land at the producing level.** `alignment_to_ops` resolves it and sets `AddResourceTransformsOp.at`. Placement is not cosmetic: an actor reads its transform buffer at its own `LocationIndex` with no ancestor fallback, a `descend` subtree runs *before* its own level's transforms, and a transform whose declared inputs are missing skips silently by default — so a derivation appended at the root of a nested pipeline derives nothing, quietly. A resource producing the class at several levels raises unless `IdentityAlignment.at` picks one, and an `at` that resolves to a level producing nothing raises too. For member-keyed sources the level is the one producing the member on its side.

**Delivery goes through the merged observation.** A router builds its child `VertexActor` at `lindex.extend((role, 0))`, where the transform buffer is empty, so derived attributes arrive by passthrough or `from` — subject to `keep_fields` and `extraction_scope`. The alignment emits `EnsureExtractedFieldsOp` when the producing router restricts either. And because that observation reaches *whichever* class the router picks, every derivation behind a router is guarded — member-keyed ones per member, an unkeyed one on the discriminator values that route onto the class (`type_map` keys, or its own name for pass-through) — so a sibling class declaring the same attribute name (`name` on every class of an all-classes router) is never handed the derived value. The refusal for such a sibling remains only where no guard can be derived: a plain `vertex` step producing the class beside the router, or routers reading different discriminators at one level.

Worked end-to-end in [Example 19](../../examples/example-19.md), and for a routed source with member-keyed derivations in [Example 21](../../examples/example-21.md).

## API

```python
from graflo.architecture.evolution import (
    AddInverseEdgesOp,
    DeclareEdgeInversesOp,
    MergeManifestsOp,
    EdgeSelector,
    MergeEdgesOp,
    MergeVerticesOp,
    ProjectManifestOp,
    RenameRelationsOp,
    RemoveVerticesOp,
    SanitizeOp,
    apply_evolution,
    apply_sanitize,
    merge_manifests,
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
        DeclareEdgeInversesOp(inverses={"employed_by": "employs"}),
        AddInverseEdgesOp(relations=["employed_by"]),
    ],
    bump_version=True,  # default: increment schema metadata MINOR (see bump_semver_minor)
)

assert manifest_hash(a) != manifest_hash(b)

# Or sanitize an existing GraphManifest (same op `Sanitizer` uses internally):
apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))
```

`CanonicalizeOp` applies a whole vocabulary map — classes, per-class attributes, relations — in one step; see [Canonical maps](#canonical-maps).

- **`bump_version`**: when `True` or `"minor"` (default), increments the numeric `MAJOR.MINOR.PATCH` prefix of `schema.metadata.version` if present (prerelease suffix preserved). Pass `bump_version=False` to leave the version string unchanged.
- **Imports**: `graflo.architecture.evolution` re-exports the ops and apply helpers; lower-level functions such as `apply_remove_vertices`, `apply_merge_vertices`, `apply_rename_relations`, `apply_add_inverse_edges`, `apply_rename_vertex_properties`, and `apply_sanitize` mutate a manifest in place (used mainly internally and by `Sanitizer`). Cross-manifest merge uses `merge_manifests` (not unary `apply_evolution`).

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

### 5) Inverse and symmetric relations

Two relation names often read one fact from its two endpoints: `person --employed_by--> institution` and `institution --employs--> person`. Some relations read the same both ways: `person --knows--> person`. Declarations are made over **relation names**:

| Term | Where | What it is |
|------|-------|------------|
| **Inverse pair** | `schema.graph.edge_config.inverses` | The logical fact that `employs` and `employed_by` are inverses. Unordered; creates nothing. |
| **Symmetric relation** | `schema.graph.edge_config.symmetric` | The logical fact that `knows` is its own inverse. Creates nothing; its edges must be `directed: false`, which is its realization (`UNDIRECTED EDGE` on TigerGraph; elsewhere see [Core components — Edge](../architecture/core_components.md#directed-undirected-and-bidirectional-edges)). |

A declaration stores nothing, and is usually all you need. Reads honour it: the inverse name resolves to the forward edge followed from its target (`graph_neighbors(edge_types=["employs"])`), schema cards and the relation vocabulary list it, and on most backends a reverse read costs nothing — so storing the inverse would only duplicate edges.

**Storing** the reverse reading is a separate, optional step — a *realization* — and a pair has at most one:

| Realization | What is stored | How `employs` is read | When |
|-------------|----------------|-----------------------|------|
| **native** | The database maintains the reverse type: `db_profile.native_inverses: [employed_by]` → `WITH REVERSE_EDGE="employs"`. One load path. | An outgoing query on the reverse type. | TigerGraph, where reverse reachability is fixed when the edge type is created. Keyed by relation, because the reverse type belongs to the edge type, which spans every `(source, target)` pair of its relation. |
| **materialized** | `(institution, person, employs)` is a declared edge, fed at ingestion. | An ordinary edge. | A backend that cannot read backwards and cannot maintain the pair, or a consumer that needs the inverse to physically exist. Portable to every backend. |

Native **and** materialized would store the same fact twice, so the schema refuses the combination when it loads.

```yaml
schema:
  graph:
    edge_config:
      edges:
        - {source: person, target: institution, relation: employed_by}
        - {source: person, target: person, relation: knows, directed: false}
      inverses:
        - {relation: employed_by, inverse: employs}   # declared; nothing stored
      symmetric: [knows]
  db_profile:                     # only for the native realization
    db_flavor: tigergraph
    native_inverses: [employed_by]
```

**Feeding a materialized inverse.** An edge step — or one entry of its `links` — that sets `emit_inverse: true` also writes the declared inverse of every edge it writes, with the same properties. The mirror is taken at assembly, *after* the relation is resolved, so it does not matter how the step names its relation: fixed, `relation_field` (with fixed or routed endpoints), `relation_map`, or `relation_from_key`. Only a materialized inverse is written: a row whose resolved relation has no declared inverse edge is written once.

```yaml
edge_config:
  edges:
    - {source: person, target: institution, relation: employed_by}
    - {source: institution, target: person, relation: employs}   # materialized
  inverses:
    - {relation: employed_by, inverse: employs}
# ...
pipeline:
  - edge:
      source_role: source
      target_role: target
      relation_field: relation_type
      relation_map: {EMPLOYED_BY: employed_by, FUNDS: funds}
      emit_inverse: true    # EMPLOYED_BY rows also write employs; FUNDS rows do not
```

A source that already reports both readings needs no flag: a step of its own that writes `employs` is a perfectly good way to feed the inverse. What matters is that **every** resource writing `employed_by` feeds `employs` somehow — otherwise the two relations disagree. That is what the audit checks.

**What the schema checks when it loads**

| Rule | Refused when |
|------|--------------|
| One inverse per relation | Pairs and symmetric names give a relation two inverses — a chain `a-b`, `b-c`, or a relation both paired and symmetric. A pair restated in either order is the same statement and is kept once; pairs are stored with their names sorted, so the order they are written in does not change the content hash. |
| No self-pair | `relation == inverse` in a pair; declare the relation in `symmetric` instead. |
| No dangling declaration | A pair or symmetric name names no edge (allowed while a relation-less template edge admits relations named at ingest time). |
| One direction per relation | Edges of one relation disagree on `directed`. `directed: false` and `symmetric` are one fact at two granularities, so a relation is undirected everywhere or nowhere. |
| Pairs are directed | A paired relation is on an undirected edge. |
| Symmetric is undirected | A symmetric relation is on a directed edge. |
| Native needs a pair | A relation in `native_inverses` has no declared pair, or is symmetric. |
| One realization | A relation is native and declared edges carry its inverse name, or both sides of one pair are native. |
| One edge type | `relation_name` overrides store a native relation under several physical names. |
| TigerGraph only | `native_inverses` on any other `db_flavor`. |
| One type namespace | The inverse name equals a vertex type (TigerGraph type names are global). Physical `relation_name` overrides are checked again when the DDL is built. |
| A flag that can write | A step naming exactly one edge sets `emit_inverse`, and its relation has no declared pair, is symmetric, or has no declared inverse edge. A step whose relation comes from the data is checked per document instead. |

#### Seeing what you have: `audit_inverses`

```python
from graflo.architecture.profile import audit_inverses

report = audit_inverses(manifest)
for line in report.to_lines():
    print(line)
```

```text
employed_by <-> employs: materialized (2/2 mirrored)
    fed in hr: step
    fed in registry: none
funded_by <-> funds: declared

repairable (1):
  - [unfed_inverse] resource 'registry' writes 'employed_by' but feeds nothing into its materialized inverse 'employs'; the two relations will disagree
      at registry:2
```

For each pair the report gives its state — `declared` when nothing realizes it, `native` or `materialized` when something does, `partial` or `conflicting` when something is wrong — how every resource that writes the forward relation feeds a materialized inverse (`emit_inverse`, a `step` of its own, edge `inference`, or `none`), and — for a pair the database does not maintain — whether it could, and if not which rule says so. Findings come in three severities, and the split is the point:

| Severity | Meaning | Examples |
|----------|---------|----------|
| `repairable` | One place states a fact and another omits it. Propagating it cannot change meaning. | `unfed_inverse` (above — typical of a manifest assembled from several sources, where only one reported both readings), `partial_endpoint_pairs` (the inverse edge exists for some endpoint pairs only), `property_drift` (one mirror declares a property the other lacks), `undirected_not_symmetric`, `idle_emit_inverse` |
| `conflict` | Two places disagree, and only the author knows which is right. | `same_side_pair` (`(S, T, a)` next to `(S, T, b)` — one is reversed), `identity_drift`, `property_type_drift`, `orphan_inverse_step` (a step writing an inverse that labels no edge), and one `native_*` finding per native-inverse rule broken |
| `note` | Nothing is wrong. | `reverse_unreadable` (a pair that is only declared, on a backend that cannot read an edge from its target), `double_fed` |

A schema runs its cross-block checks while it is validated, so a manifest that breaks them cannot be loaded the usual way; `manifest_for_audit(config)` builds it block by block, and the audit and the repair planner both work on the result. The same audit runs as a conformance profile — `graflo check --profile inverses` — where conflicts fail and omissions warn. `audit_inverses(after).introduced_since(audit_inverses(before))` is what a change did to the inverses, whatever made the change.

#### Doing something about it: the planners

Five pure planners emit primitive ops and apply nothing, in the manner of `plan_lift`. The op list is the reviewable artifact; `apply_evolution` applies it, `invert_ops` undoes it, and a stored revision records exactly those primitives, so a plan replays without the planner. Each plan is checked against a copy of the manifest before it is returned, so an op that would be refused is reported as skipped — with the reason — instead of being handed to you to trip over.

```python
from graflo.architecture.evolution import (
    apply_evolution,
    plan_declare_symmetric,
    plan_realize_inverses,
    plan_repair_inverses,
    plan_switch_realization,
    plan_withdraw_realization,
)

# Have the database maintain every eligible pair; get a reason for each of the rest.
plan = plan_realize_inverses(manifest, strategy="native")
plan.ops        # [SetNativeInversesOp(relations=["employed_by", "funds"])]
plan.skipped    # [Skipped(relation="advises", code="collides_with_vertex", reason=...)]
manifest = apply_evolution(manifest, plan.ops)
```

| Planner | Emits | Notes |
|---------|-------|-------|
| `plan_realize_inverses(manifest, strategy=, relations=)` | `set_native_inverses` and/or `add_inverse_edges` | `native`, `materialized`, or `auto` — which decides from what a reverse read costs on the target backend: native on TigerGraph, materialized where direction is the storage key, and otherwise *nothing*, with the pair left as declared and the reason stated. The native side is the relation that has edges while its inverse has none. A pair already realized the other way is skipped, never realized twice. |
| `plan_repair_inverses(manifest)` | whatever propagates each `repairable` finding | A repair is kept only if, applied to a working copy, its finding is gone and no new one has appeared. Conflicts are never touched; they are in `plan.remaining`. Works on a manifest that does not load. |
| `plan_switch_realization(manifest, relations, to=)` | withdraw, then add | The relation named is the side that stays stored. Eligibility for `native` is checked *as the schema will be after the withdrawal*, before anything is planned, so a pair is never left withdrawn and unrealized. |
| `plan_withdraw_realization(manifest, relations)` | `set_native_inverses` (off) or `remove_edges` | Stops storing the inverse and keeps the declaration: the reverse of realizing. Removing a materialized inverse's edges also clears the `emit_inverse` flags that fed them. The relation named is the side that stays stored. |
| `plan_declare_symmetric(manifest, relations)` | `set_edge_directed`, then `declare_edge_inverses` | The schema refuses either without the other, so they are two ops in a fixed order — and two ops rather than one so that each can be undone exactly. |

From the shell, with `graflo lift`'s conventions (`-o`, `--emit-ops`, `--dry-run`):

```bash
graflo inverses audit manifest.yaml                     # exits 1 on conflicts
graflo inverses repair merged.yaml -o fixed.yaml --emit-ops fix.ops.yaml
graflo inverses realize manifest.yaml --strategy native --dry-run
graflo inverses switch manifest.yaml --to native -r employed_by -o out.yaml
graflo inverses withdraw manifest.yaml -r employed_by -o out.yaml   # keep the declaration, store nothing
```

#### The primitives

Every declaration and realization is authored by an op, and `diff_manifests` emits them in an order whose refusals cannot fire.

| Op | Plane | What it does |
|----|-------|--------------|
| `declare_edge_inverses` / `retract_edge_inverses` | logical | Record or withdraw pairs and symmetric relations. Retraction is refused while a native inverse realizes the pair, or while a step naming exactly one edge of it still sets `emit_inverse`. |
| `set_edge_directed` | logical | The realization of a symmetric relation. |
| `set_native_inverses` | physical | Add to or remove from `db_profile.native_inverses`. |
| `add_inverse_edges` | logical + physical + ingestion | Materialize: for each **directed** edge of a selected relation, adds the inverse edge with swapped endpoints — copying properties and identities but not `semantics` or `description`, which describe the forward reading — copies the physical spec without `relation_name` (so the two relations never share a storage type), and sets `emit_inverse` on the edge steps that write the forward relation. A resource that already writes the inverse with a step of its own is left alone, as is an inverse edge that already exists. `infer_edge_only` / `infer_edge_except` / `extra_weights` selectors covering a forward edge gain a twin covering its mirror. |
| `set_inverse_emission` | ingestion | Set or clear `emit_inverse` on steps addressed by position: `steps: {resource: [{at: [...], step: n, link: k}]}`, where `at` descends through nested pipelines. This is what lets the differ reproduce a materialization, and what a repair emits. |
| `remove_edges` | cascade | Removing an inverse edge clears the flags that fed it, so withdrawing a materialization is `remove_edges` on the inverse edges and its undo is exact. |

```python
from graflo.architecture.evolution import (
    AddInverseEdgesOp,
    DeclareEdgeInversesOp,
    apply_evolution,
)

bidirectional = apply_evolution(
    manifest,
    [
        DeclareEdgeInversesOp(inverses={"works_at": "employs"}),  # either order
        AddInverseEdgesOp(relations=["works_at"]),  # omit to realize every pair
    ],
    bump_version=False,
)
```

Materialized inverse edges are ordinary edges afterwards: later ops on the forward edge do not reach them, which is exactly the drift the audit reports.

**Choosing** (see also [Core components — Edge](../architecture/core_components.md#directed-undirected-and-bidirectional-edges)):

| Goal | Approach |
|------|----------|
| A name for the reverse reading, nothing stored | Declare the pair. Done. (`plan_withdraw_realization` gets you back here.) |
| TigerGraph, reverse reads needed | Declared pair + `set_native_inverses` (`plan_realize_inverses(strategy="native")`) |
| The inverse must physically exist, on any backend | Declared pair + `add_inverse_edges` (`strategy="materialized"`) |
| Truly symmetric (friends, co-authors) | `plan_declare_symmetric` → `directed: false` + `symmetric: [R]` → `UNDIRECTED EDGE` on TigerGraph |
| Not sure | `plan_realize_inverses(manifest)` — `auto` decides from the target backend and says why |

#### Why it is built this way

- **Declaring and storing are separate.** A pair is a statement about names; whether the reverse reading is stored is a cost decision that depends on the backend. Most backends read an edge from its target for free, so by default nothing is stored and the declaration earns its place on the read side instead. A declared pair with no realization is not a third kind of realization; it is simply the declaration, working.
- **Materialization is a flag on the step, not generated steps.** A generated reversed step has to be restricted to exactly the relations it mirrors, and for a relation read from a field with fixed endpoints, taken from a document key, or read by a link, nothing can express that restriction — so such steps used to be skipped, leaving an inverse edge nobody wrote. Mirroring after the relation is resolved has no such cases, keeps one step one step, and makes the change a flag flip that the differ can reproduce and an undo can reverse exactly.
- **The flag is per step, so coverage can be partial — and that is detected, not prevented.** A source that reports both readings should keep its own inverse step; a source that reports one should mirror. A per-relation switch could not express that. The price is that one resource can be left out, which is precisely what `unfed_inverse` reports and `repair` propagates.
- **Aggregation is done by planners that emit primitives, never by a macro op.** A stored revision records what was done, not what was asked for, so it replays without the planner and each step inverts on its own. It is also why making a relation symmetric is two ops: folding `set_edge_directed` into `declare_edge_inverses` would make the declaration impossible to undo exactly.
- **Repair propagates omissions and never settles contradictions.** When one place is silent and another speaks, the manifest has one opinion and propagating it is safe. When two places disagree it has two, and only the author knows which is wrong.

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

#### Slicing by neighbourhood (`depth`)

Enumerating every vertex type you want means reading the schema first and transcribing the answer. `depth` turns `keep_vertices` into *seeds* and lets the op derive the rest:

```python
slice = apply_evolution(
    manifest,
    [ProjectManifestOp(keep_vertices=["person"], depth=1)],
    bump_version=False,
)
```

One rule covers every combination. Let `E` be `keep_edges` when given and every declared edge otherwise. The survivors are the vertex types within `depth` hops of a seed, walking `E` under `direction`, and then `E` restricted to surviving endpoints.

| | Behaviour |
|---|---|
| `depth: 0` (default) | `keep_vertices` is the literal list, exactly as before. |
| Edges among neighbours | **Kept.** The result is the induced subgraph on the hop ball, not a breadth-first tree — if two of `person`'s neighbours are linked to each other, that edge survives even though no walk needed it. |
| `keep_edges` + `depth` | `keep_edges` **bounds the walk**: traversal follows only those triples. "Walk out N hops, but only along these edges." |
| Isolated seed | Still dropped. `induced_prune` is unaffected by `depth`. |
| `direction` | `any` by default — `out` follows only edges where the frontier type is the source, `in` only where it is the target. An edge declared `directed: false` is followed both ways regardless. |
| `depth > 0` with no `keep_vertices` | Rejected: with no seeds the selection already means "every vertex type", so there is nothing to expand. |

`Edge.by` — the third vertex type on an `INDIRECT` edge — is not part of schema adjacency, so a walk never pulls it in. That is the same blind spot the flat selection has.

### Choosing `RenameRelationsOp` vs `MergeEdgesOp`

- Use `RenameRelationsOp` when there is a one-to-one label replacement.
- Use `MergeEdgesOp` when multiple relation labels should collapse into one canonical relation.
- Use `AddInverseEdgesOp` when forward and inverse relations should coexist as explicit edges with different labels (not a rename of the same edge kind); `SetNativeInversesOp` when TigerGraph should maintain the inverse instead.
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

## Further reading

- Bonifati, Furniss, Green, Harmer, Oshurko, Voigt — *Schema Validation and Evolution for Graph
  Databases*, ER 2019. Property-graph schema evolution expressed as graph rewriting; the closest
  prior operation set for property-graph schemas.
- Hausler, Klettke, Störl — *A language for graph database evolution and its implementation in
  Neo4j*, ER Forum 2023. An evolution language bound to one backend; the op vocabulary here is
  backend-independent and lowered per target by the physical plane.
- Bonifati — *Versatile Property Graph Transformations*, PVLDB 18(12), 2025. Declarative
  graph-to-graph transformations; the comparator for projection rather than for evolution.
- Bernstein — *Applying Model Management to Classical Meta Data Problems*, CIDR 2003. The
  operator vocabulary — Match, Merge, Diff, Merge, ModelGen — that `diff_manifests`,
  `merge_three_way`, `merge_manifests` and `resolve_db_aware()` instantiate for manifests.
