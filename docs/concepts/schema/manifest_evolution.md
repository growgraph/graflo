# Manifest evolution

GraFlo provides **contract-level** operations that transform a validated `GraphManifest` into a new manifest: logical vertices and edges, ingestion resources, optional bindings wiring, and the database profile are updated together. This is **not** an in-database migration of existing graph data; the intended workflow is to publish the new manifest and **reingest** from sources.

## Identity and validation

- **Stable hash**: use `manifest_hash` from `graflo.migrate.io` (see [`graflo.migrate.io`](../../reference/migrate/io.md)) to compare the composed `schema`, `ingestion_model`, and `bindings` blocks before and after an evolution.
- **Validation**: `apply_evolution` in `graflo.architecture.evolution` returns a deep copy and runs `GraphManifest.finish_init()` by default so the same cross-block checks apply as when loading YAML. API reference: [`graflo.architecture.contract.manifest`](../../reference/architecture/contract/manifest.md).

## Operations

| Operation | Summary |
|-----------|---------|
| **Remove vertices** | Drops named vertex types, removes incident edges, prunes ingestion resources that reference removed types (including `vertex_router` `type_map` / `vertex_from_map` via structured pipeline scan), trims `merge_collections`, filters `resource_connector` rows, and updates `db_profile`. Fails if ingestion would be left with no resources. |
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
| **Add inverse edges** | `inverses: {R: R_inv}` (injective; no relation is its own inverse). For each **directed** forward relation `R -> R_inv`, appends inverse schema edges and mirrors ingestion (`pipeline` EdgeActor steps including dynamic endpoints, `relation_field`, redefined `relation_map`, nested `descend`), `infer_edge_only` / `infer_edge_except`, `extra_weights`, and `db_profile`. Skips `directed: false`, TigerGraph `edge_specs[*].reverse_edge`, and existing inverse triples. |
| **Project manifest** | Keeps a logical subgraph by vertex names and/or edge triples `(source, target, relation)`. Prunes isolated vertex types from `keep_vertices` when they have no surviving edges (`connectivity: induced_prune`). Cascades to schema, `db_profile`, ingestion (pipeline steps, infer selectors, `extra_weights`), and bindings. Optional `keep_resources` filters ingestion resources. Inverse edges are not auto-kept. Fails if ingestion would be left empty. |
| **Replace identity** | `replacements: {vertex: {to, retire, ...}}`. Per-vertex identity policy swap covering both field-set and **mode** changes (`natural` / `hash` / `assigned` / `blank`). `retire` decides what becomes of the old field-set — `demote` (default) turns it into a secondary identity, `keep` leaves it as plain properties, `drop` removes it. `endpoints` decides whether edge steps follow the new identity (`follow_new`, default) or stay pinned to the demoted one (`pin_to_retired`). Drops `db_profile` indexes that encoded the retired identity. See [Replacing a vertex identity](#replacing-a-vertex-identity). |
| **Add / remove secondary identities** | Declares or withdraws alternate lookup keys on existing vertices. Each field-set's non-unique index is *derived* by `Schema.finish_init`, so adding one needs no index authoring; removing one drops the derived index explicitly. Removal is rejected while an edge step still selects the field-set. |
| **Replace edge identities** | Replaces `Edge.identities` (uniqueness keys) per `(source, target, relation)`. No retire policy — edge identities have no lookup plane. Non-endpoint tokens are merged into edge `properties` by `Edge.finish_init`. |
| **Add vertices / add edges** | Introduces new logical vertex types and edge relations unarily — the counterpart to what `ComposeManifestsOp` could previously only do binarily. Rejects existing names/triples and unknown endpoints. |
| **Retarget edges** | Changes which vertex types an edge connects, preserving its properties, `identities`, `directed` flag, and `db_profile` physical spec — all of which a remove-plus-add would lose. Rewrites the `EdgeId` in `edge_config`, `edge_specs`, and pipeline edge steps, keyed on the full triple so a different relation between the same types is untouched. |
| **Change field types** | Sets `Field.type` / `item_type` on vertex or edge properties. Validated against the profile's `db_flavor` via `graflo.db.field_type_support`, so an unsupported LIST target fails at op time rather than at define time. Refuses to make an identity field a LIST. |
| **Add / remove vertex & edge indexes** | Authors `db_profile.vertex_indexes` and `edge_specs[].indexes` directly. Indexes derived from `secondary_identities` cannot be removed this way — they would be re-registered by the next `finish_init`, so the op points at **remove secondary identities** instead. |
| **Set edge directed** | Sets `Edge.directed` on selected triples. Load-bearing for replay: `directed` decides what **add inverse edges** may duplicate. |
| **Sanitize** | Target-`DBType` policy: reserved-word-safe names on `DatabaseProfile`, reserved vertex field renames, and (for TigerGraph) consistent identity tuples per edge relation. This is the same work **`graflo.hq.sanitizer.Sanitizer`** applies by building a single **`SanitizeOp`**. |
| **Ensure extracted fields** | Widens a producing step's projection so named fields survive extraction (`keep_fields` gains them; under `extraction_scope: mapped_only` so does `vertex_from_map[<class>]`, seeded from the router-level `from`). Only `vertex_router` steps need it — a plain `vertex` step reads the transform buffer directly, bypassing both knobs. A no-op on an unrestricted step. Requires `ingestion_model`. |
| **Add resource transforms** | Appends transform steps to a named level of named resources' pipelines (`at`, as `descend` step indices; root by default — actor type-priority ordering runs them before vertex extraction at that level) and optionally registers named transforms (loud on same-name/different-body, mirroring compose). The only op whose primary effect is ingestion; requires `ingestion_model` and raises otherwise. Steps may reference the registry via `call.use` or carry a fully inline `call` (collision-free). Irreversible. |
| **Add / remove resources** | `add_resources` takes full `ResourceConfig` definitions (creating `ingestion_model` if absent; an existing name is rejected); `remove_resources` takes names and prunes the `resource_connector` entries that wired them. Inverses of each other — a removal that pruned a binding has none. The differ emits both. |
| **Set vertex / edge / field semantics** | Ground an existing element in an external vocabulary: `set_vertex_semantics` (`{vertex: Semantics \| null}`), `set_edge_semantics` (edge triples + one `Semantics`), `set_field_semantics` (per-target `FieldSemantics`, the only model carrying `unit`; a target is a vertex property `{vertex, field}` or an edge property `{source, target, relation, field}`). `null` clears, which is what makes each invertible. Never consulted at execution time. The differ emits all three. |
| **Canonicalize** | Applies a whole **vocabulary map** — classes, per-class attributes (keyed by the *source* class), and relations — in one step over the original schema. Attribute renames run first, then classes and relations simultaneously, so a chain (`{X: Z, Z: Q}`) and a swap resolve without an intermediate state and no op order can leak into the result. The fibers of the map are exactly the groups that merge, so a group of more than one name needs `allow_merges`; a target that already exists and does not move must be declared a member of its own group with a self entry (`Company: Company`), or the op refuses rather than merging into it silently. Reversible when it only renames. This is what a `CanonicalMap` lowers to, and the per-side step of compose. See [Canonical maps](#canonical-maps). |
| **Compose manifests** | Binary union of two full `GraphManifest`s (schema **and** resources/bindings) via `ComposeManifestsOp` + `compose_manifests(left, right, op)`. Consumes **explicit** equivalence maps only (no semantic inference): n-ary vertex clusters (`vertex_equivalences`: `left` / `right` each name one or more classes collapsing onto one `into`), property alignment, optional composed `identity`, optional `identity_alignments`, relation equivalences (`relation_equivalences`), resource renames, and `canonical_maps` (scoped `left` / `right` / `both`), which name the composed classes so `into` may be omitted. A name both sides carry that no equivalence covers is what `name_conflict` decides — refused with the declarations to add (`error`), unioned by name through a synthesized equivalence (`union_right`), or kept apart (`prefix_right`). Distinct from unary `MergeVerticesOp`. Rejected by unary `apply_evolution`. |

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
        resource_renames={},  # right resource name -> composed name
        name_conflict="error",  # or "prefix_right" / "union_right"
    ),
)
```

`left` / `right` also take a **list**, which is how an n-ary cluster is
spelled — one declaration naming every member that collapses onto `into`:

```python
ComposeManifestsOp(
    vertex_equivalences=[
        VertexEquivalence(
            left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
        )
    ],
    allow_merges=True,          # >1 member on a side is a stated intent
    allow_self_relations=False,  # forwarded to the per-side CanonicalizeOp
    allow_observation_fusion=False,
)
```

Name-disjoint sides need no equivalence at all: with `vertex_equivalences` / `relation_equivalences` empty they compose into a **disjoint union**, both resource sets and bindings retained. A name *both* sides carry is not a disjoint union and is never silently treated as one — it is what `name_conflict` decides (see the [case table](#where-a-map-and-the-equivalences-can-stand)).

A `VertexEquivalence` declaration *is* one cluster. `into` is optional — see [Canonical maps](#canonical-maps) for how a composed name is found. `ClusterConflictError` (raised before any rename) covers the three ways declarations can contradict each other: a class **claimed by two** declarations, two declarations **sharing one composed name** (that collapse is one n-ary cluster and must be spelled as one), and a composed name that already names an **existing non-member class** on a side (which would silently merge into an unrelated type). Properties with the **same spelling** on both sides after alignment fuse for free; list a `PropertyEquivalence` only to rename, to map per member (`left={"Company": "company_key", "Shop": "shop_key"}`), or to flag identity.

That fusion is a **union**, and it compares more than a name. `type` and `item_type` travel together, so `LIST<STRING>` and `LIST<INT>` are a conflict rather than a shared `LIST`; descriptions from both sides are kept; grounding blocks union their `exact_match` and `synonyms`, and a disputed `iri` clears rather than electing one side's concept. Two disagreements refuse the compose instead of resolving it: two declared **types** for one property, and two declared **units** — a property that is `m/s` on one side and `km/h` on the other would hold numerically incomparable values once fused. Edge properties fold by exactly the same rule, since it is one kernel. Neither is widened automatically, because the composed type would be one neither author wrote; retype one side with `change_field_types` first. Every conflicting property is named in one error rather than one per run.

Compose refuses to guess the composed **identity** too: when members disagree on their identity field-set after alignment and nothing resolves it, `ComposeIdentityError` names each member's key. Resolve it with `identity` on the cluster (a natural key, an `IdentityFunnel`, or a `SideIdentity` shorthand lowered to one funnel), a `PropertyEquivalence(identity=True)` flag, or an `identity_alignments` entry. A declared `identity` demotes each member's retired key to a lookup-only secondary identity unless the cluster sets `retire="keep"`.

### What compose does, in order

`compose_manifests` is one pass with a fixed order. Two steps are where they are for a reason, noted below.

1. **Fold the declared maps** per side — `both` under `left`, `both` under `right` (`merge_canonical_maps`), refusing two maps that disagree on a source or move each other's targets.
2. **Resolve the clusters** against those maps: members in each manifest's own spelling, one composed name each, and one composite relabel per side.
3. **Synthesize** a cluster for every name both sides still carry, as `name_conflict` directs, then re-resolve so the synthesized ones are ordinary clusters from here on.
4. **Capture each member's identity key and property names** — *before* the relabel, because once the members are collapsed onto one name the schema no longer records which key came from which member, and the composed identity is decided by comparing exactly those.
5. **Rename the right side's resources** per `resource_renames`, then per the collision policy.
6. **Snapshot both sides as they now stand** — also *before* the relabel: the relabel rewrites a `vertex_router`'s `type_map` values to the composed name, after which nothing says which router key produced which member. Identity alignment needs that, so it reads these snapshots.
7. **Apply the composite relabel** to each side, one `CanonicalizeOp` per side.
8. **Prefix the right side's remaining vertex and relation collisions** (`prefix_right` only — `error` and `union_right` settled theirs in step 3).
9. **Union schema, ingestion and bindings by name**, merging each name both sides carry, deciding the composed identity, and demoting retired keys to secondary identities.
10. **Assert no canonical split** on the result: no two composed types may denote one concept under different spellings.
11. **Bump the version, apply `identity_alignments`, `finish_init`.**

### Words for combining things

Five verbs recur, and they are not synonyms.

| verb | sense | where |
|---|---|---|
| **compose** | join two manifests of *unrelated lineage* by declared equivalence | `compose_manifests`, `ComposeManifestsOp` |
| **union** | assemble two collections **by name** — the outer step | `_union_schema`, `_union_ingestion`, `_union_bindings` |
| **merge** | combine the definitions **one name** has on both sides, refusing conflicts — the inner step | `merge_vertex_models`, `merge_edge_pair`, `merge_semantics` |
| **merge** (collapse) | send *several distinct* classes or relations to one name | `MergeVerticesOp`, `MergeEdgesOp`, `allow_merges` |
| **merge** (three-way) | reconcile two descendants of a **common ancestor** — a different operation entirely | `merge_three_way`, `MergeConflict` |
| **fuse** | two *records* becoming one node at ingestion | `allow_observation_fusion`, identity alignment |
| **collapse** | cluster members arriving at their composed name | `VertexEquivalence`, `CanonicalizeOp` |

Union and merge are not competing words: they are the two levels of one operation. The union walks the names; the merge is what it does at a name both sides carry.

Collapse and merge-at-a-name share one implementation — `merge_vertex_models` is called both by `MergeVerticesOp` and by the union — because at the field level they are the same work: union the properties, reconcile the identity, refuse a conflicting type or unit. What differs is the author's claim about the inputs, and `allow_merges` is where that claim is made: several *distinct* classes becoming one is a stated intent, while two views of one class needs no acknowledgement.

`fuse` is about records, not types. The one exception is the `name_conflict="union_right"` policy, which was spelled `fuse_right` before this distinction was drawn and still parses under that name.

Three-way merge is the outlier: it reconciles change sets, not schemas, and expects names to *agree*. See [Merge is not compose](versioning.md#merge-is-not-compose).

### Either side may carry no schema

A manifest needs only one block, so an overlay carrying just an `ingestion_model` and/or `bindings` — a new source wired onto an existing type vocabulary — is a valid compose input. The composed schema is the other side's, copied verbatim: physical profile, target namespace, secondary indexes and schema version all survive. With neither side carrying one, the composed manifest has no schema block and the version bump is a no-op.

Verbatim rather than "merged with an empty schema" on purpose: filling a missing side with an empty `Schema` would compose against a profile nobody wrote, and drop that side's namespace and version — a wrong answer with nothing raising.

When both sides *are* present the profile fold elects neither. `db_flavor` and `target_namespace` are single-valued and decide what DDL is emitted against which backend, so two **declared** values raise; a side that never declared one yields to the side that did. Declaration is read from what a side actually wrote, not from the value — `db_flavor` defaults to Arango, so a value-based fold could not tell a side that chose Arango from one that never spoke, and an undeclared left would silently retarget a right that named its backend. `default_property_values` union, refusing two defaults for one property. Vertex indexes union on their full definition: two entries over one field-set that disagree on `unique`, `type` or `sparse` raise rather than keeping the left's, since the next schema resolution collapses them on field-set alone and the survivor would depend on ordering.

### From the shell

```bash
graflo compose LEFT.yaml RIGHT.yaml --op OP.yaml -o OUT.yaml \
  [--canonical-map SIDE=PATH]... [--name-conflict error|prefix_right|union_right] \
  [--bump-version minor|none] [--strict-references] [--dry-run] [--check-profile NAME]
```

The verb applies the op and its canonical maps together: `--canonical-map SIDE=PATH` (`SIDE` one of `left`, `right`, `both`) is folded into the op's `canonical_maps`, so the same document may carry the maps itself. Omitting `--op` composes a disjoint union. `--name-conflict` overrides the op's policy: `error` refuses a name both sides carry, `union_right` unions by name, `prefix_right` keeps them apart under `r_` names. Exit `0` composed, `1` compose refused, `2` the command could not run.

A refusal names what to declare, and an *incomplete* one prints the declarations themselves — `ComposeIncompleteError` carries a completion, which the verb renders as YAML on stderr below the message, ready to paste into the op:

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

**On a compose op**, `ComposeManifestsOp.canonical_maps` names the composed classes. An equivalence cluster says *which* classes are one; the map says *what they are called*, so `into` is optional. The composed name of a cluster is `into` (translated through the map when the map maps it), else the canonical name the map gives a member, else the one spelling every member shares. A member may be spelled by its own name or by its canonical name. Maps are scoped: `left` / `right` apply to that manifest's own names; `both` to either side's names and to composed names — an entry that matches one side only applies there and is simply inapplicable on the other.

```python
from graflo.architecture.evolution import (
    CanonicalMap,
    ComposeManifestsOp,
    VertexEquivalence,
    compose_manifests,
)

op = ComposeManifestsOp(
    # {Firm} ~ {Org}: no `into` — the map names it Company
    vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")],
    canonical_maps={
        "left": CanonicalMap(
            vertices={"Firm": "Company"},
            properties={"Firm": {"firm_id": "company_id"}},
        )
    },
)
composed = compose_manifests(left, right, op)
```

### Previewing

`compose_manifests` raises at the first refusal — right for a function that
returns a manifest, but it means three bad declarations take three runs to
find. `preview_compose(left, right, op)` walks the same declarations and
reports **every** problem at once, as data:

```python
from graflo.architecture.evolution.preview import preview_compose

preview = preview_compose(left, right, op)
for finding in preview.blocking:
    print(finding.severity, finding.kind, finding.nodes, finding.message)
```

A `ComposePreview` carries the declaration graph — each side's classes and
attributes, the clusters over them, the canonical names the maps establish —
plus `findings` at three severities: `refusal` is the one compose raised,
`possible` is everything the preview found on its own, `note` an acknowledged
heuristic such as a satisfied entry. Every finding names the nodes it is
about, and an incomplete one carries the same `Completion` the exception
does. Pass `attempt=False` to describe the declarations without composing.

It is not a second implementation of the rules: each check calls the function
compose itself calls, one declaration or one map entry at a time, so a refusal
on one unit does not hide the next. That extends to the schema union — the
preview runs `merge_vertex_models` and `merge_edge_pair` per cluster, so a type
clash, a unit clash, two identity modes that exclude each other, a divergent
funnel, a secondary identity claimed twice and an edge whose two declarations
disagree on `type`/`by` are all found by the code that decides them.

The invariant the tests hold it to is that whatever compose refuses, the
preview has a finding of a matching kind for. It **fails closed**: a refusal
the preview cannot classify fails the suite rather than skipping the case, so a
new rule cannot be added without a finding kind to report it under. What makes
a refusal classifiable is a `check` phrase on the exception — every refusal in
the compose and union path carries one.

From the shell, `graflo compose --plot conflicts.svg --preview-json
conflicts.json` writes both — **including when compose refuses**, which is the
case they are for. `--dry-run` prints the findings table on its own.

### Checking a map before composing

A canonical map is authored against one schema long before it meets another, and
a map of hundreds of entries is not debugged through a compose. `graflo
canonical-check MAP --left manifest.yaml` classifies every entry against that
one manifest — no op, no other side — and names each one that matches nothing,
with a near-miss candidate where another spelling denotes the same concept. It
exits 1 when entries dangle and 2 when it could not run, so it gates in CI.

Give it both `--left` and `--right` and it runs the full preview instead. Give
it `--trim OUT.yaml` and it writes the map narrowed to what the manifest
declares, so pruning a map is a change with a diff rather than something
discovered at compose time. In Python the same two are
`dangling_entries(cm, manifest)` and `trim_canonical_map(cm, manifest)`.

### Vocabulary

| term | type | meaning |
|---|---|---|
| **declared map** | `CanonicalMap`, folded into `DeclaredMaps` | a map the author wrote — `op.canonical_maps[scope]` or a `canonical_maps=` pair handed to compose |
| **cluster** | `Cluster` (resolved from `ClusterSpec`) | one equivalence declaration, resolved: its members per side in the manifests' own spelling, and its **composed name** |
| **cluster map** | — | per side, every member onto its composed name (the composed name itself included, so the op merges into it rather than refusing an occupied target) |
| **composite map** | `SideMaps`, one `CanonicalizeOp` per side | the cluster map plus every declared entry that applies to a non-member: what compose applies to that side before the union by name. A relabel, not a vocabulary — two clusters may chain when one composed name is renamed away by another declaration |
| **fixed point** | — | a canonical target; no map and no cluster may move it |
| **opinion** | — | what the declared maps say a member's canonical name is: the target it maps to, or itself when it is a fixed point |
| **satisfied entry** | — | a declared entry whose source is absent and target present on a side: taken as already applied by the caller (a heuristic — it is logged) |
| **dangling entry** | — | a declared entry matching nothing on any side it could apply to: a typo, refused. Every one on a side is named by a single refusal, with a near-miss candidate where one exists; `allow_dangling_entries` drops them instead |
| **synthesized cluster** | `Cluster.synthesized` | a cluster compose declares itself under `name_conflict="union_right"` for a name both sides carry, or two spellings of one name |
| **completion** | `Completion`, on `ComposeIncompleteError` | the extension that would make an incomplete declaration consistent, as `VertexEquivalence` / `RelationEquivalence` documents |

Identity is **nominal**: a class is the same class across two manifests only by name or by declared equivalence. Nothing structural fingerprints it — content hashes address a manifest, not a class — and compose never infers a match.

### Where a map and the equivalences can stand

Renames compose, so canonicalizing a side on its own first and then declaring the cluster in canonical names is the same function as declaring it in raw names on an op that carries the map: `compose_manifests` accepts either. Canonicalizing the *union* afterwards is a different function in general (it can collapse two composed names) and is just a unary `CanonicalizeOp` on the result. `resolve_clusters` — what compose runs; `validate_and_complete_canonical_map` returns just its per-side relabels — folds the clusters and the maps into **one composite `CanonicalizeOp` per side**, applied before the schema/resource union.

One rule underlies every refusal: **the map and the equivalences must agree on where a name goes, and a canonical target is a fixed point neither may re-map.** Every case below is an instance of it.

**Per name.** For one name on one side, with `E` its cluster and `C` the declared entry that names it:

| case | outcome |
|---|---|
| neither | unchanged |
| `C` only, target free or self-declared | carried into the composite |
| `C` only, target an unmoving non-member without a self entry | refused by the op (occupied target) |
| `E` only | onto the composed name |
| `E` and `C` agree; `E` without `into` and `C` names a member; `into` itself in `dom(C)` | onto the composed name, which `C` supplies or translates |
| `E` names a member the side does not declare | `ValueError` naming the member — and, when another spelling on that side denotes the same concept, naming that too ("author the equivalence in the manifest's own spelling") |
| `E` with no `into`, no mapped member and no spelling its members share | refused as an **unnamed cluster**: give it `into`, or map a member |
| **contradiction** — `E` and `C` disagree; `E` moves a fixed point; a property equivalence renames a canonical attribute | `ComposeCanonicalConflictError`, naming both declarations |
| **ambiguity** — a canonical name denotes two members; maps disagree on translating `into` | `ComposeCanonicalConflictError` |
| **incomplete** — `C` sends a non-member onto a composed name | `ComposeIncompleteError`; the completion is the cluster extended with that member, whose identity and property maps then govern it |
| one-sided `both` entry | applied where it matches |
| `both` entry over a composed name | translation of `into`, not a dangling entry |
| dangling | refused — one refusal lists every dangling entry on that side, each with a near-miss candidate where another spelling denotes the same concept. It outranks an `incomplete` on the same side: a name that is not there at all is the more basic mistake |
| dangling, with `allow_dangling_entries` on the map or the op | dropped and logged, for a shared vocabulary deliberately broader than this manifest. Off by default — a misspelt class has the same shape, and dropping it silently renames less than the author asked |
| satisfied | no-op, logged |
| `properties` keyed by a composed or canonical class | refused — the attribute map is keyed by the source class |
| `properties` renaming a field the member does not declare, or onto a field it keeps | refused — a property rename cannot merge two fields; align them with `PropertyEquivalence` on both sides |
| chain or swap inside one map | refused at `CanonicalMap` construction |

**Across the two sides.** These are facts about a *pair* of names, decided after each side's composite map has been applied:

| case | outcome |
|---|---|
| a name both sides carry, no cluster | `error`: `ComposeIncompleteError`, whose completion declares the equivalence — naming the members in each side's **own** spelling, composed onto the shared name; `union_right`: a synthesized cluster; `prefix_right`: kept apart as `r_n` |
| two spellings of one name (`OrderLine` / `order_line`) | `error`: `ComposeNameConflictError`; `union_right`: a synthesized cluster under the left spelling; `prefix_right`: kept apart |
| a **resource** or **connector** name both sides carry | matched **exactly only** — these are addresses, not concepts, so two that key alike split nothing and `union_right` behaves as `error`. Resolve with `resource_renames` or `prefix_right` |
| two **property** names that key alike (`customer_email` / `customerEmail`) | never fused: a property name binds to a key in the source document, so the two are fed by different columns. Only exact spellings fuse |
| two declared maps chaining (`{Z: Q}` and `{X: Z}`) | refused by `merge_canonical_maps`, in either order |

A synthesized cluster is a real cluster: the union it produces goes through the same identity reconciliation as a declared one, so two same-named classes whose keys disagree raise `ComposeIdentityError` instead of composing to a key no record carries, and the right side's properties are unioned rather than dropped.

The cluster-shape checks (`ClusterConflictError`, wrapped by `validate_and_complete_canonical_map`) stay: a class claimed by two declarations, two declarations sharing one composed name, a composed name occupying an existing non-member class. `merge_canonical_maps(base, extension)` is the union two declared maps for one scope reconcile through — every source maps to one target, and a target of either map is a fixed point the other may not move. Compose deliberately re-checks nothing it already raises on (incompatible types, divergent funnels).

Self-relations and observation fusion are not merely warned about: compose forwards `allow_self_relations` / `allow_observation_fusion` from the op to the per-side `CanonicalizeOp`, so the merge guards fire unless the author acknowledges them. The fusion guard is judged per **accumulator slot**, which is what the runtime fuses on: a vertex step stores at its `role` sub-slot when it has one and at the bare level otherwise, and a router at its `role` (or `type_field`). Two members produced at one level by steps with distinct `role`s — the client/server or buyer/seller pattern, addressed from the edge by `source_role` / `target_role` — never share a slot, are not fusion, and need no flag. The flag is for two members produced by *bare* steps (or steps of one `role`) at one level, where a single document yields both and the merged observations fold into one node.

### Identity alignment

When the composed class should deduplicate entities across its sources, the identity question splits along a principle: **a primary identity is a property of the class**, so the class declares one identity over canonical attributes only — while *how* each source populates those attributes is resource knowledge, expressed as pipeline steps. `IdentityAlignment` states both halves declaratively; put it on `ComposeManifestsOp.identity_alignments` so `compose_manifests` applies it after the schema/resource union (each entry's `vertex` must be a declared cluster's composed name). Under the hood `alignment_to_ops` still emits only fundamentals:

```python
from graflo.architecture.evolution import (
    AlignmentAttribute, ComposeManifestsOp, DerivationSpec, IdentityAlignment,
    LocalKeySource, LocalKeySpec, VertexEquivalence,
    compose_manifests,
)

alignment = IdentityAlignment(
    vertex="Company",
    attributes=[AlignmentAttribute(name="match_key", sources={  # priority order
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
    vertex_equivalences=[
        VertexEquivalence(left="Company", right="Org", into="Company"),
    ],
    identity_alignments=[alignment],
)
union = compose_manifests(canonical_left, right, op, canonical_maps=[("left", cm)])
```

The emitted op sequence: `AddVertexPropertiesOp` (declare the canonical attributes), `AddResourceTransformsOp` (per-resource derivation steps, inline calls), `ReplaceIdentityOp` (a priority funnel — one branch per attribute in order, then the `local_key` fallback; `retire: keep`), and `AddSecondaryIdentitiesOp` (the retired side keys as lookup-only secondaries). Attribute order is funnel priority: a record keys by the highest-priority attribute it carries, so two records fuse when their strongest present attribute coincides — a match on a lower-priority attribute does not fuse records when one side also carries a stronger one. The `local_key` values are namespaced per resource (`a:f2` vs `b:o1`), so non-aligned records stay ingested without cross-source collisions. The tag is required so that opting out is a statement: `tag=None` keeps the raw value as the local key, no separator — the author's claim that the values are already unique across every source of the class (UUIDs, IRIs, ids the source itself prefixes), where a namespace would only be noise. It is stored as the empty tag `""`, the neutral element, which is what survives serialization.

Two idioms decide *which* records participate, and `DerivationSpec.foo` selects between them. `gated_normalized_key` gates on a **sibling** field — participation is decided by one column, the key material comes from another — and its `strip_prefix` is `str.removeprefix`, a best-effort cleanup that is a silent no-op when the prefix is absent. `affix_gated_key` puts the test on the **value itself**: it reads one field, and a marker on that value is the admission test, so `company_A42` is stripped and accepted while a bare `A42` yields `None`. The marker is an affix pair — `prefix` and `suffix`, each defaulting to `""`, which every string carries — so it can lead, trail, or bracket the key, both halves are required when both are named, and naming neither admits everything while stripping nothing. Both return `None` to decline, which is a fall-through rather than a drop — an empty value skips the funnel branch listing that attribute, and the record keys on its `local_key` instead, ingested but outside the cluster. Reach for the marker form when the key column is self-describing, and for the gated form when a different column decides.

Rules the validator enforces (`validate_alignment`, raising `AlignmentConflictError`): derivation inputs are **raw source-doc field names** — property renames rewrite `vertex.from` maps so documents keep their original keys, and transform inputs are never rewritten (pass `canonical_maps` to catch canonical names used by mistake); alignment targets must not collide with the class's current primary-identity fields; and every referenced resource must produce the aligned class at exactly one resolvable pipeline level.

### The member is the unit of derivation

A cluster names its **members** — the classes it collapses, per side: `VertexEquivalence(left=["Company", "Shop"], right=["Org", "Branch"], into="Company")`. Every record that becomes `Company` was produced *as one member* by *one resource*: by a `vertex: Shop` step, or by a `vertex_router` key whose value was `Shop`. Canonical attributes are therefore derived **per member**. `sources` is keyed by resource because derivation inputs are that resource's raw column names; a resource that produces one member needs nothing more, and a resource that produces several says how, with one of three shapes:

| `sources[resource]` | When | Lowers to |
|---|---|---|
| one `DerivationSpec` | the resource produces one member, or its members share the key column *and* the marker convention | one step writing the attribute |
| a **list** of specs | several members, each carrying its key in its **own column** — the other column is empty, which already selects | one scratch field per spec + a `coalesce_fields` step as the single writer |
| a **dict keyed by member class** | several members, and *which member a document is* must decide — they share a column, or each has its own marker | one guarded step per member, each the single writer for its own documents |
| a `SharedDerivation` | the dict above, when the call is the same for every member and only a parameter differs (or nothing does) | expands to the dict; lowers the same way |

A member is keyed by its own name on its side or by its canonical name — `Firm` or `Company` when a map renames one to the other — as in the equivalence itself. The list form needs no member names because column presence selects; the dict form is the general one, and `SharedDerivation` spells it once for the common case — one call, the members sharing it, and per member only what varies:

```python
AlignmentAttribute(name="match_key", sources={
    "r_view": SharedDerivation(                                  # the member decides
        spec=DerivationSpec(input=["secondary_key"], foo="affix_gated_key"),
        members={"Company": {"prefix": "abc_"}, "Shop": {"prefix": "def_"}},
    ),
    "r_b": DerivationSpec(input=["shared_raw"], foo="affix_gated_key",
                          params={"prefix": "abc_"}),                # one member: no key
})
# twenty members, nothing varying:  SharedDerivation(spec=..., members=[...20 names...])
# a different column or function per member: write the {member: spec} dict
local_key = LocalKeySpec(sources={
    "r_view": {"Company": LocalKeySource(field="firm_id", tag="firm"),
               "Shop":    LocalKeySource(field="shop_id", tag="shop")},
    "r_b":    LocalKeySource(field="org_id", tag="b"),
})
```

**The gate is derived, never written.** The merge has already rewritten the router's `type_map` values to the canonical name, so the union cannot say which key produced which member — but the pre-merge *sides* can, and `compose_manifests` hands them to the alignment. For each `(resource, member)` the lowering reads how the side produces it: a plain `vertex` step needs no gate (the level *is* the member); a router yields a `when` guard on its discriminator, exact match, listing the keys that mapped to the member:

```yaml
- transform:
    when: {field: kind, in: [shop]}
    call: {foo: affix_gated_key, input: [secondary_key], output: [match_key], params: {prefix: def_}}
```

A router with no entry for the member — no `type_map` at all, or a table that does not name it — routes the raw discriminator value as the class name, so the guard is the member's **own name**: that is the value which reaches it. When a level must be chosen, a step naming the class explicitly outranks such pass-through; a resource whose routers pass through at several levels picks one with `at`. Renames keep a pass-through router routing: a canonical map or a compose that renames a class writes `{old: new}` into every router's `type_map` on that side, so a raw value that used to name the class still lands on it.

A guarded step that does not fire **writes nothing**, so each member's step is the single writer of the attribute for its own documents and nothing clobbers — no scratch fields, no coalesce. Documents of other members never run it: a `person` row through the same router is not "derived and dropped", it is never derived. This is also why the gate cannot be a function returning `None`: behind a router a later `None` overwrites an earlier real value.

Rules the validator adds for member keys: the resource must produce the member on its side (the error lists what it does produce); the member must belong to the aligned cluster on that side; all members a resource keys must resolve to one pipeline level; and member-keyed sources cannot be lowered without the sides (call through `compose_manifests`, or pass `sides=` to `alignment_to_ops`). A resource that routes several members onto the class but derives with a single un-keyed spec is warned about — right when the members share a column and a marker, wrong otherwise — and so is a member dict that covers only some of the members the resource produces. `LocalKeySource.gate` / `gate_prefix` remain for an alignment applied outside a compose, where no side exists to derive the gate from; a member-keyed source may not set them.

### Routed sources

When the aligned class is produced by a `vertex_router` — one heterogeneous stream whose branches the equivalence collapses — two more things follow, and the router is never split to accommodate them.

**Derivations land at the producing level.** `alignment_to_ops` resolves it and sets `AddResourceTransformsOp.at`. Placement is not cosmetic: an actor reads its transform buffer at its own `LocationIndex` with no ancestor fallback, a `descend` subtree runs *before* its own level's transforms, and a transform whose declared inputs are missing skips silently by default — so a derivation appended at the root of a nested pipeline derives nothing, quietly. A resource producing the class at several levels raises unless `IdentityAlignment.at` picks one, and an `at` that resolves to a level producing nothing raises too. For member-keyed sources the level is the one producing the member on its side.

**Delivery goes through the merged observation.** A router builds its child `VertexActor` at `lindex.extend((role, 0))`, where the transform buffer is empty, so derived attributes arrive by passthrough or `from` — subject to `keep_fields` and `extraction_scope`. The alignment emits `EnsureExtractedFieldsOp` when the producing router restricts either. A sibling class routed at the same level that already declares one of the canonical attribute names raises: it would absorb the derived value.

Worked end-to-end in [Example 19](../../examples/example-19.md), and for a routed source with member-keyed derivations in [Example 21](../../examples/example-21.md).

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

`CanonicalizeOp` applies a whole vocabulary map — classes, per-class attributes, relations — in one step; see [Canonical maps](#canonical-maps).

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
  operator vocabulary — Match, Merge, Diff, Compose, ModelGen — that `diff_manifests`,
  `merge_three_way`, `compose_manifests` and `resolve_db_aware()` instantiate for manifests.
