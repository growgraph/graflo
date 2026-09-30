# Vertex identity

Two records describe the same machine, and GraFlo must write one vertex, not
two. This page explains how GraFlo decides that two records are the same
vertex, for anyone who writes the `schema` block of a manifest. It starts with
the common case, a natural key, and adds one mechanism for each problem a key
alone cannot solve. After reading it you can choose the identity declaration
for each vertex type.

The [identity](../glossary.md#identity) is declared once, on the vertex type.
When the graph is written, the database upserts on it: a record whose identity
matches a stored vertex updates that vertex instead of creating a second one.
On NebulaGraph the identity values form the vertex ID behind the tag,
`<tag>::<identity values>`, so vertices of two types with the same identity
values stay two vertices.

!!! note "The GraFlo file backend appends"
    The GraFlo file backend writes every record it receives and does not
    upsert. Two records with the same identity are two rows on disk, and
    ingesting the same file twice stores it twice. Write to a database when
    records must merge by key.

## A natural key

In the simplest case the records carry a field that identifies the thing.
Declare it in `identity`. Two records with the same `serial_number` become one
`machine` vertex, and the second one updates the first.

```yaml
vertices:
-   name: machine
    properties: [serial_number, model, plant]
    identity: [serial_number]
```

A key may span several fields. A composite key is not a different mode: the
database matches on all listed fields together.

```yaml
-   name: production_line
    properties: [plant, line_number, name]
    identity: [plant, line_number]
```

If you omit `identity`, every property becomes part of the key, because
`identity_from_all_properties` on `vertex_config` defaults to `true`. That suits
a lookup type such as `department` with a single `name` property. It does not
suit a type that carries measurements, where a changed value would create a
new vertex. Set `identity_from_all_properties: false` to make an omitted
`identity` an error when the manifest is loaded.

A `LIST`-typed property cannot be part of an identity. A natural identity field
typed `UUID` is checked when the graph is written: a value that is not a UUID
raises an error. GraFlo does not generate a value for it.

In [the CSV example (01)](../../examples/csv-two-resources/index.md), both
files mention each person; `identity: [id]` is what makes the two mentions one
vertex.

## Records without a key

Some records carry no field that identifies them. GraFlo offers three answers,
depending on whether the records should merge when they are ingested again.

### A hash of properties

A combination of fields may identify a record without being a key you want to
store and match on, for example a sensor and a timestamp. List those fields in
`hash_identity_properties`. When the records are
[cast](../glossary.md#casting), GraFlo hashes their values (SHA-256) into a
synthetic `id`, and the vertex upserts on `id`. The
same values always give the same `id`, so ingesting the same rows again does
not create duplicates.

```yaml
-   name: reading
    properties: [sensor_id, taken_at, value]
    identity: [id]
    hash_identity_properties: [sensor_id, taken_at]
```

The hash is written to `id` only when the record has none, so a record
carrying its own `id` would keep it. A vertex keyed by a hash that leaves
`identity` out therefore may not declare a property `id`; loading one is
refused, as is `replace_identity` onto a hash or a funnel for a vertex that
declares one.

Only the listed fields enter the hash. A record whose listed fields are all
empty gets no `id`, rather than the hash of `{sensor_id: null, taken_at: null}`;
otherwise every such record would fold onto one vertex. A record with no
identity is dropped after casting, and the count is logged. That is
`drop_empty_identity_docs` on `IngestionParams`, which defaults to `true`.

### A blank vertex

A [blank vertex](../glossary.md#blank-vertex) is a placeholder with no business
identity at all, such as a mention that exists only to hang an edge on. When
the graph is written, each record that carries no `id` gets a random UUID. Blank
vertices therefore never merge: the same record ingested twice makes two
vertices.

```yaml
-   name: mention
    properties: [text]
    blank: true
```

Because a blank vertex has no key, edges to it cannot be matched by key either.
When the graph is written, GraFlo adds them: for every declared edge with a
blank vertex at one end, the blank records of a batch are paired with the other
endpoint's records from the same batch. The pairing uses the identity fields
the two types share, and position in the batch when they share none.

### An assigned UUID

An assigned vertex has a UUID primary key by design, such as an event. If a
record carries the UUID, GraFlo keeps it and checks its format; a value that is
not a UUID raises an error. If the field is empty, GraFlo fills it with a new
UUID when the records are cast, before edges are built, so edges can use the
key. Unlike a blank vertex, an assigned vertex is an ordinary entity with a
stable key once it has one.

```yaml
-   name: event
    properties:
    -   {name: id, type: UUID}
    -   {name: payload, type: STRING}
    identity: [id]
    assigned: true
```

A record without a UUID gets a new one every time it is ingested, so ingesting
it again creates another vertex. Assigned identity fits sources that carry the
UUID, or records that are ingested once. A natural key that happens to be a
UUID (`identity: [external_id]` with `external_id` typed `UUID`) is a natural
key, not an assigned one.

## A source that knows only an alternative identifier

A source that only describes relations often names its endpoints by a business
key that is not the primary identity, and carries no primary key at all. A
work-order export names machines by `asset_tag`, while `machine` upserts on
`serial_number`.

Declare the alternative key as a
[secondary identity](../glossary.md#secondary-identity) on the vertex.
Upserts keep using `identity`; a secondary identity is used only to find a
vertex that already exists.

```yaml
-   name: machine
    properties: [serial_number, asset_tag, model]
    identity: [serial_number]
    secondary_identities:
    -   name: by_tag
        fields: [asset_tag]
```

Then, in the [resource](../glossary.md#resource) that refers to machines
without owning them, mark the [vertex step](../glossary.md#vertex-step)
`lookup_only: true`. Its records take part in edges but are never written as
vertices; without the flag, each row would be written as a machine with no
`serial_number`. On the [edge step](../glossary.md#edge-step), choose the
secondary identity per endpoint with `source_match` or `target_match`:

```yaml
resources:
-   name: work_orders
    pipeline:
    -   vertex: machine
        lookup_only: true
    -   vertex: work_order
    -   from: work_order
        to: machine
        relation: targets
        target_match: by_tag
```

A selector is a declared name, a field list equal to a declared field set, or
`secondary` when the vertex declares exactly one. Omitted, or `identity`, means
the primary identity. An unknown selector is refused when the manifest is
initialized, before any record is read, and the error lists the declared names.
A secondary identity written as a bare field list, without `name`, is named
`secondary_<n>`, where `n` is its position in the list, counting from zero.

When the endpoint is filled by a [vertex router](../glossary.md#vertex-router),
its rows belong to several vertex types, which need not declare the same
secondary identity. Give a mapping from type to selector. A type the mapping
does not name is matched on its primary identity. Here a maintenance log names
either a machine, by its asset tag, or a sensor, by its primary key:

```yaml
-   name: maintenance_log
    pipeline:
    -   type_field: equipment_type
        vertex_from_map:
            machine: {asset_tag: equipment_ref}
            sensor: {sensor_id: equipment_ref}
        lookup_only: true
    -   vertex: work_order
    -   from: work_order
        target_role: equipment_type
        relation: targets
        target_match: {machine: by_tag}
```

The edge step names the router by its role, which is its `type_field` unless
the router sets `role`. A router takes `lookup_only: true` for every type it
routes to, or a list of the types it only looks up; the others are written as
usual.

Endpoints are found by reading the database immediately before the edge is
written. The resource that owns the vertices must therefore be ingested before
the resource that refers to them. Resources run in the order they are declared,
each one finishing before the next starts. A row whose key matches no vertex,
or whose composite key is incomplete, produces no edge and is counted in the
log. A partial key is never partially matched.

Each secondary identity gets a non-unique index automatically, and on
NebulaGraph the lookup cannot run without it; see
[backend indexes](backend_indexes.md).

[The secondary identities example (16)](../../examples/secondary-identities/index.md)
runs a source that describes only relations and finds its endpoints by
alternative identifiers.

## Sources that fill different identifiers

Two sources describe the same people. One knows their email, the other their
phone number and country. No single field set keys every record. An
[identity funnel](../glossary.md#identity-funnel) declares an ordered list of
branches. For each record, the first branch whose required fields are all
present and non-empty wins, and its values are hashed into `id`.

```yaml
-   name: party
    properties: [email, phone, country, name]
    identity: [id]
    identity_funnel:
        branches:
        -   id: email
            fields: [email]
        -   id: phone
            fields: [phone, country]
```

A funnel is the general form of `hash_identity_properties`: a funnel with one
branch hashes the same fields. The branch id is part of the hash by default
(`include_branch_id: true`), so two branches over equal values cannot produce
the same key. A record that completes no branch has no identity and is
dropped, as with a flat hash. GraFlo does not invent a key for it, because a
random key would create a new vertex on every ingest.

A funnel merges records that share a branch. A person whose email appears in
one source and whose phone appears in the other becomes two vertices: deciding
that the two identifiers belong to one person needs evidence the funnel does
not have. [Cross-resource identity discovery](cross_resource_identity.md)
looks for that evidence in samples. The same rule is why, in a union, a
member's own key stops deduplicating its records once the merged type is keyed
on a funnel; see
[Merging manifests](merging_manifests.md#the-old-key-no-longer-deduplicates).

[The identity funnel example (17)](../../examples/identity-funnel/index.md)
runs two sources through one funnel.

## When a lookup matches several vertices

A secondary identity is only softly unique. Its index is non-unique, so the
database accepts two machines with the same `asset_tag`, and a lookup can match
both. `endpoints_on_ambiguous` on `ingestion_model` decides what happens then.

| Policy | Behavior |
|---|---|
| `all` (default) | Attach the edge to every match, so no relation is lost |
| `first` | Attach it to the match whose primary identity sorts first, so every run chooses the same vertex |
| `skip` | Write no edge for that row, and count it |
| `error` | Raise an error, which stops the batch |

An edge step overrides the model default with `on_ambiguous`. Neither setting
affects endpoints matched on their primary identity.

```yaml
ingestion_model:
    endpoints_on_ambiguous: skip
    resources:
    -   name: work_orders
        pipeline:
        -   vertex: machine
            lookup_only: true
        -   vertex: work_order
        -   from: work_order
            to: machine
            relation: targets
            target_match: by_tag
            on_ambiguous: error
```

On the GraFlo file backend, ingesting the owning resource twice stores every
machine twice, and every lookup then matches two rows.

## Options

On a vertex:

| Field | Default | Meaning |
|---|---|---|
| `identity` | all properties, when `identity_from_all_properties` is `true` | Fields the vertex upserts on. GraFlo sets it to `[id]` for hash, funnel, blank and assigned vertices |
| `hash_identity_properties` | `[]` | Fields hashed into `id` |
| `identity_funnel` | none | Ordered branches hashed into `id`: `branches` (each with `id`, `fields`, and `when_all_present`, which defaults to `fields` and must be a subset of them), `include_branch_id` (`true`), `digest` (`sha256`, the only value) |
| `secondary_identities` | `[]` | Alternative field sets for lookups; each has `fields` and an optional `name` |
| `blank` | `false` | Placeholder that gets a random UUID when the graph is written |
| `assigned` | `false` | UUID primary key, filled when empty |

Elsewhere:

| Field | Where | Default |
|---|---|---|
| `identity_from_all_properties` | `vertex_config` | `true` |
| `lookup_only` | `vertex` step (`true`), or `vertex_router` step (`true`, or a list of the types only looked up) | `false` |
| `source_match`, `target_match` | edge step; for an endpoint filled by a `vertex_router`, a mapping `{type: selector}` | the primary identity |
| `on_ambiguous` | edge step | the model's `endpoints_on_ambiguous` |
| `endpoints_on_ambiguous` | `ingestion_model` | `all` |
| `drop_empty_identity_docs` | `IngestionParams` | `true` |

The property `Vertex.identity_mode` reports which of `natural`, `hash`, `blank`
or `assigned` applies to a vertex. A funnel reports `hash`;
`Vertex.has_identity_funnel` tells the two apart.

## Rules and limits

- The modes exclude each other. A vertex that declares `blank` with `assigned`,
  either of them with `hash_identity_properties` or `identity_funnel`, or
  `hash_identity_properties` with `identity_funnel` is refused when it is
  loaded. Otherwise GraFlo would use one declaration and ignore the other.
- A blank vertex cannot declare secondary identities. Its identity is
  generated, so no source can know it.
- A secondary identity may not repeat the primary identity, and two secondary
  identities may not share a field set.
- The hashed fields, the funnel branches and their order all feed the hash.
  Changing any of them gives every stored vertex a different `id`. The schema
  migration planner reports this as `REKEY_VERTEX` at critical risk; see
  [migration and practices](../operations/migration_and_practices.md). To
  change an identity in a manifest, use the `replace_identity` operation
  described in [manifest evolution](manifest_evolution.md).
- A secondary identity cannot be derived from a hash or a funnel, and a vertex
  cannot be upserted by its secondary identity.
- Hash and assigned ids exist from the moment the records are cast, so edges
  built from the same records can use them. Blank ids exist only once the graph
  is written.
- The TigerGraph bulk load path does not assign blank ids and does not add
  edges to blank vertices. Load blank vertex types through the ordinary write
  path.

## Inferring an identity

When you do not know which fields identify a record, GraFlo can propose an
identity from a sample of your data. Single-resource inference proposes a
natural key (one field or several) or, when no narrow key exists, a hash over
the fields that do identify the records. [Finding a key for your
data](../../guides/identity_inference.md) is the task guide, and
[the identity inference example (15)](../../examples/identity-inference/index.md)
runs it on two CSV files. When several sources describe one vertex type under
different column names, [cross-resource identity
discovery](cross_resource_identity.md) aligns their columns and can propose a
funnel; [the cross-resource identity
example (18)](../../examples/cross-resource-identity/index.md) shows it.

## What to read next

- [Backend indexes](backend_indexes.md): the index each backend creates for the
  identity and for secondary identities.
- [Finding a key for your data](../../guides/identity_inference.md): propose an
  identity from your own records.
- [Creating a manifest](../../getting_started/creating_manifest.md): the schema
  block around these declarations.
