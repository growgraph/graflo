# Core components

When you know what graph you want, this page gives the exact keys to write it
down: the vertex and edge types of the `schema`, the
[resource](../glossary.md#resource) and its steps in the `ingestion_model`, and
the names a target database stores. Each key comes with its default and the
rule behind it. How sources are wired to resources is in the
[creating a manifest](../../getting_started/creating_manifest.md) guide.

## Schema

The `schema` block declares the graph: vertex types, edge types, and the
profile of the target database. It says nothing about where records come from
or how they are transformed; that belongs to the `ingestion_model`.

```yaml
schema:
  metadata:
    name: plant
    version: "1.0.0"
  graph:
    vertex_config:
      vertices: [...]
    edge_config:
      edges: [...]
  db_profile: {}
```

`metadata.name` labels the schema. It also names the namespace in the target
(the ArangoDB or Neo4j database, the TigerGraph graph, the NebulaGraph space),
adjusted to the names that database accepts, unless the connection config or
`db_profile.target_namespace` names another. `metadata.version`, when given,
is a semantic version such as `1.0.0`. The key `graph` may also be written
`core_schema`.

### Vertex

A vertex type has a name, its properties, and the properties that identify it.

```yaml
vertices:
  - name: machine
    properties: [serial, model, installed_on]
    identity: [serial]
```

`identity` lists the properties that make two records the same vertex: records
with the same identity values are written as one vertex. An identity may span
several properties (`identity: [plant, tag]`). A property named in `identity`
is added to `properties` if you leave it out. After casting, a vertex with no
value for any identity property is dropped together with its edges
(`IngestionParams.drop_empty_identity_docs`, on by default).

When `identity` is omitted, all properties form the identity. That default is
set per schema by `vertex_config.identity_from_all_properties` (default
`true`). Set it to `false` to require an explicit `identity` on every vertex
type; a forgotten key then fails when the schema loads instead of producing
one vertex per distinct row.

Four declarations cover records without a natural key:

- `blank: true`: a placeholder vertex that gets a random id when the graph is
  written.
- `assigned: true`: a vertex whose key is a UUID made when the record is cast.
- `hash_identity_properties: [a, b]`: a key computed by hashing the listed
  fields, so the same values always give the same key.
- `identity_funnel`: ordered alternatives for that hash; the first whose
  fields are all present is used.

When `identity` is omitted, each of them makes the identity `id`. They are
mutually exclusive. `secondary_identities` declares other field sets that an
edge step may match endpoints on. The rules for each are on
[vertex identity](../schema/vertex_identity.md).

`filters` drops vertex documents that fail a condition when records are cast.
It applies to the documents a vertex step builds from transform output, one
transform result at a time, and not to properties taken straight from the
record or through `from`. The
[filters and weights example](../../examples/vertex-filters-and-weights/index.md)
(5) keeps only measurements with a positive value:

```yaml
- name: metric
  properties: [name, value]
  identity: [name, value]
  filters:
    - field: value
      operator: __gt__
      value: 0
```

`description` and `semantics` document the type for people and for
[cards](../schema/cards.md).

### Supported field types

A property is a name and an optional type. Write the short form when you do not
need types, and the mapping form when you do:

```yaml
properties:
  - serial
  - { name: model, type: STRING }
  - { name: installed_on, type: DATETIME }
  - { name: tags, type: LIST, item_type: STRING }
```

| Type | Holds |
|---|---|
| `INT`, `UINT` | integers |
| `FLOAT`, `DOUBLE` | floating point numbers |
| `BOOL` | booleans |
| `STRING` | text |
| `DATETIME` | timestamps |
| `UUID` | a UUID; TigerGraph, NebulaGraph and PostgreSQL store it as text |
| `LIST` | a list of values of one scalar type, given by `item_type` |

Types are optional on every target. ArangoDB, Neo4j, Memgraph, FalkorDB and
the file backend store each value as it arrives. TigerGraph and NebulaGraph
declare a typed attribute per property and use `STRING` for an untyped one.
PostgreSQL stores every scalar property as `TEXT` and a `LIST` as an array.

Rules for `LIST`, each checked when the schema loads:

- `item_type` is required and must be a scalar type; a list of lists or a list
  of objects is not a `LIST`.
- `item_type` is only valid together with `type: LIST`.
- A `LIST` property cannot be an identity, a hash or funnel field, or part of
  a secondary identity, because two lists have no stable order to compare.
- NebulaGraph has no list property: defining a schema with a `LIST` property
  there raises `UnsupportedFieldTypeError`. The other targets store lists
  (TigerGraph `LIST<T>`, PostgreSQL arrays, Cypher list properties, ArangoDB
  arrays).

For a mixed or nested value, declare a `STRING` property and store JSON in it
yourself. GraFlo never converts a list to a string on its own.

The same property may be declared twice, for instance once in `properties` and
once through an identity. Declarations with the same type fold into one, a
typed declaration wins over an untyped one, and two different types for one
name are refused.

### Edge

An edge type connects a source vertex type to a target vertex type, optionally
under a relation name.

```yaml
edges:
  - source: machine
    target: line
    relation: installed_on
    properties: [since]
```

- `source`, `target`: vertex type names; both must be declared in
  `vertex_config`.
- `relation`: the relationship type (Neo4j) or edge type (TigerGraph,
  NebulaGraph). Omit it when one edge type between the pair is enough. Declare
  several edges with different relations when the same pair is connected in
  several ways (see the
  [relation field example](../../examples/csv-relation-field/index.md) (3)).
- `properties`: attributes stored on the edge, in the same forms as vertex
  properties.
- `identities`: lists of tokens that make an edge unique, so that several edges
  between the same two vertices stay distinct. `source` and `target` stand for
  the endpoints; any other token is an edge property and is added to
  `properties` if missing. Without `identities` an edge is keyed by its
  endpoints and relation alone, and its other properties are written, never
  matched on. Neo4j, Memgraph and FalkorDB match an existing edge (Cypher
  `MERGE`) on the endpoints, relation and the properties of the first identity; see
  [backend indexes](../schema/backend_indexes.md#edge-upserts-and-merge-neo4j-memgraph-falkordb).
- `directed` (default `true`): see the next section.
- `description`, `semantics`: documentation, as on a vertex.

Where the relation comes from when records are cast (a column, a JSON key)
and which records form an edge are set on the `edge` step, not on the schema
edge; see [the `edge` step](#the-edge-step).

### Directed, undirected, and bidirectional edges

An edge is directed by default: `machine installed_on line` is not `line
installed_on machine`. The table lists the other choices.

| You want | Declare | TigerGraph DDL |
|---|---|---|
| One direction | one edge, `directed: true` (default) | `ADD DIRECTED EDGE` |
| A name for the reverse reading, nothing extra stored (a declared inverse) | the pair in `edge_config.inverses` | one `ADD DIRECTED EDGE`; a read of the inverse name raises there, so use one of the next two |
| Both directions stored, on any target (a materialized inverse) | the pair in `inverses`, an edge for each direction, and `emit_inverse: true` on the forward edge steps | two `ADD DIRECTED EDGE` |
| Both directions stored, maintained by the database (a native inverse, TigerGraph only) | the pair in `inverses` and the forward relation in `db_profile.native_inverses` | `ADD DIRECTED EDGE ... WITH REVERSE_EDGE` |
| No direction at all | `directed: false` on the edge | `ADD UNDIRECTED EDGE` |

A relation that is its own inverse (`adjacent_to`) goes in
`edge_config.symmetric`, and its edges must be `directed: false`. All edges of
one relation must agree on `directed`, and the relation of an undirected edge
cannot be part of an inverse pair, because it already reads both ways.

```yaml
edge_config:
  edges:
    - source: machine
      target: machine
      relation: standby_for
  inverses:
    - relation: standby_for
      inverse: covered_by
```

A declared inverse is a statement about the model: a read may ask for
`covered_by`, and GraFlo follows the stored `standby_for` edge from its
target. Store the inverse only when the target needs it. A native inverse
needs `db_profile.db_flavor: tigergraph`, is refused next to declared edges of
the inverse name, and cannot apply to a symmetric relation.

TigerGraph is the only target with an undirected edge type. Everywhere else
`directed: false` is stored as a directed edge and read as a statement about
the model: a read may follow it either way. Those targets accept the
declaration: when the schema is applied, each logs one message per undirected
edge saying what it does with it, and
`Connection.edge_direction_diagnostics(schema)` returns the same messages as
data.

`Connection.fetch_edges(from_type, from_id, direction=...)` reads the edges of
one vertex. `direction` is an `EdgeDirection`
(`from graflo.architecture.graph_types import EdgeDirection`): `OUT`, the
default, follows edges that start at the vertex, `IN` those that end there,
and `ANY` both; `to_type` and `to_id` constrain the vertex at the other end.
Read an undirected edge with `ANY`. A reverse read (`IN` or `ANY`) costs:

| Target | Reverse read |
|---|---|
| ArangoDB, PostgreSQL | the same as a forward read: both endpoints are indexed |
| Neo4j, Memgraph, FalkorDB | little: relationships are stored in both directions |
| NebulaGraph | little: GraFlo adds the reverse clause to the query |
| TigerGraph | possible only on an undirected edge type or one with a native inverse; otherwise it raises `UnsupportedEdgeDirectionError` instead of returning half a neighborhood |
| GraFlo file backend | an index GraFlo builds over the edge files it reads |

!!! warning "Identity keys keep endpoint order"
    On an undirected edge the `source` and `target` tokens of an identity key
    still resolve by position, so `(a, b)` and `(b, a)` count as two keys.
    Write such edges from a consistent side.

The operations that add inverse edges to an existing manifest are on
[manifest evolution](../schema/manifest_evolution.md#inverse-relations-in-detail);
the [edge inverses example](../../examples/edge-inverses/index.md) (19) shows
all three ways of storing an inverse in one manifest.

## Resource and its steps

A resource is the recipe that turns one kind of record into vertices and
edges. It has a name, which connectors refer to, and a `pipeline`: a list of
steps that run on every record.

```yaml
ingestion_model:
  resources:
    - name: work_orders
      pipeline:
        - transform:
            rename: { wo_id: id }
        - vertex: work_order
        - vertex: machine
          from: { serial: machine_serial }
        - edge:
            from: work_order
            to: machine
```

From the record `{wo_id: W1, machine_serial: HP-1, model: X}`, this resource
makes the vertices `work_order {id: W1}` and `machine {serial: HP-1, model: X}`
and an edge between them.

Two rules explain most results:

- **Steps run by kind.** Within one level of the pipeline, GraFlo runs
  `descend` steps first, then transforms in the order written, then vertex
  routers, vertex steps and edge steps. A vertex step therefore sees the
  output of every transform at its level, wherever the transform is written.
- **Edges between the vertices of one record are added for you.** The
  vertices a record produces are connected by every edge the schema declares
  between their types (`infer_edges`, on by default). The `edge` step above
  is only needed when the relation comes from the record, when a record
  should get only some of the relations the schema declares between two
  types, or when the edge needs options such as `vertex_weights`. Between two
  vertices of the same type, the inferred edge starts at the outer one: a
  record and the records nested in it give edges from the record to each
  nested one.

### The `vertex` step

```yaml
- vertex: machine
  from: { serial: machine_serial }
```

- `vertex`: the vertex type to produce.
- `from`: `{property: field}` for the properties whose field name differs.
  Properties with the same name as a field need no entry; the step takes them
  from the record. This is called passthrough.
- `extraction_scope`: `full` (default) allows passthrough; `mapped_only` takes
  only the properties named in `from`, plus those written by transforms at the
  same level. Use `mapped_only` when one record produces several vertices of
  the same type, because the record's other columns describe only one of
  them: in a row about a person that also names the person's parent, the
  parent would otherwise receive the row's `name` and `age`. See the
  [roles and edge links example](../../examples/vertex-roles-edge-links/index.md)
  (6).
- `keep_fields`: restrict passthrough to the listed properties.
- `role`: a name for this vertex when one record produces several vertices of
  the same type (`role: parent`, `role: child`). An `edge` step refers to
  them by `source_role` and `target_role`.
- `lookup_only: true`: find an existing vertex for an edge endpoint, but never
  write it. Set it on resources that only add edges and whose records carry
  another identifier than the vertex's primary key, because writing such a
  record would create a vertex without its key.
- `find: <name>`: find the existing vertex by the named
  [secondary identity](../glossary.md#secondary-identity) and write the
  record's properties onto it; a record that finds none is skipped, never
  created. Edges built from the step match that end on the same identity. With
  `lookup_only: true` the step only finds the vertex for edges.

### The `edge` step

An edge step connects vertices that this resource produces. Each endpoint is
named by its vertex type or, when a record holds several vertices of one type
or the type varies, by a role.

```yaml
- edge:
    from: machine
    to: line
    relation_field: rel_type
    properties: [since]
```

Endpoints, one for each side:

- `from` / `to`: vertex types produced by vertex steps of this resource (also
  written `source` / `target`).
- `source_role` / `target_role`: the `role` of a vertex step or a
  `vertex_router` step, for records where the type or the role varies.
- `links`: a list of `{source_role, target_role, relation}` entries, when one
  record holds several relationships. `links` replaces the endpoint keys
  above.

The relation, one of:

- `relation`: fixed.
- `relation_field`: the record field that holds the relation; a transform may
  compute it. `relation_map` translates raw values to relation names, and with
  `relation_map_only: true` a value that is not in the map writes no edge
  instead of being used as the relation name.
- `relation_from_key: true`: the JSON key under which the record was reached
  (see the [relation from key example](../../examples/json-relation-from-key/index.md)
  (4)).

A step that names no relation takes the one the schema declares between its
two types. When the schema declares several, the manifest is refused at load
and the step must name one. An edge whose `(source, target, relation)` the
schema does not declare is not written; the writer logs a warning that names
it, once per run.

Payload and selection:

- `properties`: more edge properties, added to the schema edge. Like the ones
  the schema declares, their values are read from the record.
- `vertex_weights`: `[{name: <vertex type>, fields: [...]}]` copies properties
  of an endpoint vertex onto the edge, under the name `<vertex type>@<field>`
  (see the [filters and weights example](../../examples/vertex-filters-and-weights/index.md)
  (5)). `filter: {<field>: <value>}` on an entry reads only the vertices whose
  field has that value. Each entry goes on every edge the step writes for the
  record. When an entry reads as many vertices as there are edges, they pair by
  position; any other count takes the first vertex, with a warning.
- `match_source` / `match_target` / `match`: only connect vertices reached
  under this key of a nested record. `exclude_source` / `exclude_target` skip
  vertices reached under it.
- `source_match` / `target_match`: find an endpoint by a named secondary
  identity instead of its primary identity. `on_ambiguous` says what to do
  when several vertices match: `all`, `first`, `skip` or `error`, defaulting
  to `ingestion_model.endpoints_on_ambiguous` (`all`). See
  [vertex identity](../schema/vertex_identity.md).
- `emit_inverse: true`: also write the inverse of every edge this step writes,
  when the schema declares the inverse edge. On a step with `links`, set it on
  each link.
- `strict_edge_types: true`: for steps whose endpoints come from roles, skip a
  record whose resolved `(source, target, relation)` the schema does not
  declare, neither as that edge nor as a relation-less edge between the same
  endpoints. Otherwise such an edge type is added when it is first seen.
  Ingestion sets it on every step, through `strict_references`.

### The `transform` step

A transform renames fields or computes new ones before a `vertex` or `edge`
step reads them. `rename` maps field names; `call` runs a Python function,
named by `module` and `foo`, on chosen fields.

```yaml
- transform:
    rename: { wo_id: id }
- transform:
    call:
      module: builtins
      foo: int
      input: [quantity]
```

A transform used by several resources is declared once under
`ingestion_model.transforms` and run with `call: { use: <name> }`. Every
option is on [transforms](../ingestion/transforms.md).

### The `descend` step

JSON records nest. A `descend` step runs its own pipeline on one part of the
record: the value under `key`, or each value in turn with `any_key: true`.

```yaml
- key: sensors
  pipeline:
    - vertex: sensor
```

When the value is a list, the inner pipeline runs once per element. Inside,
the steps see only that element, not the parent record, and transforms stay
on their own level. A field of the parent that a child vertex needs has to be
in the element itself. Vertices from different levels of one record are still
connected: inferred edges and edge steps see every vertex the record produced.
The [records that refer to their own kind example](../../examples/json-self-edges/index.md)
(2) descends into a list of references.

### The `vertex_router` step

One table sometimes holds several kinds of things, with a column naming the
kind. A `vertex_router` step reads that column and produces a vertex of the
matching type.

```yaml
- vertex_router:
    type_field: kind
    type_map: { M: machine, L: line }
    from: { serial: object_id }
```

- `type_field`: the field whose value chooses the vertex type. A record
  without it produces no vertex.
- `type_map`: raw value to vertex type. A value with no entry is used as the
  vertex type name as it is, and a value that names no declared type is
  skipped. A column that already holds type names therefore needs no
  `type_map` at all.
- `type_map_only: true`: skip values that are not in `type_map`, so the router
  produces only the listed types.
- `vertex_types`: the types the router may produce. A value that resolves,
  after `type_map`, to any other type is skipped. Unlike `type_map_only`, it
  bounds the types rather than the raw values, so unmapped values still pass
  through as type names.
- `from`, `keep_fields`, `extraction_scope`: as on the vertex step, applied to
  every routed type. `vertex_from_map: {type: {property: field}}` replaces
  `from` for one type; it says how a type is projected, not that the router
  produces it.
- `role`: the name an edge step uses in `source_role` / `target_role`.
  Defaults to the name given in `type_field`.
- `lookup_only`: `true` for every routed type, or a list of the types to look
  up without writing.
- `find`: per type, the secondary identity its rows find an existing vertex
  by, as on the vertex step: `find: {machine: by_tag}`.

A router without `type_map_only` or `vertex_types` can produce any vertex type
the schema declares, so GraFlo treats its resource as producing all of them.

Two routers can split one column between them, each with its own `role`:

```yaml
- vertex_router: {type_field: kind, role: plant, vertex_types: [machine, line]}
- vertex_router: {type_field: kind, role: probe, type_map: {S: sensor}, vertex_types: [sensor]}
```

Schema changes keep what a router routes: each value still reaches its type,
renamed, or nothing once the type is removed. When merging types would make a
router admit values it used to skip, the change closes the router instead
(`type_map_only` over the values it accepted). Merging types the router
projects differently splits it into one closed router per projection, on the
same `type_field` and `role`. A property rename or removal gives the type its
own `vertex_from_map` entry. Merging a type the router only looks up with one
it writes is refused. A router left with no type to produce
is removed, together with the edge steps that address its role.

Two examples show the two shapes: [a type map](../../examples/vertex-router-type-map/index.md)
(7) and [rows that name their own types](../../examples/vertex-router-flat-rows/index.md)
(8).

### Resource options

Set beside `name` and `pipeline`:

| Key | Default | Effect |
|---|---|---|
| `infer_edges` | `true` | Connect the vertices of one record by every edge the schema declares between their types. With `false`, only edge steps write edges. |
| `infer_edge_only` / `infer_edge_except` | empty | Allow-list or deny-list of `{source, target, relation}` for inferred edges. The two are exclusive, and each entry must match a declared edge. A pair an edge step already connects is never inferred, so explicit and inferred edges do not duplicate. |
| `drop_trivial_input_fields` | `false` | Remove top-level fields whose value is `null` or `""` before the steps run. `0` and `false` stay; nested values are not touched. |
| `fail_fast` | `false` | Fail the record when a transform's input fields are missing. By default a `rename` maps the fields that are present and a `call` writes nothing. |
| `tolerate_transform_errors` | `true` | When a transform raises, set its output fields to `null`, record the failure, and continue with the record. See [document cast errors](../ingestion/doc_errors.md). |
| `types` | empty | `{field: type}` conversions applied to top-level fields before the steps. The types are `int`, `float`, `str`, `bool`, `bytes`, `list`, `dict`, `tuple` and `set`; other names are ignored. |
| `encoding` | `utf-8` | The character encoding the resource's files are read with: `utf-8` or `ISO-8859-1`. |
| `extra_weights` | empty | Edge properties copied from endpoint vertices as stored in the database, read between the vertex and edge writes of each batch. The resource then runs serially; see [parallelism](../ingestion/parallelism.md). |

## Names in the target database

Some databases refuse some names: TigerGraph rejects its reserved words, names
with a space, `-`, `.` or other punctuation, and names that start with
`gsql_sys_`. A name the database cannot store is a fact about that database,
so it is recorded in `schema.db_profile`, and the names in the schema and the
ingestion model do not change.

| `db_profile` key | Stores |
|---|---|
| `vertex_storage_names` | the collection, label or tag name per vertex type |
| `edge_specs[].relation_name` | the stored relation name per edge |
| `vertex_property_names` | the stored attribute name per vertex property |
| `edge_specs[].property_names` | the stored attribute name per edge property |

Records keep the manifest's names from start to end. The writer translates
every document to stored names at the database call and translates back what
it reads, and the schema is created in the database under the stored names.
With nothing renamed, both translations change nothing.

You do not have to fill the profile yourself. Defining a schema, ingesting,
migrating and the schema-aware reads fill in any stored name the profile lacks
for the target, and names already in the profile win. Record them anyway when
the manifest is shared or changes over time, so they stay stable and are
visible to anyone who reads the database directly:

```python
from graflo import DBType
from graflo.hq.sanitizer import Sanitizer

manifest = Sanitizer(db_flavor=DBType.TIGERGRAPH).sanitize_manifest(manifest)
```

`sanitize_manifest` changes the manifest in place, sets `db_profile.db_flavor`
to the target and returns the same object. `GraphEngine.infer_manifest(...)`
runs it for the engine's target before returning, so an inferred manifest
already has names its target accepts.

Two kinds of read exist. `Connection.graph_neighbors` and `Connection.traverse`
take and return the manifest's names, including an edge read through its
declared inverse name. `fetch_docs`, `fetch_edges`, `fetch_all_docs` and
`fetch_all_edges` take a stored name and return stored names as they are: they
show the database's view, not the schema's.

Beyond names, `db_profile` holds the target flavor (`db_flavor`, default
`arango`), an optional `target_namespace`, secondary indexes (`vertex_indexes`,
`edge_specs[].indexes`; see [backend indexes](../schema/backend_indexes.md)),
`native_inverses`, and `default_property_values`.

An `edge_specs` entry may carry a `purpose`: a second stored copy of the same
edge, with its own `relation_name` and its own indexes (`indexes_mode` says
whether they `inherit`, `append` to or `replace` those of the entry without a
purpose). Only ArangoDB reads it, and only when the schema is defined: it
creates one edge collection per purpose. Ingestion writes every edge to the
entry without a purpose, so a purpose collection holds what you write to it
yourself.

TigerGraph stores a value for every attribute a vertex or edge type declares,
including one a record does not supply. `default_property_values` sets that
value, as the `DEFAULT` clause of the attribute when the schema is created; a
reading of `-1.0` then marks a sensor that never reported, where the type's
own default could be mistaken for a measurement. The other targets ignore the
key.

```yaml
db_profile:
    db_flavor: tigergraph
    default_property_values:
        vertices:
            sensor:
                reading: -1.0
        edges:
        -   source: sensor
            target: machine
            relation: mounted_on
            values:
                since_year: 0
```

## GraphContainer

A `GraphContainer` is the graph held in memory before it is written: the
vertices of one batch, grouped by vertex type, and its edges, grouped by
`(source, target, relation)`. It does not depend on any database. Casting a
batch produces one, the writer consumes it, and `GraphEngine.export_graph()`
returns one for a whole graph, together with its schema, as a `GraFloOutput`.
In JSON each edge group is keyed by a string holding the array
`["source","target","relation"]`.

## What to read next

- [Transforms](../ingestion/transforms.md): every option of the `transform`
  step.
- [Vertex identity](../schema/vertex_identity.md): keys, records without a
  key, and secondary identities.
- [Creating a manifest](../../getting_started/creating_manifest.md): the
  `bindings` block and credentials at run time.
