# Glossary

This page defines the terms used across the GraFlo documentation, one name and one short definition per term, grouped in the order you meet them. Other pages link a term's first use here, and each entry ends with a link to the page that covers the term in full.

## The big picture

### GraFlo

A Python library that loads records from files, SQL tables, RDF, REST APIs and Kafka topics into a labeled property graph, from a description of the graph written once in a manifest. The product is GraFlo; the package, the import and the command are `graflo`. See [Home](../index.md).

### GSTL

Graph Schema & Transformation Language: the language a manifest is written in, covering the graph's types, how records become vertices and edges, and the operations that change a manifest. GraFlo implements it. See [Concepts overview](index.md).

### manifest

One YAML document, or one `GraphManifest` object, that describes a graph and how to load it in three blocks: the schema, the ingestion model and the bindings. Any block may be left out, but not all three, and a manifest names no database and holds no credentials. See [Creating a manifest](../getting_started/creating_manifest.md).

### schema

The `schema` block of a manifest: the vertex and edge types under `graph:` (also spelled `core_schema:`), a `metadata` block with a required `name`, and the database profile. The class is `Schema`. See [Core components](architecture/core_components.md).

### ingestion model

The `ingestion_model` block of a manifest: the resources, a registry of named transforms, and two write policies, `edges_on_duplicate` and `endpoints_on_ambiguous`. The class is `IngestionModel`. See [Creating a manifest](../getting_started/creating_manifest.md).

### bindings

The `bindings` block of a manifest: the connectors, `resource_connector` rows that pair each resource with its connectors, and `connector_connection` rows that give a connector a connection proxy. A connector may also name its resource directly with `resource_name`. See [Creating a manifest](../getting_started/creating_manifest.md).

### GraphEngine

The Python class you call to act on a manifest: it defines the schema in a database, ingests, samples sources, infers a manifest, and exports or migrates a graph. Import it with `from graflo import GraphEngine`. See [Quick start](../getting_started/quickstart.md).

## Schema

### vertex

A node of the graph. A vertex type, declared under `vertex_config.vertices`, describes many vertices: its name, its properties and its identity.

```yaml
- name: machine
  properties: [serial_number, model]
  identity: [serial_number]
```

See [Core components](architecture/core_components.md).

### edge

A link between two vertices. An edge type, declared under `edge_config.edges`, names a source and a target vertex type and optionally a relation, and it is directed unless it sets `directed: false`.

```yaml
- source: work_order
  target: machine
  relation: targets
```

See [Core components](architecture/core_components.md).

### relation

The name of an edge type, its `relation` key; `(source, target, relation)` identifies the edge type, so two edge types between the same vertex types differ by relation. Most databases store it as the relationship or edge type, and ArangoDB stores it as a `relation` attribute on the edge. See [Core components](architecture/core_components.md).

### property

A named value on a vertex or an edge, written as a bare name (`serial_number`) or as a mapping with a `type` (`INT`, `FLOAT`, `STRING`, `DATETIME`, `LIST` and others) and optional grounding, such as `{name: temp_c, type: FLOAT}`. Properties are not called weights; `vertex_weights` is a separate edge step option that copies vertex properties onto an edge. See [Core components](architecture/core_components.md).

### grounding

An optional `semantics` block on the schema, a vertex type, an edge type or a property that ties it to an external vocabulary: an `iri`, `exact_match` IRIs, `synonyms` and, on a property, a `unit`. Nothing reads it when records are cast or written; the `world-model` profile checks it. See [Conformance profiles](schema/world_model_profile.md).

### identity

The properties that decide whether two records are the same vertex: records with equal identity values become one vertex, which the database upserts. Each vertex type has one identity mode, `natural` (the `identity` list), `hash` (`hash_identity_properties` or an identity funnel), `blank` (a random key per vertex) or `assigned` (a UUID filled in when records are cast), and a type that declares none is keyed on all its properties because `identity_from_all_properties` defaults to `true`. See [Vertex identity](schema/vertex_identity.md).

### secondary identity

Another set of properties, declared under `secondary_identities`, that finds an existing vertex without being used to write it, so an edge step can match an endpoint by a business key with `source_match` or `target_match`. The database does not enforce it as unique, and `endpoints_on_ambiguous` decides what happens when it matches several vertices. See [Vertex identity](schema/vertex_identity.md).

### identity funnel

An ordered list of branches, each naming properties that identify a record; the first branch whose fields are all present is hashed into the vertex's identity field: `id`, or the one field `identity` names. A record that completes no branch gets no identity and is dropped, and a funnel with one branch equals `hash_identity_properties`.

```yaml
identity_funnel:
  branches:
    - {id: serial, fields: [serial_number]}
    - {id: tag, fields: [plant_code, asset_tag]}
```

See [Vertex identity](schema/vertex_identity.md).

### blank vertex

A vertex type declared with `blank: true`, for things that have no key of their own. Each of its vertices gets a random UUID when the graph is written, so blank vertices never deduplicate. See [Vertex identity](schema/vertex_identity.md).

### inverse

A second relation name for reading an edge type from its target, such as `has_work_order` for `targets`, declared once per pair under `edge_config.inverses`; a symmetric relation, one that is its own inverse such as `adjacent_to`, goes under `edge_config.symmetric` and its edges must be `directed: false`. The declaration alone stores nothing; storing the reverse edges is a separate choice, a materialized or a native inverse.

```yaml
edge_config:
  inverses:
    - {relation: targets, inverse: has_work_order}
  symmetric: [adjacent_to]
```

See [Manifest evolution](schema/manifest_evolution.md#inverse-relations-in-detail).

### materialized inverse

An inverse pair stored as ordinary edges in the reverse direction: the reverse edge type is declared, and edge steps with `emit_inverse: true` write it from the same records. It works on every target backend, and `graflo inverses audit` reports resources that feed one direction and not the other. See [Manifest evolution](schema/manifest_evolution.md#inverse-relations-in-detail).

### native inverse

An inverse pair that the database maintains itself, listed by relation under `db_profile.native_inverses`. Only TigerGraph supports it, and a relation cannot be both native and materialized. See [Manifest evolution](schema/manifest_evolution.md#inverse-relations-in-detail).

### database profile

The `db_profile` block of the schema: what the target database needs and the logical model does not, such as `db_flavor` (default `arango`), the namespace, storage names for names the database refuses, secondary indexes and native inverses. The class is `DatabaseProfile`. See [Core components](architecture/core_components.md).

### namespace

The database, graph or space a schema is deployed into: an ArangoDB or Neo4j database, a TigerGraph graph, a NebulaGraph space. It is, in this order, an explicit override from the call or the connection config, `db_profile.target_namespace`, or `schema.metadata.name` rewritten to what the database accepts. See [Graph namespace and schema](../guides/graph_namespace_and_schema.md).

## Ingestion

### record

One item a connector yields: a CSV row, a JSON object, an element of an API response, a Kafka message, or the properties of one RDF subject. Error reports call a record a document. See [Concepts overview](index.md).

### resource

The recipe that turns one kind of record into vertices and edges: a `name` and a pipeline of steps. Which files, tables or topics feed it is set in the bindings, and the class is `ResourceConfig`.

```yaml
- name: work_orders
  pipeline:
    - vertex: work_order
    - vertex: machine
    - edge: {from: work_order, to: machine, relation: targets}
```

See [Core components](architecture/core_components.md).

### pipeline

The ordered list of steps under a resource's `pipeline:` key, run on every record of the resource. `apply:` is accepted as an alias. See [Core components](architecture/core_components.md).

### step

One entry of a pipeline, recognized by its key: `vertex`, `edge`, `transform`, `descend` or `vertex_router`. The API reference calls the classes that run steps actors. See [Core components](architecture/core_components.md).

### vertex step

A step that makes one vertex of a named type from the current record, taking each property whose name matches a field and mapping the others with `from`. With `lookup_only: true` it only finds an existing vertex for an edge and writes nothing. See [Core components](architecture/core_components.md).

### edge step

A step that connects vertices made by earlier steps, named by `from` and `to` or by `source_role` and `target_role`, under a fixed `relation` or one read from the record. A resource already connects the vertices of one record by every edge type the schema declares between them (`infer_edges`, default `true`), so an edge step is needed only when that is not enough. See [Core components](architecture/core_components.md).

### transform step

A step that rewrites the record before later steps read it: `rename` maps field names, and `call` runs a Python function on named fields. `call: {use: <name>}` runs a named transform. See [Transforms](ingestion/transforms.md).

### descend step

A step that runs a nested pipeline on one part of the record: the value under `key`, or every value with `any_key: true`. When that value is a list, the nested pipeline runs once per element. See [Core components](architecture/core_components.md).

### vertex router

A step that reads the vertex type from a field of the record, for a table that holds several kinds of things. `type_field` names the field and `type_map` translates its values; a value with no entry is used as the type name unless `type_map_only: true`. `vertex_types` limits the types it may produce.

```yaml
- vertex_router:
    type_field: kind
    type_map: {M: machine, S: sensor}
```

See [Core components](architecture/core_components.md).

### transform

A named, reusable function call declared under `ingestion_model.transforms`: a Python `module`, a function `foo`, its `input` and `output` fields and fixed `params`. A transform step runs it by name, and the class is `ProtoTransform`. See [Transforms](ingestion/transforms.md).

### role

A name on a vertex step or a vertex router that lets one record yield several vertices of the same type, such as a machine and the machine that feeds it. An edge step addresses them with `source_role` and `target_role`.

```yaml
- {vertex: machine, role: self}
- vertex: machine
  role: feeder
  from: {serial_number: feeder_serial}
  extraction_scope: mapped_only
- edge: {source_role: feeder, target_role: self, relation: feeds}
```

See [the vertex roles example (06)](../examples/vertex-roles-edge-links/index.md).

### GraphContainer

The graph held in memory before it is written: vertex records grouped by vertex type and edge records grouped by `(source, target, relation)`. Casting produces one per batch, every target backend's writer reads it, and `GraphEngine.export_graph()` returns one for a whole graph. See [Core components](architecture/core_components.md).

### casting

Running a resource's pipeline over a batch of records to produce a GraphContainer, without reading or writing a database. "When records are cast" means during this step, and "when the graph is written" means during the database write that follows it. See [Parallelism](ingestion/parallelism.md).

### ingestion parameters

The options of one ingestion run, set in Python as `IngestionParams` and not stored in the manifest: batch size, parallelism, which resources to run, and what to do with a record that fails. Import them with `from graflo import IngestionParams`. See [Parallelism](ingestion/parallelism.md).

### document error sink

A gzip JSON Lines file, set with `IngestionParams(doc_error_sink_path=...)`, that receives one entry per record or transform step that failed to cast. With `on_doc_error="skip"` (the default) ingestion continues past a failure, and with `"fail"` the first failure stops the batch. See [Document cast errors](ingestion/doc_errors.md).

## Sources and targets

### connector

An entry of `bindings.connectors` that says where a resource's records come from: a file connector (`regex`, `sub_path`), a table connector (`table_name`), a SPARQL connector (`rdf_class`), an API connector (`path`) or a Kafka connector (`topics`). GraFlo tells the kind from its fields, and a connector holds no credentials.

```yaml
- name: work_orders
  regex: "^work_orders.*\\.csv$"
  sub_path: data
  resource_name: work_orders
```

See [Creating a manifest](../getting_started/creating_manifest.md).

### data source

The runtime object that reads a connector's records in batches, such as a file reader, a SQL query, an API pager or a Kafka consumer. GraFlo builds data sources from the bindings when ingestion starts; in Python you can also pass records through an in-memory data source. See [Concepts overview](index.md).

### connection proxy

A label in the bindings, the `conn_proxy` key, that stands for a connection whose address and credentials live outside the manifest. A connection provider resolves it at run time, so one manifest serves every environment.

```yaml
connector_connection:
  - {connector: work_orders, conn_proxy: plant_db}
```

See [the connection proxy example (11)](../examples/connection-proxy/index.md).

### connection provider

The runtime object that turns each connection proxy into a real connection config. `InMemoryConnectionProvider` holds configs you register in Python, and reads API and Kafka configs from environment variables prefixed with the upper-cased proxy label (`PLANT_API_BASE_URL` for `plant_api`). See [API env wiring](../guides/api_env_wiring.md).

### target backend

The database the graph is written to, chosen when you run ingestion by the connection config you pass. Eight are supported: ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, PostgreSQL and the file backend, listed in the `DBType` enum. See [Concepts overview](index.md).

### file backend

A target backend that is a directory instead of a database: `schema.yaml`, `INDEX.json`, and gzip JSON Lines chunks under `vertices/` and `edges/`, configured with `GraFloBackendConfig`. It appends every record it receives and does not upsert, so loading the same records twice without recreating the schema stores them twice. See [Graph export and migration](operations/graph_export_migration.md).

### graph migration

Copying a whole graph, schema and data, from one database to another with `GraphEngine.migrate_graph()`, without a manifest. The source can be Neo4j, ArangoDB, PostgreSQL or the file backend, and the target any of the eight target backends. See [Graph export and migration](operations/graph_export_migration.md).

## Evolution

### op

One typed change to a manifest, such as `rename_vertices` or `add_edges`, written in YAML with an `op:` key or built in Python as a class such as `RenameVerticesOp`. GraFlo carries an op through every place that names what it changes, and an op changes the manifest, never a database. See [Manifest evolution](schema/manifest_evolution.md#operations).

### change set

An ordered list of ops that turns one manifest into another. `apply_evolution` applies it, `invert_ops` computes the ops that undo it where that is possible, and a commit records it.

```yaml
- op: rename_vertices
  renames: {Asset: Machine}
- op: add_vertex_properties
  additions: {Machine: [{name: commissioned_on, type: DATETIME}]}
```

See [Manifest evolution](schema/manifest_evolution.md#what-an-operation-is).

### diff

The change set that turns one manifest into another, derived by `diff_manifests(base, target)` together with warnings for changes no op expresses; `diff_manifests_verified` also replays it and checks the content hash. The differ does not guess renames: without `RenameHints`, a renamed type appears as a removal and an addition. See [Manifest evolution](schema/manifest_evolution.md#deriving-ops-from-two-manifests).

### content hash

A SHA-256 hash of a manifest's canonical form, computed by `manifest_hash`, that covers the schema, the ingestion model and the bindings but not the name, version or other metadata. Two manifests that describe the same graph hash equal, which is how a commit checks that replaying its ops rebuilds the same manifest. See [Version control](schema/versioning.md#content-addressing).

### canonical map

A translation of one manifest's names into shared names, in three parts: `vertices`, `properties` (keyed by the type's name on that side) and `relations`; a name with no entry keeps its spelling. A union applies one map per side, and `graflo canonical-check` lists entries that match nothing in a manifest. See [Merging manifests](schema/merging_manifests.md#naming-the-merged-type).

### vertex equivalence

A declaration in a union that vertex types on the two sides are one type, written as an entry of `vertex_equivalences` such as `{left: Asset, right: Device, into: Machine}`: `left` and `right` name the members, and `into` names the merged type. `relation_equivalences` does the same for relations. See [Merging manifests](schema/merging_manifests.md#declaring-which-types-are-one).

### cluster

A group of a union taken as a whole: the types its equivalences name, every type a canonical map merges with one of them, and the one merged name they take. Error messages use the word, as in `unnamed vertex cluster`. See [Merging manifests](schema/merging_manifests.md#a-vocabulary-that-merges-several-types).

### identity branch

One entry of a vertex equivalence's `identity`, the merged type's key in priority order: a property the members carry (`serial_number`, or a composite `[plant, tag]`), a derived branch that each resource computes from its own columns (`{name, sources}`, class `DerivedBranch`), a name or composite over attributes declared in `derive`, or a `local_key` fallback, always last (`LocalKeyBranch`). One property branch is a natural key; any other list keys the type on an identity funnel over the branches. See [Merging manifests](schema/merging_manifests.md#keying-the-merged-type).

### union

Combining two unrelated manifests into one with `merge_manifests(left, right, op)` or `graflo merge`. Nothing is inferred: the op declares equivalences with their identities and canonical maps, and the union refuses, naming the problem, when the two sides disagree on a type, a unit, a key or a database setting. See [Merging manifests](schema/merging_manifests.md).

### three-way merge

Reconciling two branches of one manifest's history against their common base, with `merge_three_way` or `graflo merge3`. Changes to different slots merge, the same change on both sides merges once, and different changes to one slot are reported as conflicts rather than resolved by a guess. See [Version control](schema/versioning.md#merging-two-branches).

### slot

A place in a manifest that an op changes, such as the type of one property of one vertex type; it is the unit of conflict in a three-way merge. A whole pipeline is one slot, a rename occupies both the old and the new name, and an op that touches a contested slot is held back whole. See [Version control](schema/versioning.md#slots).

### commit

A recorded change set with the content hash before and after it, its parent commits, a kind (`root`, `edit`, `merge`, `merge3` or `revert`) and an id derived from its content. Commits are stored one YAML file each under `.graflo/commits` and handled with `graflo commit`, `log`, `checkout`, `verify`, `revert` and `stamp`. See [Version control](schema/versioning.md#commits).

### live drift

The difference between a schema and the database that should follow it, reported by `GraphEngine.diff_live_schema()`: vertex types, edge types and property names present on one side only. Property types, identities and indexes are not compared. See [Live schema drift](schema/live_drift.md).

### schema migration

Changing a live database so that it matches a new schema version, with `graflo migrate-schema` or the `graflo.migrate` package. Each planned change carries a risk level, from low to critical, and only low-risk changes run by default; ops, by contrast, change only the manifest. See [Schema migration](operations/migration_and_practices.md).

### lift

Adding state and provenance types to a manifest with `graflo lift` or `plan_lift`, from a spec that says what the types mean. The result is a change set that changes the schema only: no resource feeds the new types until you write one.

```yaml
grounding: {Machine: {iri: "http://www.w3.org/ns/sosa/FeatureOfInterest"}}
stateful: {Machine: [status]}
observed: [Machine]
measured: {Machine.operating_temp: Cel}
```

See [the state-core lift example (23)](../examples/state-core-lift/index.md).

## Checks

### conformance profile

A named, versioned set of assertions about a manifest, run with `graflo check --profile <name>`; GraFlo ships `world-model` (the default) and `inverses`. A check reads only the manifest and exits `0` when it conforms, `1` when it does not, and `2` when it could not run. See [Conformance profiles](schema/world_model_profile.md).

### assertion

One checkable claim in a conformance profile, with an id such as `declared-units`. Its status on a manifest is `pass`, `fail`, `warn`, `waived`, or `not_applicable` when there was nothing to examine. See [Conformance profiles](schema/world_model_profile.md).

### waiver

An operator's decision, with a required reason, that one assertion does not apply to a deployment, such as `{assertion: temporal, reason: reference data that does not change}`. Waivers live in a separate YAML file passed with `--waivers`, not in the manifest, and a waived assertion reports `waived`, never `pass`. See [Conformance profiles](schema/world_model_profile.md#waivers).

### world-model profile

The conformance profile named `world-model`, version 0.1, with six assertions: `grounded-types`, `declared-identity`, `declared-directionality`, `declared-units`, `temporal` and `provenance`. Together they check that a manifest says what its types mean, how they are keyed, which way its edges run, what its numbers measure, and when and where its facts come from. See [Conformance profiles](schema/world_model_profile.md#the-world-model-profile).

### state-core

The types a lift adds so that a manifest can record how facts change and where they came from: a state type and an observation type for each entity type (the thing facts are about, such as `Machine`), plus shared `Evidence` and `Agent` types. They are grounded in the PROV-O and SOSA vocabularies and linked by `specializationOf`, `hasFeatureOfInterest`, `wasDerivedFrom` and `wasAttributedTo` edges. See [the state-core lift example (23)](../examples/state-core-lift/index.md).

### state

A vertex type named `<Type>State` that holds the properties of an entity that change over time, each value valid from `valid_from` to `valid_to`: a machine keeps its serial number, and its `status` moves to `MachineState`. It is keyed on the entity's key plus `valid_from`, so each value is a separate fact rather than an overwrite of the last one. See [the state-core lift example (23)](../examples/state-core-lift/index.md).

### observation

A vertex type named `<Type>Observation` that holds one measurement of an entity at one time: `observed_property`, `result_value`, `result_unit` (a UCUM unit such as `Cel`) and `result_time`. A temperature reading taken on a machine is one `MachineObservation`. See [the state-core lift example (23)](../examples/state-core-lift/index.md).

## Inference and sampling

### sample

Records pulled from sources and kept as they were read, flat rows for a table and nested documents for an API, one `ResourceSample` per resource inside a `SourceSample`. `GraphEngine.sample_resources()` takes a PostgreSQL config, a bindings block or a file path. See [Sampling and profiling](schema/sampling_and_profiling.md).

### profile

The flat, typed description of a sample, computed by `profile_sample`: one entry per field path (`customer.id`, `items[].sku`) with its type, how often it is present or null, and example values. It is unrelated to a conformance profile. See [Sampling and profiling](schema/sampling_and_profiling.md).

### card

A short summary of one manifest element, such as a `SchemaCard`, `VertexCard`, `EdgeCard` or `ResourceCard`, for a person or a language model meeting it for the first time. Each card carries an estimate of its size in tokens. See [Cards](schema/cards.md).

### identity inference

Proposing an identity for one vertex type from sample records: a single unique column, a composite key, or a hash when no key exists. `IdentityInferencer` makes the proposal, and nothing changes until you write it into the manifest. See [Finding a key for your data](../guides/identity_inference.md).

### cross-resource identity discovery

Proposing one identity for a vertex type that several resources describe under different column names. Similar names and overlapping values choose which columns to compare, but only exact equality of values proves a key; `CrossResourceIdentityInferencer` proposes and `apply_proposal_to_vertex` applies a proposal you accept. See [Cross-resource identity discovery](schema/cross_resource_identity.md).

### schema inference

Producing a manifest, or its schema, from an existing source instead of writing it by hand: from a SQL database with `GraphEngine.infer_manifest()` (PostgreSQL) or SQLAlchemy reflection (other engines), from an OWL or RDFS ontology with `infer_schema_from_rdf()`, or from a graph database with `infer_schema_from_graph()`. The result is a draft to review and edit. See [Inferring a graph from a SQL database](../guides/sql_schema_inference.md).
