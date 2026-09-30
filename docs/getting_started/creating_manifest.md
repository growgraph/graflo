# Creating a manifest

A manifest is the one file that tells GraFlo what your graph looks like, how
records become vertices and edges, and where the records come from. This page
explains its three blocks one level deeper than the [quick start](quickstart.md),
so that you can write a manifest for your own data. It assumes you know YAML
and what a labeled property graph is.

## The shape of a manifest

A manifest has three blocks. Each is optional, but at least one must be
present: a file can carry only a `schema`, or only `bindings`, and the rest
can be supplied in Python. This is the manifest of the quick start:

```yaml
schema:
    metadata:
        name: hr
    graph:
        vertex_config:
            vertices:
            -   name: person
                properties: [id, name, age]
                identity: [id]
            -   name: department
                properties: [name]
                identity: [name]
        edge_config:
            edges:
            -   source: person
                target: department
    db_profile: {}

ingestion_model:
    resources:
    -   name: people
        pipeline:
        -   vertex: person
    -   name: departments
        pipeline:
        -   vertex: person
            from: {id: person_id, name: person}
        -   vertex: department
            from: {name: department}

bindings:
    connectors:
    -   regex: "^people.*\\.csv$"
        sub_path: data
        resource_name: people
    -   regex: "^dep.*\\.csv$"
        sub_path: data
        resource_name: departments
```

In Python the three blocks are the attributes `graph_schema`,
`ingestion_model` and `bindings` of `GraphManifest`; the YAML key of the first
is `schema`.

## `schema`: what the graph looks like

The schema declares the graph and nothing about the data.

- `metadata`: `name` and an optional `version`. The name labels the manifest
  and, unless you choose otherwise, names the database, graph or space the
  schema is created in. See
  [Graph namespace and schema](../guides/graph_namespace_and_schema.md).
- `graph`: the vertex and edge types. GraFlo also accepts the key
  `core_schema`, which is the name of the Python attribute and the key GraFlo
  writes when it saves a manifest; the examples use `graph`.
    - `vertex_config.vertices`: one entry per vertex type with `name`,
      `properties` and `identity`. `identity` lists the properties that make
      a vertex unique; two records with the same identity values become one
      vertex. A vertex without an `identity` is keyed by all of its
      properties. Other ways to key a vertex, such as a hash over chosen
      properties or an ordered list of fallback keys, are described in
      [Vertex identity](../concepts/schema/vertex_identity.md).
    - `edge_config.edges`: one entry per edge type with `source` and `target`
      vertex types, an optional `relation` name, and optional `properties`.
      Edges are directed by default; declaring the reverse reading of a
      relation, or a relation that reads the same both ways, is described in
      [Directed, undirected, and bidirectional edges](../concepts/architecture/core_components.md#directed-undirected-and-bidirectional-edges).
- `db_profile`: what differs per database and does not change the logical
  graph, such as secondary indexes and stored names. An empty `{}` is fine to
  start with; its keys are listed under
  [Names in the target database](../concepts/architecture/core_components.md#names-in-the-target-database).

Properties can carry a type. Write a mapping instead of a bare name:

```yaml
-   name: article
    identity: [doi]
    properties:
    -   {name: doi, type: STRING}
    -   {name: tags, type: LIST, item_type: STRING}
```

The types are `INT`, `UINT`, `FLOAT`, `DOUBLE`, `BOOL`, `STRING`, `DATETIME`,
`UUID` and `LIST`. `LIST` needs a scalar `item_type`, and a list property
cannot be part of an identity. Types are optional; TigerGraph, which needs a
type for every attribute, gets `STRING` for a property without one.

## `ingestion_model`: how a record becomes vertices and edges

A resource is the recipe that turns one kind of record into vertices and
edges. Its `pipeline` is a list of steps that run on every record, in order:

- `vertex: <name>` reads the properties of that vertex type from the record.
  `from: {property: field}` names the record field for each property whose
  name differs.
- `transform` renames fields or computes new ones before a `vertex` step
  reads them.
- A step with a `key` and its own `pipeline` moves into the nested part of the
  record under that key and runs its steps there.
- `edge` connects vertices that earlier steps produced. You need it only when
  the schema declares several edges between the same two types, or when the
  relation comes from the record.
- `vertex_router` picks the vertex type for each record from one of its
  fields.

When one record yields a `person` and a `department`, and the schema declares
an edge between those types, GraFlo adds the edge without an `edge` step.

A transform that several steps share is declared once under
`ingestion_model.transforms`, with a `name`, and used from a step as
`transform: {call: {use: <name>}}`. Transforms are described in
[Transforms](../concepts/ingestion/transforms.md), and every step and its
options in [Resource and its steps](../concepts/architecture/core_components.md#resource-and-its-steps).
Options on a resource decide how it treats imperfect records, such as empty
fields, missing inputs and failing transforms; they are listed under
[Resource options](../concepts/architecture/core_components.md#resource-options).

## `bindings`: where the records come from

A connector describes one source of records: a file name pattern, a table, an
RDF class, an API endpoint or a Kafka topic. Each connector is matched to the
resource that reads its records.

- `connectors`: the list of connectors. GraFlo tells the kind from the keys:
  `regex` and `sub_path` make a file connector, `table_name` a table,
  `rdf_class` an RDF class, `path` an API endpoint, `topics` a Kafka topic. A
  connector names its resource with `resource_name`, as in the example above.
- `resource_connector`: the alternative to `resource_name`, a list of
  `{resource: <resource name>, connector: <connector name>}` rows. One
  resource may appear on several rows when its records come from several
  places. A connector that a row refers to needs a `name`.
- `connector_connection`: for sources that need credentials, a list of
  `{connector: <connector name>, conn_proxy: <label>}` rows. The manifest
  holds only the label; at run time you supply the connection settings for
  that label, so the file never holds a password. The
  [connection proxy example (11)](../examples/connection-proxy/index.md)
  shows how.

The fields of each connector kind are described in
[Table views](../concepts/connectors/table_views.md),
[API connector](../concepts/connectors/api_connector.md) and
[Kafka connector](../concepts/connectors/kafka_connector.md). Changing a
connector after the manifest is loaded, for example to narrow a table to a
date range, is described in
[Runtime connector updates](../concepts/connectors/runtime_updates.md).

When the sources are known only at run time, leave `bindings` out of the file
and set it in Python:

```python
from graflo import Bindings, FileConnector, GraphManifest

manifest = GraphManifest.from_yaml("manifest.yaml")
manifest.bindings = Bindings(
    connectors=[
        FileConnector(
            regex=r"^people.*\.csv$", sub_path="data", resource_name="people"
        ),
        FileConnector(
            regex=r"^dep.*\.csv$", sub_path="data", resource_name="departments"
        ),
    ]
)
```

## Load, check and run

Loading the file checks each block: an edge that names an undeclared vertex
type, two edges with the same source, target and relation, or two resources
with the same name are rejected. `finish_init()` then connects the resources to
the schema. It does not check that the vertex types named in steps are
declared unless you call it as `finish_init(strict_references=True)`.

```python
from graflo import GraphEngine, GraphManifest
from graflo.connections import ArangoConfig
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_yaml("manifest.yaml")
manifest.finish_init(strict_references=True)

conn_conf = ArangoConfig.from_env()

engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=False),
    recreate_schema=False,
)
```

`define_and_ingest` creates the schema in the database and then loads the
records. With `recreate_schema=False` it raises an error when the schema
already exists; to load more records into an existing graph, call
`engine.ingest(manifest=manifest, target_db_config=conn_conf)` instead. With
`clear_data=False` the data already there is kept, and a record whose identity
matches a stored vertex updates it. Where `ArangoConfig.from_env()` gets its
values, and the classes for the other databases, are described in
[Database connections](../guides/database_connections.md).

### Load part of the data

`IngestionParams` can restrict one run without changing the manifest:
`resources=["people"]` runs only those resources, `connectors=[...]` only
those connectors (by name or hash, as in `bindings.resource_connector`), and
`vertices=["person"]` writes only those vertex types. `max_items` stops after
that many records from each file or table, which is useful for a first look at a large
source.

```python
IngestionParams(resources=["people"], max_items=100)
```

### Run from the command line

`graflo ingest` does the same without Python. It reads the database settings
from a YAML file (see [Database connections](../guides/database_connections.md))
and the records through the manifest's bindings:

```bash
graflo ingest --db-config-path db.yaml --schema-path manifest.yaml \
    --source-path . --fresh-start true
```

File connectors resolve `sub_path` against the directory you run the command
from, so run it where the manifest expects its data. `--fresh-start true`
recreates the schema; without it the records are added to an existing graph.

For files and SQL tables you can also list the sources in a separate file and
pass it with `--data-source-config-path`, instead of declaring them in the
manifest's bindings:

```yaml
data_sources:
-   source_type: file
    resource_name: people
    path: data/people.csv
```

API and Kafka sources cannot be listed this way: declare them in the
manifest's bindings and supply their credentials with a connection provider,
as described in [API connector](../concepts/connectors/api_connector.md) and
[Kafka connector](../concepts/connectors/kafka_connector.md).

## Writing tips

- Give each edge between the same two vertex types its own `relation`; two
  edges that differ in nothing else are rejected.
- Start with an empty `db_profile` and let GraFlo fill in stored names for the
  target database; record them only when the manifest is shared.
- Once a manifest is in use, change it with recorded operations rather than by
  hand, so that the change can be reviewed, replayed and undone; see
  [Evolving a manifest](../guides/evolving_a_manifest.md). Changing the
  manifest does not change data already in a database: load the data again,
  or plan a schema migration with `graflo migrate-schema`.

## What to read next

- [Examples](../examples/index.md): a manifest for each shape of data, from
  self-references to nested JSON and routed rows.
- [Vertex identity](../concepts/schema/vertex_identity.md): every way to say
  what makes a vertex unique.
- [Transforms](../concepts/ingestion/transforms.md): reshaping records before
  they become vertices.
