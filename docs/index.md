# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

GraFlo is a Python library that turns records from files, SQL databases, RDF,
REST APIs or Kafka topics into a labeled property graph. You describe the graph
once, in a YAML file called a manifest, and GraFlo creates the schema and
writes the vertices and edges into the graph database of your choice, or into
a directory on disk.

It is for engineers who build a graph from several sources and want its
description in one reviewable file rather than spread across load scripts.

## What you can do with it

- **Describe a graph once and load data into it.** A manifest names the vertex
  and edge types, says which properties identify a vertex, and says how each
  kind of record becomes vertices and edges. The same manifest loads into
  ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, PostgreSQL or
  the file backend, and records with the same identity become one vertex.
  GraFlo also copies an existing graph from Neo4j, ArangoDB or PostgreSQL into
  another database.
- **Change the description over time, with a recorded history.** Renaming a
  type, combining two types or changing a property type is a typed operation.
  Operations are recorded as commits that you can replay, check and, for most
  operations, undo. Two branches of changes to one manifest are reconciled
  with a three-way merge, and two manifests written by different teams are
  combined into one with a union.
- **Check and infer descriptions.** GraFlo infers a manifest from a PostgreSQL
  database or an OWL ontology, proposes the properties that identify a record
  from sample data, and checks a manifest against a conformance profile, a set
  of modeling rules such as "every vertex type declares its identity".

## A taste

A manifest has three blocks: `schema` says what the graph looks like,
`ingestion_model` says how records map onto it, and `bindings` says where the
records come from. This one reads CSV files with the columns `person_id`,
`person` and `department`:

```yaml
schema:
    metadata: {name: hr}
    graph:
        vertex_config:
            vertices:
            -   {name: person, properties: [id, name], identity: [id]}
            -   {name: department, properties: [name], identity: [name]}
        edge_config:
            edges: [{source: person, target: department}]
ingestion_model:
    resources:
    -   name: departments
        pipeline:
        -   {vertex: person, from: {id: person_id, name: person}}
        -   {vertex: department, from: {name: department}}
bindings:
    connectors:
    -   {regex: "^dep.*\\.csv$", sub_path: data, resource_name: departments}
```

This loads it into ArangoDB:

```python
from graflo import GraphEngine, GraphManifest
from graflo.connections import ArangoConfig

manifest = GraphManifest.from_yaml("manifest.yaml")
manifest.finish_init()
engine = GraphEngine()
engine.define_and_ingest(manifest=manifest, target_db_config=ArangoConfig.from_env())
```

## What to read next

- [Installation](getting_started/installation.md): install the package and get
  a database to load into.
- [Quick start](getting_started/quickstart.md): two CSV files into a graph,
  step by step.
- [Creating a manifest](getting_started/creating_manifest.md): the three
  blocks of a manifest, one level deeper.
- [Examples](examples/index.md): runnable examples, one question each.
