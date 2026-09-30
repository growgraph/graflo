---
hide:
- navigation
- toc
---

<div class="gf-hero" markdown>
<div class="gf-hero__text" markdown>

# GraFlo

GraFlo is a Python library that turns records from files, SQL databases, RDF,
REST APIs or Kafka topics into a labeled property graph.
{ .gf-lead }

You describe the graph once, in a YAML file called a manifest, and GraFlo
creates the schema and writes the vertices and edges into the graph database of
your choice, or into a directory on disk.

It is for engineers who build a graph from several sources and want its
description in one reviewable file rather than spread across load scripts.

```bash
pip install graflo
```

[Quick start](getting_started/quickstart.md){ .md-button .md-button--primary }
[Examples](examples/index.md){ .md-button }

</div>
<figure class="gf-flow">
<svg viewBox="0 0 400 330" role="img" aria-labelledby="gf-flow-title" xmlns="http://www.w3.org/2000/svg">
<title id="gf-flow-title">Records from files, SQL databases, RDF, REST APIs and Kafka topics pass through one manifest into ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, PostgreSQL or the file backend.</title>
<g class="gf-flow__edge">
<path d="M118,65 C150,65 140,165 168,165"/>
<path d="M118,115 C150,115 140,165 168,165"/>
<path d="M118,165 L168,165"/>
<path d="M118,215 C150,215 140,165 168,165"/>
<path d="M118,265 C150,265 140,165 168,165"/>
<path d="M232,165 C260,165 250,25 282,25"/>
<path d="M232,165 C260,165 250,65 282,65"/>
<path d="M232,165 C260,165 250,105 282,105"/>
<path d="M232,165 C260,165 250,145 282,145"/>
<path d="M232,165 C260,165 250,185 282,185"/>
<path d="M232,165 C260,165 250,225 282,225"/>
<path d="M232,165 C260,165 250,265 282,265"/>
<path d="M232,165 C260,165 250,305 282,305"/>
</g>
<g class="gf-flow__node">
<circle cx="112" cy="65" r="6"/>
<circle cx="112" cy="115" r="6"/>
<circle cx="112" cy="165" r="6"/>
<circle cx="112" cy="215" r="6"/>
<circle cx="112" cy="265" r="6"/>
<circle class="gf-flow__hub" cx="200" cy="165" r="32"/>
<circle cx="288" cy="25" r="6"/>
<circle cx="288" cy="65" r="6"/>
<circle cx="288" cy="105" r="6"/>
<circle cx="288" cy="145" r="6"/>
<circle cx="288" cy="185" r="6"/>
<circle cx="288" cy="225" r="6"/>
<circle cx="288" cy="265" r="6"/>
<circle cx="288" cy="305" r="6"/>
</g>
<g text-anchor="end">
<text x="98" y="69.5">Files</text>
<text x="98" y="119.5">SQL databases</text>
<text x="98" y="169.5">RDF</text>
<text x="98" y="219.5">REST APIs</text>
<text x="98" y="269.5">Kafka topics</text>
</g>
<text class="gf-flow__hub-label" x="200" y="169" text-anchor="middle">manifest</text>
<g>
<text x="302" y="29.5">ArangoDB</text>
<text x="302" y="69.5">Neo4j</text>
<text x="302" y="109.5">TigerGraph</text>
<text x="302" y="149.5">FalkorDB</text>
<text x="302" y="189.5">Memgraph</text>
<text x="302" y="229.5">NebulaGraph</text>
<text x="302" y="269.5">PostgreSQL</text>
<text x="302" y="309.5">File backend</text>
</g>
</svg>
</figure>
</div>

## What you can do with it

<div class="gf-columns" markdown>
<div markdown>

### Describe a graph once and load data into it

A manifest names the vertex and edge types, says which properties identify a
vertex, and says how each kind of record becomes vertices and edges. The same
manifest loads into ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph,
NebulaGraph, PostgreSQL or the file backend, and records with the same identity
become one vertex. GraFlo also copies an existing graph from Neo4j, ArangoDB or
PostgreSQL into another database.

</div>
<div markdown>

### Change the description over time, with a recorded history

Renaming a type, combining two types or changing a property type is a typed
operation. Operations are recorded as commits that you can replay, check and,
for most operations, undo. Two branches of changes to one manifest are
reconciled with a three-way merge, and two manifests written by different teams
are combined into one with a union.

</div>
<div markdown>

### Check and infer descriptions

GraFlo infers a manifest from a PostgreSQL database or an OWL ontology,
proposes the properties that identify a record from sample data, and checks a
manifest against a conformance profile, a set of modeling rules such as "every
vertex type declares its identity".

</div>
</div>

## A taste

<div class="gf-taste" markdown>
<div markdown>

A manifest has three blocks: `schema` says what the graph looks like,
`ingestion_model` says how records map onto it, and `bindings` says where the
records come from. This one reads CSV files with the columns `person_id`,
`person` and `department`:

</div>
<div markdown>

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

</div>
<div markdown>

This loads it into ArangoDB:

</div>
<div markdown>

```python
from graflo import GraphEngine, GraphManifest
from graflo.connections import ArangoConfig

manifest = GraphManifest.from_yaml("manifest.yaml")
manifest.finish_init()
engine = GraphEngine()
engine.define_and_ingest(manifest=manifest, target_db_config=ArangoConfig.from_env())
```

</div>
</div>

## What to read next

<div class="grid cards gf-next" markdown>

-   **[Installation](getting_started/installation.md)**

    Install the package and get a database to load into.

-   **[Quick start](getting_started/quickstart.md)**

    Two CSV files into a graph, step by step.

-   **[Creating a manifest](getting_started/creating_manifest.md)**

    The three blocks of a manifest, one level deeper.

-   **[Examples](examples/index.md)**

    Runnable examples, one question each.

</div>
