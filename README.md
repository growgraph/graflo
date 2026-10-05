# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/refs/heads/main/docs/assets/project_logo.png" alt="GraFlo logo" height="32" width="32"/>

**Describe a property graph once in YAML; load it from files, SQL, RDF, APIs or Kafka into the database of your choice.**

[![tests](https://img.shields.io/github/actions/workflow/status/growgraph/graflo/tests.yml?branch=main&label=tests)](https://github.com/growgraph/graflo/actions/workflows/tests.yml)
[![pre-commit](https://img.shields.io/github/actions/workflow/status/growgraph/graflo/pre-commit.yml?branch=main&label=pre-commit)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![PyPI](https://img.shields.io/pypi/v/graflo?color=224777)](https://pypi.org/project/graflo/)
[![Python](https://img.shields.io/pypi/pyversions/graflo?color=224777)](https://pypi.org/project/graflo/)
[![Downloads](https://img.shields.io/pepy/dt/graflo?color=224777)](https://pepy.tech/projects/graflo)
[![Docs](https://img.shields.io/badge/docs-growgraph.github.io-224777)](https://growgraph.github.io/graflo/)
[![License](https://img.shields.io/pypi/l/graflo?color=224777)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![DOI](https://img.shields.io/badge/DOI-10.5281%2Fzenodo.15446131-224777)](https://doi.org/10.5281/zenodo.15446131)

GraFlo is a Python library that turns records from files, SQL databases, RDF,
REST APIs or Kafka topics into a labeled property graph. You describe the graph
once, in a YAML file called a manifest, and GraFlo creates the schema and
writes the vertices and edges into the graph database of your choice, or into
a directory on disk.

It is for engineers who build a graph from several sources and want its
description in one reviewable file rather than spread across load scripts.

**Documentation:** [growgraph.github.io/graflo](https://growgraph.github.io/graflo/)

## What you can do with it

- **Describe a graph once and load data into it.** A manifest names the vertex
  and edge types, says which properties identify a vertex, and says how each
  kind of record becomes vertices and edges. The same manifest loads into
  ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, PostgreSQL or
  the file backend, and records with the same identity become one vertex.
  GraFlo also copies an existing graph from Neo4j, ArangoDB or PostgreSQL into
  another database (`GraphEngine.migrate_graph`).
- **Change the description over time, with a recorded history.** Renaming a
  type, combining two types or changing a property type is a typed operation.
  Operations are recorded as commits (`graflo commit`, `log`, `checkout`,
  `verify`, `revert`) that you can replay, check and, for most operations,
  undo. Two
  branches of changes to one manifest are reconciled with a three-way merge
  (`graflo merge3`), and two manifests written by different teams are combined
  into one with a union (`graflo merge`).
- **Check and infer descriptions.** GraFlo infers a manifest from a PostgreSQL
  database or an OWL ontology, proposes the properties that identify a record
  from sample data, and checks a manifest against a conformance profile
  (`graflo check`), a set of modeling rules such as "every vertex type
  declares its identity".

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

`ArangoConfig.from_env()` reads `ARANGO_URI`, `ARANGO_USERNAME`,
`ARANGO_PASSWORD` and `ARANGO_DATABASE`; every database has such a class. See
[Database connections](https://growgraph.github.io/graflo/guides/database_connections/).

## Documentation

Full documentation: [growgraph.github.io/graflo](https://growgraph.github.io/graflo)

- [Quick start](https://growgraph.github.io/graflo/getting_started/quickstart/): two CSV files into a graph, step by step
- [Creating a manifest](https://growgraph.github.io/graflo/getting_started/creating_manifest/): the three blocks of a manifest
- [Examples](https://growgraph.github.io/graflo/examples/): runnable examples, one question each, with their data under [`examples/`](https://github.com/growgraph/graflo/tree/main/examples)
- [Concepts](https://growgraph.github.io/graflo/concepts/): schema, identity, ingestion, connectors, evolution and version control
- [Guides](https://growgraph.github.io/graflo/guides/): database connections, graph migration, schema inference, API wiring, bulk load
- [GraFlo ontology](https://growgraph.github.io/graflo/concepts/schema/ontology/): a manifest as RDF (`graflo manifest-to-rdf`, `graflo rdf-to-manifest`)

## Installation

GraFlo needs Python 3.11 or newer. The database clients, RDF and Kafka support
are part of the default install.

```bash
pip install graflo
```

Optional extras (see the
[Installation](https://growgraph.github.io/graflo/getting_started/installation/) guide):

- `dev`: pytest and its plugins, hypothesis, ty, pre-commit
- `docs`: ProperDocs and its plugins, for building the documentation site
- `plot`: draws `graflo plot-manifest` and the `--plot` figures of `graflo merge`
  and `graflo merge3` (SVG, PDF, PNG); no system Graphviz or fonts needed

```bash
pip install "graflo[dev,docs,plot]"
```

## Development

To install from a clone:

```shell
git clone git@github.com:growgraph/graflo.git && cd graflo
uv sync --extra dev
```

See the [Contributing Guide](https://growgraph.github.io/graflo/contributing/) for the full workflow.

### Tests

The database tests need the database containers. Start them from a clone with
the scripts under [docker/](https://github.com/growgraph/graflo/tree/main/docker):

```shell
cd docker
./start-all.sh    # Start all services
./stop-all.sh     # Stop all services
./cleanup-all.sh  # Remove containers and volumes
```

Per-engine compose files and ports are documented in the
[docker README](https://github.com/growgraph/graflo/blob/main/docker/README.md).

To run the tests:

```shell
uv run pytest test
```

TigerGraph, NebulaGraph and Kafka tests are skipped unless you pass
`--run-tigergraph`, `--run-nebula` or `--run-kafka`.

The suites that need no database run without the containers, and CI runs them
on every pull request:

```shell
uv run pytest test --ignore=test/db --ignore=test/data_source --ignore=test/object_storage
```

## License

Open source under the [Apache License 2.0](https://github.com/growgraph/graflo/blob/main/LICENSE).
Copyright and trademark notices are in
[NOTICE](https://github.com/growgraph/graflo/blob/main/NOTICE): the license grants no rights in the
**GraFlo** and **GrowGraph** marks. Releases before the relicensing shipped under the Business
Source License 1.1 and keep those terms; see the
[changelog](https://github.com/growgraph/graflo/blob/main/CHANGELOG.md).

## Contributing

Contributions are welcome. See the
[Contributing Guide](https://growgraph.github.io/graflo/contributing/). Contributors accept the
[Contributor License Agreement](https://github.com/growgraph/graflo/blob/main/CLA.md) once, by
commenting on their first pull request.
