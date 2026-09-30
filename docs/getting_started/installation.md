# Installation

GraFlo is a Python package. This page installs it, with or without the extras
for development, documentation and diagrams, and tells you where to get a
database to load into.

## What you need

- Python 3.11 or newer.
- A graph database to load into, unless you use the file backend, which writes
  the graph to a directory. See [Get a database](#get-a-database) below.

## Install the package

With pip:

```bash
pip install graflo
```

With uv, into a project of yours:

```bash
uv add graflo
```

From a clone of the repository, which also gives you the examples and the
database containers:

```bash
git clone https://github.com/growgraph/graflo.git
cd graflo
uv sync
```

The default install includes the clients for every supported database, RDF
and SPARQL support, and the Kafka client.

## Optional extras

The extras add tooling only; they do not switch ingestion features on or off.

| Extra | What it adds |
|-------|--------------|
| `dev` | Tests and checks: `pytest` and its plugins, `hypothesis`, `ty`, `pre-commit` |
| `docs` | Building this site: ProperDocs and its plugins |
| `plot` | `pygraphviz`, which draws the diagrams of `graflo plot-manifest` and the `--plot` figures of `graflo merge` and `graflo merge3` |

With pip, name the extras you want:

```bash
pip install "graflo[plot]"
pip install "graflo[dev,docs,plot]"
```

From a clone, name every extra you want in one command, because `uv sync`
removes the extras you leave out:

```bash
uv sync --extra dev --extra docs --extra plot
```

The `plot` extra needs no system Graphviz: the `pygraphviz` wheels include it.

## Check the installation

```bash
graflo --version
```

```text
graflo, version 1.15.0
```

The version you see is the one you installed. In a clone, run it as
`uv run graflo --version`.

## Get a database

GraFlo writes to ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph
and PostgreSQL. A clone of the repository has a Docker Compose setup for each
of them, plus Apache Fuseki and Kafka as sources to read from and MinIO as
object storage.
[`docker/README.md`](https://github.com/growgraph/graflo/blob/main/docker/README.md)
explains how to start one or all of them; the examples read their connection
settings from those containers.

If you do not want to run a database yet, use the file backend: it writes the
graph to a directory, and you can load it into a database later. The
[file backend example (14)](../examples/file-backend-export/index.md) shows
how.

## What to read next

- [Quick start](quickstart.md): two CSV files into a graph, step by step.
- [Database connections](../guides/database_connections.md): how to point
  GraFlo at a database of your own.
