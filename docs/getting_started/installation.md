# Installation

GraFlo is a Python package. This page installs it with the database clients you
need, and the extras for development, documentation and diagrams, and tells you
where to get a database to load into.

## What you need

- Python 3.12 or newer.
- A graph database to load into, unless you use Grafeo, which runs inside your
  Python process, or the file backend, which writes the graph to a directory. See [Get a database](#get-a-database) below.

## Install the package

Name the database you load into as an extra. With pip:

```bash
pip install "graflo[neo4j]"
```

With uv, into a project of yours:

```bash
uv add "graflo[neo4j]"
```

`graflo[all]` installs every database client and source integration.

From a clone of the repository, which also gives you the examples and the
database containers:

```bash
git clone https://github.com/growgraph/graflo.git
cd graflo
uv sync --extra all
```

## Databases and sources

A plain `pip install graflo` holds the manifest, the ingestion pipeline, file,
REST API and RDF file sources, the file backend and TigerGraph, whose client is
plain HTTP. Every other database and source needs its extra; without it,
connecting raises an error naming the extra to install.

| Extra | What it adds |
|-------|--------------|
| `arango` | ArangoDB |
| `neo4j` | Neo4j |
| `falkordb` | FalkorDB |
| `memgraph` | Memgraph |
| `nebula` | NebulaGraph 3.x and 5.x |
| `postgres` | PostgreSQL as a target |
| `grafeo` | Grafeo, an embedded database that runs inside your Python process |
| `sql` | SQL databases as a source (SQLAlchemy, with the PostgreSQL driver) |
| `kafka` | Kafka topics as a source |
| `s3` | S3-compatible object storage |
| `sparql` | SPARQL endpoints as a source |
| `all` | Every extra above |

## Optional tooling

These extras add tooling; they do not switch ingestion features on or off.

| Extra | What it adds |
|-------|--------------|
| `bulk` | Bulk loading into an empty target: DuckDB and Zstandard for the staging files |
| `dev` | Tests and checks: `pytest` and its plugins, `hypothesis`, `ty`, `pre-commit` |
| `docs` | Building this site: ProperDocs and its plugins |
| `plot` | Drawing the diagrams of `graflo plot-manifest` and the `--plot` figures of `graflo merge` and `graflo merge3`, as SVG, PDF or PNG |

With pip, name the extras you want:

```bash
pip install "graflo[neo4j,plot]"
pip install "graflo[dev,docs,plot]"
```

`dev` includes `all` and `bulk`. From a clone, name every extra you want in one
command, because `uv sync` removes the extras you leave out:

```bash
uv sync --extra dev --extra docs --extra plot
```

The `plot` extra needs no system packages: Graphviz runs as WebAssembly inside
Python, and PDF and PNG text uses fonts shipped with graflo, so it works the
same on a desktop and in a slim container image. Those fonts cover Latin, Greek
and Cyrillic; other scripts show in SVG, which leaves text to the viewer's fonts.
On Alpine, install `libgcc` (`apk add libgcc`) for the WebAssembly runtime.
[Drawing a manifest](../guides/drawing_a_manifest.md) explains the figures.

## Check the installation

```bash
graflo --version
```

```text
graflo, version 1.15.1
```

The version you see is the one you installed. In a clone, run it as
`uv run graflo --version`.

## Get a database

GraFlo writes to ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph,
PostgreSQL and Grafeo. A clone of the repository has a Docker Compose setup for
each server among them, plus Apache Fuseki and Kafka as sources to read from and
SeaweedFS as object storage.
[`docker/README.md`](https://github.com/growgraph/graflo/blob/main/docker/README.md)
explains how to start one or all of them; the examples read their connection
settings from those containers.

If you do not want to run a database server, use Grafeo or the file backend.
Grafeo is a graph database that runs inside your Python process, from a file or
from memory, with nothing to install but `graflo[grafeo]`. The file backend
writes the graph to a directory, and you can load it into a database later. The
[file backend example (14)](../examples/file-backend-export/index.md) shows
how.

## What to read next

- [Quick start](quickstart.md): two CSV files into a graph, step by step.
- [Database connections](../guides/database_connections.md): how to point
  GraFlo at a database of your own.
