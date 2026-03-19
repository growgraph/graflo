# Installation

## Prerequisites

- Python 3.11+
- A graph database (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph) if you plan to use database features

## Installation Methods

### Using pip

```bash
pip install graflo
```

### Using uv (recommended)

```bash
uv add graflo
```

### From Source

1. Clone the repository:
```bash
git clone https://github.com/growgraph/graflo.git
cd graflo
```

2. Install with development dependencies (pytest, ty, pre-commit):

```bash
uv sync --extra dev
```

To build the documentation locally, add the `docs` extra:

```bash
uv sync --extra dev --extra docs
```

## Optional extras

The default package includes RDF/SPARQL support (`rdflib`, `SPARQLWrapper`) and graph database clients. Optional [project.optional-dependencies](https://docs.astral.sh/uv/concepts/projects/dependencies/#optional-dependencies) extras are **tooling only** (`dev`, `docs`, `plot`)—they do not toggle ingestion features:

| Extra | Purpose |
|-------|---------|
| `dev` | Development: `pytest`, `ty`, `pre-commit` |
| `docs` | Building this site: MkDocs and plugins |
| `plot` | `plot_manifest` / schema diagrams via `pygraphviz` |

### pip

```bash
pip install "graflo[dev]"
pip install "graflo[docs]"
pip install "graflo[plot]"
# combine as needed, e.g.:
pip install "graflo[dev,docs,plot]"
```

### uv

From another project:

```bash
uv add graflo
uv add "graflo[plot]"  # optional: plot_manifest
```

From a clone of this repository:

```bash
uv sync --extra dev
uv sync --extra plot
```

### `plot` extra (Graphviz)

Install the system Graphviz libraries first (e.g. Debian/Ubuntu: `apt install graphviz graphviz-dev`), then install `graflo[plot]`.

## Verifying Installation

To verify your installation, you can run:

```python
import graflo
print(graflo.__version__)
```


## Spinning up databases

Instructions on how to spin up ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, and Apache Fuseki as Docker containers using `docker compose` are provided here: [github.com/growgraph/graflo/docker](https://github.com/growgraph/graflo/tree/main/docker)

## Configuration

After installation, you may need to configure your graph database connection. See the [Quick Start Guide](quickstart.md) for details on setting up your environment.

For more detailed troubleshooting, refer to the [API Reference](../reference/index.md) or open an issue on GitHub. 