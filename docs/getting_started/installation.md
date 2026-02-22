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

2. Install with development dependencies:
```bash
uv sync --group dev
```

## Optional Dependencies

graflo has optional dependency groups that can be installed based on your needs.

### RDF / SPARQL support

Adds `rdflib` and `SPARQLWrapper` for reading `.ttl`/`.rdf`/`.n3` files and querying SPARQL endpoints:

```bash
pip install graflo[sparql]
```

### Schema visualisation

For generating schema visualisations with graphviz (requires the system `graphviz` package, e.g. `apt install graphviz-dev`):

```bash
pip install graflo[graphviz]
```

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