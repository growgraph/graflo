# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

A framework for transforming **tabular** (CSV, SQL) and **hierarchical** data (JSON, XML) into property graphs and ingesting them into graph databases (ArangoDB, Neo4j, **TigerGraph**).

> **⚠️ Package Renamed**: This package was formerly known as `graphcast`.

![Python](https://img.shields.io/badge/python-3.11-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)

## Core Concepts

### Property Graphs
graflo works with property graphs, which consist of:

- **Vertices**: Nodes with properties and optional unique identifiers
- **Edges**: Relationships between vertices with their own properties
- **Properties**: Both vertices and edges may have properties

### Schema
The Schema defines how your data should be transformed into a graph and contains:

- **Vertex Definitions**: Specify vertex types, their properties, and unique identifiers
- **Edge Definitions**: Define relationships between vertices and their properties
- **Resource Mapping**: describe how data sources map to vertices and edges
- **Transforms**: Modify data during the casting process

### Resources
Resources are your data sources that can be:

- **Table-like**: CSV files, database tables
- **JSON-like**: JSON files, nested data structures

## Features

- **Graph Transformation Meta-language**: A powerful declarative language to describe how your data becomes a property graph:
    - Define vertex and edge structures
    - Set compound indexes for vertices and edges
    - Use blank vertices for complex relationships
    - Specify edge constraints and properties
    - Apply advanced filtering and transformations
- **Parallel processing**: Use as many cores as you have
- **Database support**: Ingest into ArangoDB, Neo4j, and **TigerGraph** using the same API (database agnostic)
- **Server-side filtering**: Efficient querying with server-side filtering support (TigerGraph REST++ API)

## Documentation
Full documentation is available at: [growgraph.github.io/graflo](https://growgraph.github.io/graflo)

## Installation

```bash
pip install graflo
```

## Usage Examples

### Simple ingest

```python
from suthing import ConfigFactory, FileHandle

from graflo import Schema, Caster, Patterns


schema = Schema.from_dict(FileHandle.load("schema.yaml"))

conn_conf = ConfigFactory.create_config({
        "protocol": "http",
        "hostname": "localhost",
        "port": 8535,
        "username": "root",
        "password": "123",
        "database": "_system",
}
)

patterns = Patterns.from_dict(
    {
        "patterns": {
            "work": {"regex": "\Sjson$"},
        }
    }
)

schema.fetch_resource()

caster = Caster(
    schema,
)

caster.ingest_files(
    path="./data",
    conn_conf=conn_conf,
    patterns=patterns,
)
```

## Development

To install requirements

```shell
git clone git@github.com:growgraph/graflo.git && cd graflo
uv sync --dev
```

### Tests

#### Test databases
Spin up Arango from [arango docker folder](./docker/arango) by

```shell
docker-compose --env-file .env up arango
```

Neo4j from [neo4j docker folder](./docker/neo4j) by

```shell
docker-compose --env-file .env up neo4j
```

and TigerGraph from [tigergraph docker folder](./docker/tigergraph) by

```shell
docker-compose --env-file .env up tigergraph
```

To run unit tests

```shell
pytest test
```

## Requirements

- Python 3.11+
- python-arango

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.