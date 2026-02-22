# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

graflo is a framework for transforming **tabular** (CSV, SQL), **hierarchical** (JSON, XML), and **RDF/SPARQL** data into property graphs and ingesting them into graph databases (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph).

![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)

<!-- [![pytest](https://github.com/growgraph/graflo/actions/workflows/pytest.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pytest.yml) -->


## Core Concepts

### Property Graphs
graflo works with property graphs, which consist of:

- **Vertices**: Nodes with properties and optional unique identifiers
- **Edges**: Relationships between vertices with their own properties
- **Properties**: Both vertices and edges may have properties

### Schema
The Schema defines how your data should be transformed into a graph:

- **Vertex Definitions**: Vertex types, their properties, and unique identifiers. Fields may carry optional types (`INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`).
- **Edge Definitions**: Relationships between vertices, with optional weight fields.
- **Resource Mapping**: How data sources map to vertices and edges.
- **Transforms**: Modify data during the casting process.
- **Automatic Schema Inference**: Generate schemas from PostgreSQL 3NF databases (PK/FK heuristics) or from OWL/RDFS ontologies.

### Data Sources
Data Sources define where data comes from:

- **File Sources**: CSV, JSON, JSONL, Parquet files
- **SQL Sources**: PostgreSQL and other SQL databases via SQLAlchemy
- **RDF Sources**: Local Turtle/RDF/N3/JSON-LD files via rdflib
- **SPARQL Sources**: Remote SPARQL endpoints (e.g. Apache Fuseki) via SPARQLWrapper
- **API Sources**: REST API endpoints with pagination and authentication
- **In-Memory Sources**: Python objects (lists, DataFrames)

### Resources
Resources define how data is transformed into a graph (semantic mapping). They work with data from any DataSource type:

- **Table-like processing**: CSV files, SQL tables, API responses
- **JSON-like processing**: JSON files, nested data structures
- **RDF processing**: Triples grouped by subject into flat documents

### GraphEngine
`GraphEngine` orchestrates graph database operations:

- Schema inference from PostgreSQL databases or RDF/OWL ontologies
- Schema definition in target graph databases
- Pattern creation from data sources
- Data ingestion with async support

## Key Features

- **Declarative graph transformation**: Define vertex/edge structures, indexes, weights, and transforms in YAML. Resources describe how each data source maps to vertices and edges.
- **PostgreSQL schema inference**: Automatically generate schemas from normalized PostgreSQL databases (3NF) with proper PK/FK constraints. See [Example 5](examples/example-5.md).
- **RDF / SPARQL ingestion**: Read `.ttl`/`.rdf`/`.n3` files or query SPARQL endpoints. Auto-infer schemas from OWL/RDFS ontologies: `owl:Class` maps to vertices, `owl:ObjectProperty` to edges, `owl:DatatypeProperty` to vertex fields. Install with `pip install graflo[sparql]`.
- **Typed fields**: Vertex fields and edge weights support types (`INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`) for validation and database-specific optimisation.
- **Parallel batch processing**: Configurable batch sizes and multi-core execution.
- **Database-agnostic target**: Single API for ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, and NebulaGraph.
- **Advanced filtering**: Server-side filtering (e.g. TigerGraph REST++ API) and client-side filter expressions.
- **Blank vertices**: Create intermediate nodes for complex relationship modelling.

## Quick Links

- [Installation](getting_started/installation.md)
- [Quick Start Guide](getting_started/quickstart.md)
- [API Reference](reference/index.md)
- [Examples](examples/index.md)

## Use Cases

- **Data Migration**: Transform relational data into graph structures. Infer schemas from PostgreSQL 3NF databases (PK/FK heuristics) and migrate data directly.
- **RDF-to-Property-Graph**: Read RDF triples from files or SPARQL endpoints, auto-infer schemas from OWL ontologies, and ingest into ArangoDB, Neo4j, etc.
- **Knowledge Graphs**: Build knowledge representations from heterogeneous sources (SQL, files, APIs, RDF).
- **Data Integration**: Combine multiple data sources into a unified property graph.
- **Graph Views**: Create graph views of existing PostgreSQL databases without schema changes.

## Requirements

- Python 3.11 or higher (3.11 and 3.12 officially supported)
- A graph database (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph) as target
- Optional: PostgreSQL for SQL data sources and schema inference
- Optional: `rdflib` + `SPARQLWrapper` for RDF/SPARQL support (`pip install graflo[sparql]`)
- Full dependency list in `pyproject.toml`

## Contributing

We welcome contributions! Please check out our [Contributing Guide](contributing.md) for details on how to get started.
