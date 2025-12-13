# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

graflo is a framework for transforming **tabular** data (CSV) and **hierarchical** data (JSON, XML) into property graphs and ingesting them into graph databases (ArangoDB, Neo4j, TigerGraph).

![Python](https://img.shields.io/badge/python-3.10-blue.svg) 
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
The Schema defines how your data should be transformed into a graph and contains:

- **Vertex Definitions**: Specify vertex types, their properties, and unique identifiers
  - Fields can be specified as strings (backward compatible) or typed `Field` objects with types (INT, FLOAT, STRING, DATETIME, BOOL)
  - Type information enables better validation and database-specific optimizations
- **Edge Definitions**: Define relationships between vertices and their properties
  - Weight fields support typed definitions for better type safety
- **Resource Mapping**: describe how data sources map to vertices and edges
- **Transforms**: Modify data during the casting process
- **Automatic Schema Inference**: Generate schemas automatically from PostgreSQL 3NF databases

### Data Sources
Data Sources define where data comes from:

- **File Sources**: JSON, JSONL, CSV/TSV files
- **API Sources**: REST API endpoints with pagination and authentication
- **SQL Sources**: SQL databases via SQLAlchemy
- **In-Memory Sources**: Python objects (lists, DataFrames)

### Resources
Resources define how data is transformed into a graph (semantic mapping). They work with data from any DataSource type:

- **Table-like processing**: CSV files, SQL tables, API responses
- **JSON-like processing**: JSON files, nested data structures, hierarchical API responses

## Key Features

- **Graph Transformation Meta-language**: A powerful declarative language to describe how your data becomes a property graph:
    - Define vertex and edge structures with typed fields
    - Set compound indexes for vertices and edges
    - Use blank vertices for complex relationships
    - Specify edge constraints and properties with typed weight fields
    - Apply advanced filtering and transformations
- **Typed Schema Definitions**: Enhanced type support throughout the schema system
    - Vertex fields support types (INT, FLOAT, STRING, DATETIME, BOOL) for better validation
    - Edge weight fields can specify types for improved type safety
    - Backward compatible: fields without types default to None (suitable for databases like ArangoDB)
- **PostgreSQL Schema Inference**: Automatically generate schemas from PostgreSQL 3NF databases
    - Introspect PostgreSQL schemas to identify vertex-like and edge-like tables
    - Automatically map PostgreSQL data types to graflo Field types
    - Infer vertex configurations from table structures
    - Infer edge configurations from foreign key relationships
    - Create Resource mappings from PostgreSQL tables
- **Parallel Processing**: Efficient processing with multi-threading
- **Database Integration**: Seamless integration with Neo4j, ArangoDB, TigerGraph, and PostgreSQL (as source)
- **Advanced Filtering**: Powerful filtering capabilities for data transformation with server-side filtering support
- **Blank Node Support**: Create intermediate vertices for complex relationships

## Quick Links

- [Installation](getting_started/installation.md)
- [Quick Start Guide](getting_started/quickstart.md)
- [API Reference](reference/index.md)
- [Examples](examples/index.md)

## Use Cases

- **Data Migration**: Transform relational data into graph structures
- **Knowledge Graphs**: Build complex knowledge representations
- **Data Integration**: Combine multiple data sources into a unified graph

## Requirements

- Python 3.10 or higher
- Graph database (Neo4j, ArangoDB, or TigerGraph) for storage
- Optional: PostgreSQL or other SQL databases for data sources
- Dependencies as specified in pyproject.toml

## Contributing

We welcome contributions! Please check out our [Contributing Guide](contributing.md) for details on how to get started.
