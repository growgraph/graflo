# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

GraFlo is a **Graph Schema & Transformation Language (GSTL)** for Labeled Property Graphs (LPG).

It provides a declarative, database-agnostic specification for mapping heterogeneous data sources — tabular (CSV, SQL), hierarchical (JSON, XML), and RDF/SPARQL — to a unified LPG representation. A `Resource` abstraction decouples transformation logic from data retrieval; a `GraphContainer` (covariant graph representation) abstracts away database idiosyncrasies; and a `DataSourceRegistry` manages source adapters so that files, SQL tables, REST APIs, and SPARQL endpoints plug into the same pipeline. Supported targets: ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph.

![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)

<!-- [![pytest](https://github.com/growgraph/graflo/actions/workflows/pytest.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pytest.yml) -->


## Pipeline

**Source Instance** → **Resource** → **Graph Schema** → **Covariant Graph Representation** → **Graph DB**

| Stage | Role | Code |
|-------|------|------|
| **Source Instance** | A concrete data artifact — a CSV file, a PostgreSQL table, a SPARQL endpoint, a `.ttl` file. | `AbstractDataSource` subclasses with a `DataSourceType` (`FILE`, `SQL`, `SPARQL`, `API`, `IN_MEMORY`). |
| **Resource** | A reusable transformation pipeline — actor steps (descend, transform, vertex, edge) that map raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`. | `Resource` (part of `Schema`). |
| **Graph Schema** | Declarative vertex/edge definitions, indexes, typed fields, and named transforms. | `Schema`, `VertexConfig`, `EdgeConfig`. |
| **Covariant Graph Representation** | A database-independent collection of vertices and edges. | `GraphContainer`. |
| **Graph DB** | The target LPG store — same API for all supported databases. | `ConnectionManager`, `DBWriter`. |

## Core Concepts

### Labeled Property Graphs

GraFlo targets the LPG model:

- **Vertices** — nodes with typed properties and unique identifiers.
- **Edges** — directed relationships between vertices, carrying their own properties and weights.

### Schema

The Schema is the single source of truth for the graph structure:

- **Vertex definitions** — vertex types, fields (optionally typed: `INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`), and indexes.
- **Edge definitions** — relationships between vertex types, with optional weight fields.
- **Resources** — reusable actor pipelines that map raw records to vertices and edges (see below).
- **Transforms** — named data transformations referenced by Resources.
- **Schema inference** — generate schemas from PostgreSQL 3NF databases (PK/FK heuristics) or from OWL/RDFS ontologies.

### Resource

A `Resource` is the central abstraction that bridges data sources and the graph schema. Each Resource defines a reusable pipeline of actors (descend, transform, vertex, edge) that maps raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`, so the same transformation logic applies regardless of whether data arrives from a file, an API, or a SPARQL endpoint.

### DataSourceRegistry

The `DataSourceRegistry` manages `AbstractDataSource` adapters, each carrying a `DataSourceType`:

| `DataSourceType` | Adapter | Sources |
|---|---|---|
| `FILE` | `FileDataSource` | CSV, JSON, JSONL, Parquet files |
| `SQL` | `SQLDataSource` | PostgreSQL and other SQL databases via SQLAlchemy |
| `SPARQL` | `RdfFileDataSource` | Turtle/RDF/N3/JSON-LD files via rdflib |
| `SPARQL` | `SparqlEndpointDataSource` | Remote SPARQL endpoints (e.g. Apache Fuseki) via SPARQLWrapper |
| `API` | `APIDataSource` | REST API endpoints with pagination and authentication |
| `IN_MEMORY` | `InMemoryDataSource` | Python objects (lists, DataFrames) |

### GraphEngine

`GraphEngine` orchestrates end-to-end operations: schema inference, schema definition in the target database, pattern creation from data sources, and data ingestion.

## Key Features

- **Declarative LPG schema** — Define vertices, edges, indexes, weights, and transforms in YAML or Python. The `Schema` is the single source of truth, independent of source or target.
- **Database abstraction** — One schema, one API. Target ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph without rewriting pipelines. Database idiosyncrasies are handled by the `GraphContainer` (covariant graph representation).
- **Resource abstraction** — Each `Resource` defines a reusable actor pipeline that maps raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`, decoupling transformation logic from data retrieval.
- **DataSourceRegistry** — Register `FILE`, `SQL`, `API`, `IN_MEMORY`, or `SPARQL` data sources. Each `DataSourceType` plugs into the same Resource pipeline.
- **SPARQL & RDF support** — Query SPARQL endpoints (e.g. Apache Fuseki), read `.ttl`/`.rdf`/`.n3` files, and auto-infer schemas from OWL/RDFS ontologies. Install with `pip install graflo[sparql]`.
- **Schema inference** — Generate graph schemas from PostgreSQL 3NF databases (PK/FK heuristics) or from OWL/RDFS ontologies. See [Example 5](examples/example-5.md).
- **Typed fields** — Vertex fields and edge weights carry types for validation and database-specific optimisation.
- **Parallel batch processing** — Configurable batch sizes and multi-core execution.
- **Advanced filtering** — Server-side filtering (e.g. TigerGraph REST++ API) and client-side filter expressions.
- **Blank vertices** — Create intermediate nodes for complex relationship modelling.

## Quick Links

- [Installation](getting_started/installation.md)
- [Quick Start Guide](getting_started/quickstart.md)
- [API Reference](reference/index.md)
- [Examples](examples/index.md)

## Use Cases

- **Data Migration** — Transform relational data into LPG structures. Infer schemas from PostgreSQL 3NF databases and migrate data directly.
- **RDF-to-LPG** — Read RDF triples from files or SPARQL endpoints, auto-infer schemas from OWL ontologies, and ingest into ArangoDB, Neo4j, etc.
- **Knowledge Graphs** — Build knowledge representations from heterogeneous sources (SQL, files, APIs, RDF/SPARQL).
- **Data Integration** — Combine multiple data sources into a unified labeled property graph.
- **Graph Views** — Create graph views of existing PostgreSQL databases without schema changes.

## Requirements

- Python 3.11 or higher (3.11 and 3.12 officially supported)
- A graph database (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph) as target
- Optional: PostgreSQL for SQL data sources and schema inference
- Optional: `rdflib` + `SPARQLWrapper` for RDF/SPARQL support (`pip install graflo[sparql]`)
- Full dependency list in `pyproject.toml`

## Contributing

We welcome contributions! Please check out our [Contributing Guide](contributing.md) for details on how to get started.
