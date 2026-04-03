# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

**GraFlo** is a **Python package** and **manifest format** (`GraphManifest`: YAML **`schema`** + **`ingestion_model`** + **`bindings`**) for **labeled property graphs**. It is a **Graph Schema & Transformation Language (GSTL)**: you encode the LPG **once** at the logical layer (vertices, edges, typed **`properties`**, identity), express **how** records become graph elements with **`Resource`** actor pipelines, and **project** that model per backend before load. **`GraphEngine`** covers inference, DDL, and ingest; **`Caster`** focuses on batching records into a **`GraphContainer`** and **`DBWriter`**.

## Why GraFlo

- **DB-agnostic LPG** — The **logical schema** describes an LPG independent of ArangoDB, Neo4j, Cypher-family stores, TigerGraph, and so on. You do not fork your “graph design” per vendor; you fork only **projection** and connectors.
- **Expressive, composable transforms** — **`Resource`** pipelines chain **actors** (descend into nested data, apply named **transforms**, emit **vertices** and **edges**, route by type with **VertexRouter** / **EdgeRouter**). The same pipeline can be bound to CSV, PostgreSQL, SPARQL, or an API via **`Bindings`**.
- **Clear boundaries** — **`Schema`** is structure only. **`IngestionModel`** holds resources and shared transforms. **`Bindings`** map ingestion resource names to one or more **connectors** and optional **`conn_proxy`** labels—so manifests stay credential-free at rest.
- **Multi-target ingestion** — One code path and manifest can target **ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph**; backend quirks are handled in **DB-aware** types and writers, not in your logical model.

![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)

<!-- [![pytest](https://github.com/growgraph/graflo/actions/workflows/pytest.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pytest.yml) -->


## Pipeline

**Source Instance** → **Resource** (actor pipeline) → **Logical Graph Schema** → **Covariant Graph Representation** (`GraphContainer`) → **DB-aware Projection** → **Graph DB**

| Stage | Role | Code |
|-------|------|------|
| **Source Instance** | A concrete data artifact — a CSV file, a PostgreSQL table, a SPARQL endpoint, a `.ttl` file. | `AbstractDataSource` subclasses with a `DataSourceType` (`FILE`, `SQL`, `SPARQL`, `API`, `IN_MEMORY`). |
| **Resource** | A reusable transformation pipeline — actor steps (descend, transform, vertex, edge, vertex_router, edge_router) that map raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`. | `Resource` (part of `IngestionModel`). |
| **Graph Schema** | Declarative logical vertex/edge definitions, identities, typed **properties**, and DB profile. | `Schema`, `VertexConfig`, `EdgeConfig`. |
| **Covariant Graph Representation** | A database-independent collection of vertices and edges. | `GraphContainer`. |
| **DB-aware Projection** | Resolves DB-specific naming/default/index behavior from logical schema + `DatabaseProfile`. | `Schema.resolve_db_aware()`, `VertexConfigDBAware`, `EdgeConfigDBAware`. |
| **Graph DB** | The target LPG store — same API for all supported databases. | `ConnectionManager`, `DBWriter`, DB connectors. |

## Core Concepts

### Labeled Property Graphs

GraFlo targets the LPG model:

- **Vertices** — nodes with typed **properties** (manifest key: `properties`) and logical **identity** keys for upserts.
- **Edges** — directed relationships between vertices; relationship attributes are declared as **`properties`** on the logical edge (same list-of-names-or-`Field` shape as vertices).

### Schema

The Schema is the single source of truth for the graph structure:

- **Vertex definitions** — vertex types, **`properties`** (optionally typed: `INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`), identity, and filters; secondary indexes live under **`database_features`**.
- **Edge definitions** — source/target (and optional `relation`), **`properties`** for relationship payload, and optional **`identities`** for parallel-edge / MERGE semantics.
- **Schema inference** — generate schemas from PostgreSQL 3NF databases (PK/FK heuristics) or from OWL/RDFS ontologies.

Resources and transforms are part of `IngestionModel`, not `Schema`.

### IngestionModel

`IngestionModel` defines how source records are transformed into graph entities:

- **Resources** — reusable actor pipelines that map raw records to vertices and edges.
- **Transforms** — reusable named transforms referenced by resource steps.

### Resource

A `Resource` is the central abstraction that bridges data sources and the graph schema. Each Resource defines a reusable pipeline of actors (descend, transform, vertex, edge) that maps raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`, so the same transformation logic applies regardless of whether data arrives from a file, an API, or a SPARQL endpoint.

For wide rows with many empty or null columns, **`drop_trivial_input_fields`** (default `false`) removes only **top-level** keys whose value is `null` or `""` before the pipeline runs—no recursion into nested structures.

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

`GraphEngine` orchestrates end-to-end operations: schema inference, schema definition in the target database, connector creation from data sources, and data ingestion.

## Key Features

- **Declarative LPG schema DSL** — Define vertices, edges, indexes, edge **properties**, and transforms in YAML or Python. The `Schema` is the single source of truth, independent of source or target.
- **Database abstraction** — One logical schema and transformation DSL, one API. Target ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph without rewriting pipelines. DB idiosyncrasies are handled in DB-aware projection (`Schema.resolve_db_aware(...)`) and connector/writer stages.
- **Resource abstraction** — Each `Resource` defines a reusable actor pipeline that maps raw records to graph elements. Actor types include descend, transform, vertex, edge, plus **VertexRouter** and **EdgeRouter** for dynamic type-based routing (see [Concepts — Actor](concepts/index.md#actor)). Data sources bind to Resources by name via the `DataSourceRegistry`, decoupling transformation logic from data retrieval.
- **DataSourceRegistry** — Register `FILE`, `SQL`, `API`, `IN_MEMORY`, or `SPARQL` data sources. Each `DataSourceType` plugs into the same Resource pipeline.
- **SPARQL & RDF support** — Query SPARQL endpoints (e.g. Apache Fuseki), read `.ttl`/`.rdf`/`.n3` files, and auto-infer schemas from OWL/RDFS ontologies (`rdflib` and `SPARQLWrapper` are included in the default install).
- **Schema inference** — Generate graph schemas from PostgreSQL 3NF databases (PK/FK heuristics) or from OWL/RDFS ontologies. See [Example 5](examples/example-5.md).
- **Schema migration planning/execution** — Generate typed migration plans between schema versions, apply low-risk additive changes with risk gates, and track revision history via `migrate_schema`.
  - Compare `from` and `to` schemas before execution to preview structural deltas and blocked high-risk operations.
- **Typed properties** — Vertex and edge **`properties`** carry optional types for validation and database-specific optimisation.
- **Parallel batch processing** — Configurable batch sizes and multi-core execution.
- **Advanced filtering** — Server-side filtering (e.g. TigerGraph REST++ API), client-side filter expressions, and **SelectSpec** for declarative SQL view/filter control before data reaches Resources.
- **Blank vertices** — Create intermediate nodes for complex relationship modelling.

## Quick Links

- [Installation](getting_started/installation.md)
- [Quick Start Guide](getting_started/quickstart.md)
- [Concepts (architecture diagrams)](concepts/index.md)
- [Concepts — Schema Migration](concepts/index.md#schema-migration-v1)
- [Concepts — Comparing Two Schemas](concepts/index.md#comparing-two-schemas)
- [API Reference](reference/index.md)
- [Examples](examples/index.md)

> Note: Mermaid diagrams are kept in section pages (for example `concepts/`) rather than on this landing page.

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
- Optional extras (see [Installation](getting_started/installation.md)): `dev` (tests and typing), `docs` (MkDocs), `plot` (`plot_manifest` via `pygraphviz`; system Graphviz required)
- Full dependency list in `pyproject.toml`

## Contributing

We welcome contributions! Please check out our [Contributing Guide](contributing.md) for details on how to get started.
