# GraFlo — Graph Schema & Transformation Language (GSTL) <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)

**GraFlo** is a manifest-driven toolkit for **labeled property graphs (LPGs)**: describe vertices, edges, and ingestion (`GraphManifest` — YAML or Python), then project and load into a target graph database.

It is a **Python package** and **Graph Schema & Transformation Language (GSTL)**. **`GraphEngine`** covers inference, DDL, and ingest; **`Caster`** focuses on batching records into a **`GraphContainer`** and **`DBWriter`**.

### What you get

- **One pipeline, several graph databases** — The same manifest targets ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph; `DatabaseProfile` and DB-aware types absorb naming, defaults, and indexing differences.
- **Explicit identities** — Vertex identity fields and indexes back upserts so reloads merge on keys instead of blindly duplicating nodes.
- **Reusable ingestion** — `Resource` actor pipelines (including **vertex_router** / **edge_router** steps and the **`VertexRouterActor`** / **`EdgeRouterActor`** implementations) bind to files, SQL, SPARQL/RDF, APIs, or in-memory batches via `Bindings` and the `DataSourceRegistry`.
- **Manifest-first sanitization** — `Sanitizer` normalizes schema identifiers (reserved words, TigerGraph relation/index constraints) and synchronizes related ingestion mappings via `sanitize_manifest(GraphManifest)`.

### What’s in the manifest

- **`schema`** — `Schema`: metadata, **`core_schema`** (vertices, edges, typed **`properties`**, identities), and **`db_profile`** (`DatabaseProfile`: target flavor, storage names, secondary indexes, TigerGraph `default_property_values`, …).
- **`ingestion_model`** — `IngestionModel`: named **`resources`** (actor sequences: *descend*, *transform*, *vertex*, *edge*, …) and a registry of reusable **`transforms`**.
- **`bindings`** — Connectors (e.g. `FileConnector`, `TableConnector`, `SparqlConnector`) plus **`resource_connector`** wiring. Optional **`connector_connection`** maps connectors to **`conn_proxy`** labels so YAML stays secret-free; a runtime **`ConnectionProvider`** supplies credentials.

### Runtime path

1. **Source instance** — Batches from a `DataSourceType` adapter (`FileDataSource`, `SQLDataSource`, `SparqlEndpointDataSource`, `APIDataSource`, …).
2. **Resource (actors)** — Maps records to graph elements against the logical schema (validated during `IngestionModel.finish_init` / pipeline execution).
3. **`GraphContainer`** — Intermediate, database-agnostic vertex/edge batches.
4. **DB-aware projection** — `Schema.resolve_db_aware()` plus `VertexConfigDBAware` / `EdgeConfigDBAware` for the active `DBType`.
5. **Graph DB** — `DBWriter` + `ConnectionManager` and the backend-specific `Connection` implementation.

| Piece | Role | Code |
|-------|------|------|
| **Logical graph schema** | Manifest `schema`: vertex/edge definitions, identities, typed **properties**, DB profile. Constrains pipeline output and projection; not a separate queue between steps. | `Schema`, `VertexConfig`, `EdgeConfig` (under `core_schema`). |
| **Source instance** | Concrete input: file, SQL table, SPARQL endpoint, API payload, in-memory rows. | `AbstractDataSource` + `DataSourceType`. |
| **Resource** | Ordered actors; resources are looked up by name when sources are registered. | `Resource` in `IngestionModel`. |
| **Covariant graph** (`GraphContainer`) | Batches of vertices/edges before load. | `GraphContainer`. |
| **DB-aware projection** | Physical names, defaults, indexes for the target. | `Schema.resolve_db_aware()`, `VertexConfigDBAware`, `EdgeConfigDBAware`. |
| **Graph DB** | Target LPG; each `DBType` has its own connector, orchestrated the same way. | `ConnectionManager`, `DBWriter`, per-backend `Connection`. |

### Supported source types (`DataSourceType`)

| DataSourceType | Adapter | DataSource | Schema inference |
|---|---|---|---|
| `FILE` — CSV / JSON / JSONL / Parquet | `FileConnector` | `FileDataSource` | manual |
| `SQL` — relational tables (docs focus on PostgreSQL; other engines via SQLAlchemy where supported) | `TableConnector` | `SQLDataSource` | automatic for PostgreSQL-style 3NF (PK/FK heuristics) |
| `SPARQL` — RDF files (`.ttl`, `.rdf`, `.n3`) | `SparqlConnector` | `RdfFileDataSource` | automatic (OWL/RDFS ontology) |
| `SPARQL` — SPARQL endpoints (Fuseki, …) | `SparqlConnector` | `SparqlEndpointDataSource` | automatic (OWL/RDFS ontology) |
| `API` — REST APIs | — | `APIDataSource` | manual |
| `IN_MEMORY` — list / DataFrame | — | `InMemoryDataSource` | manual |

### Supported targets

The graph engines listed in **What you get** are the supported **output** `DBType` values in `graflo.onto`. Each backend uses its own `Connection` implementation under the shared `ConnectionManager` / `DBWriter` / `GraphEngine` flow.

<!-- [![pytest](https://github.com/growgraph/graflo/actions/workflows/pytest.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pytest.yml) -->

## Core Concepts

### Labeled Property Graphs

GraFlo targets the LPG model:

- **Vertices** — nodes with typed **properties** (manifest key: `properties`) and logical **identity** keys for upserts.
- **Edges** — directed relationships between vertices; relationship attributes are declared as **`properties`** on the logical edge (same list-of-names-or-`Field` shape as vertices).

### Schema

The `Schema` is the single source of truth for **graph structure** (not for ingestion transforms):

- **Vertex definitions** — vertex types, **`properties`** (optionally typed: `INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`), identity, and filters. **Secondary indexes** and physical naming live under **`schema.db_profile`** (`DatabaseProfile`: e.g. `vertex_indexes`, `edge_specs`; see [Backend indexes](concepts/backend_indexes.md)).
- **Edge definitions** — source/target (and optional `relation`), **`properties`** for relationship payload, and optional **`identities`** for parallel-edge / MERGE semantics.
- **Schema inference** — generate schemas from PostgreSQL 3NF databases (PK/FK heuristics) or from OWL/RDFS ontologies.

Resources and transforms are part of `IngestionModel`, not `Schema`.

### IngestionModel

`IngestionModel` defines how source records are transformed into graph entities:

- **Resources** — reusable actor pipelines that map raw records to vertices and edges.
- **Transforms** — reusable named transforms referenced by resource steps.

### Resource

A `Resource` is the central abstraction that bridges data sources and the graph schema. Each Resource defines a reusable pipeline of actors (descend, transform, vertex, edge) that maps raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`, so the same transformation logic applies regardless of whether data arrives from a file, an API, or a SPARQL endpoint.

For wide rows with many empty or null columns, **`drop_trivial_input_fields`** (default `false`) removes only **top-level** keys whose value is `null` or `""` before the pipeline runs. The filter is **shallow**: nested dicts and lists are not walked, and empty `{}` / `[]` values are kept because they are not `null` or `""`. **`0`** and **`false`** are kept.

For **TigerGraph**, optional attribute defaults belong in the covariant physical layer: **`schema.db_profile.default_property_values`** maps logical vertex/edge properties to YAML literals that GraFlo turns into GSQL **`DEFAULT`** clauses when defining the graph schema (same idea as `CREATE VERTEX Sensor (id STRING PRIMARY KEY, reading FLOAT DEFAULT -1.0)` in the [TigerGraph schema reference](https://docs.tigergraph.com/gsql-ref/4.2/ddl-and-loading/defining-a-graph-schema)).

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

## More capabilities

- **SPARQL & RDF** — Endpoints and RDF files; optional OWL/RDFS schema inference (`rdflib`, `SPARQLWrapper` in the default install).
- **Schema inference** — From PostgreSQL-style 3NF layouts (PK/FK heuristics) or from OWL/RDFS (`owl:Class` → vertices, `owl:ObjectProperty` → edges, `owl:DatatypeProperty` → vertex fields). See [Example 5](examples/example-5.md).
- **Schema migrations** — Plan and apply guarded schema deltas (`migrate_schema` console script → `graflo.cli.migrate_schema`; library in `graflo.migrate`). Compare `from` / `to` schemas before execution to preview deltas and blocked high-risk operations. See [Concepts — Schema Migration](concepts/index.md#schema-migration-v1).
- **Typed `properties`** — Optional field types (`INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`) on vertices and edges.
- **Batching & concurrency** — Configurable batch sizes, worker counts (`IngestionParams.n_cores`), and DB write concurrency (`IngestionParams.max_concurrent_db_ops` / `DBWriter`).
- **Advanced filtering** — Server-side filtering (e.g. TigerGraph REST++ API), client-side filter expressions, and **SelectSpec** for declarative SQL view/filter control before data reaches Resources.
- **Blank vertices** — Intermediate nodes for complex relationship modelling.

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
