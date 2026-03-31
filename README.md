# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

A **Graph Schema & Transformation Language (GSTL)** for Labeled Property Graphs (LPG).

GraFlo provides a declarative, database-agnostic specification for mapping heterogeneous data sources — tabular (CSV, SQL), hierarchical (JSON, XML), and RDF/SPARQL — to a unified LPG representation and ingesting it into ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph.

> **Package Renamed**: This package was formerly known as `graphcast`.

![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)

## Overview

GraFlo separates *what the graph looks like* from *where data comes from* and *which database stores it*.

```mermaid
%%{ init: { 
  "theme": "base",
  "themeVariables": {
    "primaryColor": "#90CAF9",
    "primaryTextColor": "#111111",
    "primaryBorderColor": "#1E88E5",
    "lineColor": "#546E7A",
    "secondaryColor": "#A5D6A7",
    "tertiaryColor": "#CE93D8"
  }
} }%%

flowchart LR
    SI["<b>Source Instance</b><br/>File · SQL · SPARQL · API"]
    R["<b>Resource</b><br/>Actor Pipeline"]
    GS["<b>Logical Graph Schema</b><br/>Vertex/Edge Definitions<br/>Identities · DB Profile"]
    DBA["<b>DB-aware Projection</b><br/>DatabaseProfile<br/>VertexConfigDBAware · EdgeConfigDBAware"]
    GC["<b>GraphContainer</b><br/>Covariant Graph Representation"]
    DB["<b>Graph DB (LPG)</b><br/>ArangoDB · Neo4j · TigerGraph · Others"]

    SI --> R --> GS --> GC --> DBA --> DB
```

**Source Instance** → **Resource** → **Logical Graph Schema** → **Covariant Graph Representation** → **DB-aware Projection** → **Graph DB**

| Stage | Role | Code |
|-------|------|------|
| **Source Instance** | A concrete data artifact — a CSV file, a PostgreSQL table, a SPARQL endpoint, a `.ttl` file. | `AbstractDataSource` subclasses (`FileDataSource`, `SQLDataSource`, `SparqlEndpointDataSource`, …) with a `DataSourceType`. |
| **Resource** | A reusable transformation pipeline — actor steps (descend, transform, vertex, edge, vertex_router, edge_router) that map raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`. | `Resource` (part of `IngestionModel`). |
| **Graph Schema** | Declarative logical vertex/edge definitions, identities, typed fields, and DB profile — defined in YAML or Python. | `Schema`, `VertexConfig`, `EdgeConfig`. |
| **Covariant Graph Representation** | A database-independent collection of vertices and edges. | `GraphContainer`. |
| **DB-aware Projection** | Resolves DB-specific naming/default/index behavior from logical schema + `DatabaseProfile`. | `Schema.resolve_db_aware()`, `VertexConfigDBAware`, `EdgeConfigDBAware`. |
| **Graph DB** | The target LPG store — same API for all supported databases. | `ConnectionManager`, `DBWriter`, DB connectors. |

### Supported source types (`DataSourceType`)

| DataSourceType | Connector | DataSource | Schema inference |
|---|---|---|---|
| `FILE` — CSV / JSON / JSONL / Parquet | `FileConnector` | `FileDataSource` | manual |
| `SQL` — PostgreSQL tables | `TableConnector` | `SQLDataSource` | automatic (3NF with PK/FK) |
| `SPARQL` — RDF files (`.ttl`, `.rdf`, `.n3`) | `SparqlConnector` | `RdfFileDataSource` | automatic (OWL/RDFS ontology) |
| `SPARQL` — SPARQL endpoints (Fuseki, …) | `SparqlConnector` | `SparqlEndpointDataSource` | automatic (OWL/RDFS ontology) |
| `API` — REST APIs | — | `APIDataSource` | manual |
| `IN_MEMORY` — list / DataFrame | — | `InMemoryDataSource` | manual |

### Supported targets

ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph — same API for all.

## Features

- **Declarative LPG schema** — Define vertices, edges, vertex identity, secondary DB indexes, weights, and transforms in YAML or Python. The `Schema` is the single source of truth, independent of source or target.
- **Database abstraction** — One logical schema, one API. Target ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, or NebulaGraph without rewriting pipelines. DB idiosyncrasies are handled in DB-aware projection (`Schema.resolve_db_aware(...)`) and connector/writer stages.
- **Resource abstraction** — Each `Resource` defines a reusable actor pipeline (descend, transform, vertex, edge, plus **VertexRouter** and **EdgeRouter** for dynamic type-based routing) that maps raw records to graph elements. Data sources bind to Resources by name via the `DataSourceRegistry`, decoupling transformation logic from data retrieval.
- **SPARQL & RDF support** — Query SPARQL endpoints (e.g. Apache Fuseki), read `.ttl`/`.rdf`/`.n3` files, and auto-infer schemas from OWL/RDFS ontologies (`rdflib` and `SPARQLWrapper` ship with the default package).
- **Schema inference** — Generate graph schemas from PostgreSQL 3NF databases (PK/FK heuristics) or from OWL/RDFS ontologies (`owl:Class` → vertices, `owl:ObjectProperty` → edges, `owl:DatatypeProperty` → vertex fields).
- **Typed fields** — Vertex fields and edge weights carry types (`INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`) for validation and database-specific optimisation.
- **Parallel batch processing** — Configurable batch sizes and multi-core execution.
- **Credential-free source contracts** — `Bindings.connector_connection` maps each `TableConnector` / `SparqlConnector` (by **connector name** or **hash**) to a `conn_proxy` label. Manifests stay free of secrets; a runtime `ConnectionProvider` resolves each proxy to concrete `GeneralizedConnConfig` (for example PostgreSQL or SPARQL endpoint settings). Ingestion resource names are separate and may map to multiple connectors.

## Documentation
Full documentation is available at: [growgraph.github.io/graflo](https://growgraph.github.io/graflo)

## Installation

```bash
pip install graflo
```

Optional extras (see `pyproject.toml` → `[project.optional-dependencies]`):

- `dev` — pytest, ty, pre-commit
- `docs` — MkDocs stack for building the documentation site
- `plot` — `pygraphviz` for the `plot_manifest` CLI (install system Graphviz first)

```bash
pip install "graflo[dev]"
pip install "graflo[dev,docs,plot]"
```

## Usage Examples

### Simple ingest

```python
from suthing import FileHandle

from graflo import Bindings, GraphManifest
from graflo.db.connection.onto import ArangoConfig

manifest = GraphManifest.from_config(FileHandle.load("schema.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Option 1: Load config from docker/arango/.env (recommended)
conn_conf = ArangoConfig.from_docker_env()

# Option 2: Load from environment variables
# Set: ARANGO_URI, ARANGO_USERNAME, ARANGO_PASSWORD, ARANGO_DATABASE
conn_conf = ArangoConfig.from_env()

# Option 3: Load with custom prefix (for multiple configs)
# Set: USER_ARANGO_URI, USER_ARANGO_USERNAME, USER_ARANGO_PASSWORD, USER_ARANGO_DATABASE
user_conn_conf = ArangoConfig.from_env(prefix="USER")

# Option 4: Create config directly
# conn_conf = ArangoConfig(
#     uri="http://localhost:8535",
#     username="root",
#     password="123",
#     database="mygraph",  # For ArangoDB, 'database' maps to schema/graph
# )
# Note: If 'database' (or 'schema_name' for TigerGraph) is not set,
# Caster will automatically use Schema.metadata.name as fallback

from graflo.architecture.contract.bindings import FileConnector
import pathlib

# Create Bindings with file connectors
bindings = Bindings()
work_connector = FileConnector(regex="\Sjson$", sub_path=pathlib.Path("./data"))
bindings.add_connector(
    work_connector,
)
bindings.bind_resource("work", work_connector)

# Or initialize via connectors + resource_connector
# bindings = Bindings(
#     connectors=[
#         FileConnector(
#             name="work_files",
#             regex="^work\\.json$",
#             sub_path=pathlib.Path("./data"),
#         )
#     ],
#     resource_connector=[{"resource": "work", "connector": "work_files"}],
#     # Optional: for SQL/SPARQL connectors, name a proxy; register secrets via ConnectionProvider.
#     # connector_connection=[{"connector": "work_files", "conn_proxy": "files_readonly"}],
# )

from graflo.hq.caster import IngestionParams
from graflo.hq import GraphEngine

# Option 1: Use GraphEngine for schema definition and ingestion (recommended)
engine = GraphEngine()
ingestion_params = IngestionParams(
    clear_data=False,
    # max_items=1000,  # Optional: limit number of items to process
    # batch_size=10000,  # Optional: customize batch size
)

ingest_manifest = manifest.model_copy(update={"bindings": bindings})
ingest_manifest.finish_init()

engine.define_and_ingest(
    manifest=ingest_manifest,
    target_db_config=conn_conf,  # Target database config
    ingestion_params=ingestion_params,
    recreate_schema=False,  # Set to True to drop and redefine schema (script halts if schema exists)
)

# Option 2: Use Caster directly (schema must be defined separately)
# from graflo.hq import GraphEngine
# engine = GraphEngine()
# engine.define_schema(manifest=manifest, target_db_config=conn_conf, recreate_schema=False)
# 
# caster = Caster(schema=schema, ingestion_model=ingestion_model)
# caster.ingest(
#     target_db_config=conn_conf,
#     bindings=bindings,
#     ingestion_params=ingestion_params,
# )
```

### PostgreSQL Schema Inference

```python
from graflo.hq import GraphEngine
from graflo.db.connection.onto import PostgresConfig, ArangoConfig
from graflo import Caster
from graflo.onto import DBType

# Connect to PostgreSQL
postgres_config = PostgresConfig.from_docker_env()  # or PostgresConfig.from_env()

# Create GraphEngine and infer schema from PostgreSQL 3NF database
# Connection is automatically managed inside infer_schema()
engine = GraphEngine(target_db_flavor=DBType.ARANGO)
manifest = engine.infer_manifest(
    postgres_config,
    schema_name="public",  # PostgreSQL schema name
)
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Define schema in target database (optional, can also use define_and_ingest)
target_config = ArangoConfig.from_docker_env()
engine.define_schema(
    manifest=manifest,
    target_db_config=target_config,
    recreate_schema=False,
)

# Use the inferred schema with Caster for ingestion
caster = Caster(schema=schema, ingestion_model=ingestion_model)
# ... continue with ingestion
```

### RDF / SPARQL Ingestion

```python
from pathlib import Path
from graflo.hq import GraphEngine
from graflo.db.connection.onto import ArangoConfig
from graflo.architecture.manifest import GraphManifest

engine = GraphEngine()

# Infer schema from an OWL/RDFS ontology file
ontology = Path("ontology.ttl")
schema, ingestion_model = engine.infer_schema_from_rdf(source=ontology)

# Create source bindings (reads a local .ttl file per rdf:Class)
bindings = engine.create_bindings_from_rdf(source=ontology)

# Or point at a SPARQL endpoint instead:
# from graflo.db.connection.onto import SparqlEndpointConfig
# sparql_cfg = SparqlEndpointConfig(uri="http://localhost:3030", dataset="mydata")
# bindings = engine.create_bindings_from_rdf(
#     source=ontology,
#     endpoint_url=sparql_cfg.query_endpoint,
# )

target = ArangoConfig.from_docker_env()
engine.define_and_ingest(
    manifest=GraphManifest(
        graph_schema=schema,
        ingestion_model=ingestion_model,
        bindings=bindings,
    ),
    target_db_config=target,
)
```

## Development

To install requirements

```shell
git clone git@github.com:growgraph/graflo.git && cd graflo
uv sync --extra dev
```

### Tests

#### Test databases

**Quick Start:** To start all test databases at once, use the convenience scripts from the [docker folder](./docker):

```shell
cd docker
./start-all.sh    # Start all services
./stop-all.sh      # Stop all services
./cleanup-all.sh   # Remove containers and volumes
```

**Individual Services:** To start individual databases, navigate to each database folder and run:

Spin up Arango from [arango docker folder](./docker/arango) by

```shell
docker-compose --env-file .env up arango
```

Neo4j from [neo4j docker folder](./docker/neo4j) by

```shell
docker-compose --env-file .env up neo4j
```

TigerGraph from [tigergraph docker folder](./docker/tigergraph) by

```shell
docker-compose --env-file .env up tigergraph
```

FalkorDB from [falkordb docker folder](./docker/falkordb) by

```shell
docker-compose --env-file .env up falkordb
```

Memgraph from [memgraph docker folder](./docker/memgraph) by

```shell
docker-compose --env-file .env up memgraph
```

NebulaGraph from [nebula docker folder](./docker/nebula) by

```shell
docker-compose --env-file .env up
```

and Apache Fuseki from [fuseki docker folder](./docker/fuseki) by

```shell
docker-compose --env-file .env up fuseki
```

To run unit tests

```shell
uv run pytest test
```

> **Note**: Tests require external database containers (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, Fuseki) to be running. CI builds intentionally skip test execution. Tests must be run locally with the required database images started (see [Test databases](#test-databases) section above). NebulaGraph tests are gated behind `pytest --run-nebula`.

## Requirements

- Python 3.11+ (Python 3.11 and 3.12 are officially supported)
- python-arango
- nebula3-python>=3.8.3 (NebulaGraph v3.x support)
- nebula5-python>=5.2.1 (NebulaGraph v5.x support)
- sqlalchemy>=2.0.0 (for PostgreSQL and SQL data sources)
- rdflib>=7.0.0 + SPARQLWrapper>=2.0.0 (included in the default install)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.