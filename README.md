# GraFlo — Graph Schema & Transformation Language (GSTL) <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>


![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)

**GraFlo** is a manifest-driven toolkit for **labeled property graphs (LPGs)**: describe vertices, edges, and ingestion (`GraphManifest` — YAML or Python), then project and load into a target graph database.

### What you get

- **One pipeline, several graph databases** — The same manifest targets ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, or Grafeo; `DatabaseProfile` and DB-aware types absorb naming, defaults, and indexing differences.
- **Explicit identities** — Vertex identity fields and indexes back upserts so reloads merge on keys instead of blindly duplicating nodes.
- **Reusable ingestion** — `Resource` actor pipelines (including **vertex_router** / **edge_router** steps) bind to files, SQL, SPARQL/RDF, APIs, or in-memory batches via `Bindings` and the `DataSourceRegistry`.

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

| DataSourceType | Connector | DataSource | Schema inference |
|---|---|---|---|
| `FILE` — CSV / JSON / JSONL / Parquet | `FileConnector` | `FileDataSource` | manual |
| `SQL` — relational tables (docs focus on PostgreSQL; other engines via SQLAlchemy where supported) | `TableConnector` | `SQLDataSource` | automatic for PostgreSQL-style 3NF (PK/FK heuristics) |
| `SPARQL` — RDF files (`.ttl`, `.rdf`, `.n3`) | `SparqlConnector` | `RdfFileDataSource` | automatic (OWL/RDFS ontology) |
| `SPARQL` — SPARQL endpoints (Fuseki, …) | `SparqlConnector` | `SparqlEndpointDataSource` | automatic (OWL/RDFS ontology) |
| `API` — REST APIs | — | `APIDataSource` | manual |
| `IN_MEMORY` — list / DataFrame | — | `InMemoryDataSource` | manual |

### Supported targets

The graph engines listed in **What you get** are the supported **output** `DBType` values in `graflo.onto`. Each backend uses its own `Connection` implementation under the shared `ConnectionManager` / `DBWriter` / `GraphEngine` flow.

## More capabilities

- **SPARQL & RDF** — Endpoints and RDF files (`.ttl`, `.rdf`, `.n3`, …); optional OWL/RDFS schema inference (`rdflib`, `SPARQLWrapper` in the default install).
- **Schema inference** — From PostgreSQL-style 3NF layouts (PK/FK heuristics) or from OWL/RDFS (`owl:Class` → vertices, `owl:ObjectProperty` → edges, `owl:DatatypeProperty` → vertex fields).
- **Schema migrations** — Plan and apply guarded schema deltas (`migrate_schema` console script → `graflo.cli.migrate_schema`; library in `graflo.migrate`; see docs).
- **Typed `properties`** — Optional field types (`INT`, `FLOAT`, `STRING`, `DATETIME`, `BOOL`) on vertices and edges.
- **Batching & concurrency** — Configurable batch sizes, worker counts, and DB write concurrency on `IngestionParams` / `DBWriter`.
- **`GraphEngine`** — High-level orchestration for infer, define schema, and ingest (`define_and_ingest`, …); `Caster` stays available for lower-level control.

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

> **Note**: Most tests require external database containers (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, Fuseki) to be running. See [Test databases](#test-databases) above. Grafeo tests need no external services and run out of the box. NebulaGraph tests are gated behind `pytest --run-nebula`.

## Requirements

- Python 3.11+ (Python 3.11 and 3.12 are officially supported)
- python-arango
- nebula3-python>=3.8.3 (NebulaGraph v3.x support)
- nebula5-python>=5.2.1 (NebulaGraph v5.x support)
- sqlalchemy>=2.0.0 (for PostgreSQL and SQL data sources)
- rdflib>=7.0.0 + SPARQLWrapper>=2.0.0 (included in the default install)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.