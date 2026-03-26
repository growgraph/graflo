# Quick Start Guide

This guide will help you get started with graflo by showing you how to transform data into a graph structure.

## Basic Concepts

- graflo uses `Caster` class to cast data into a property graph representation and eventually graph database. 
- Class `Schema` encodes the logical graph representation (vertices, edges, identities, DB profile).
- Class `IngestionModel` defines resources/transforms and how records are mapped into graph entities.
- `Resource` class defines how data is transformed into a graph (semantic mapping).
- `DataSource` defines where data comes from (files, APIs, SQL databases, in-memory objects).
- `Bindings` manages the mapping of resources to their physical data sources (files or PostgreSQL tables). 
- `DataSourceRegistry` maps DataSources to Resources (many DataSources can map to the same Resource).
- Database backend configurations use Pydantic `BaseSettings` with environment variable support. Use `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, `FalkordbConfig`, `MemgraphConfig`, `NebulaConfig`, or `PostgresConfig` directly, or load from docker `.env` files using `from_docker_env()`. All configs inherit from `DBConfig` and support unified `database`/`schema_name` structure with `effective_database` and `effective_schema` properties for database-agnostic access. If `effective_schema` is not set, `GraphEngine.define_schema()` automatically uses `schema.metadata.name` as fallback.

## Basic Example

Here's a simple example of transforming CSV files of two types, `people` and `department` into a graph:

```python
import pathlib
from suthing import FileHandle
from graflo import Bindings, Caster, GraphManifest
from graflo.architecture.contract.bindings import FileConnector
from graflo.db.connection.onto import ArangoConfig

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

caster = Caster(schema=schema, ingestion_model=ingestion_model)

# Option 1: Load config from docker/arango/.env (recommended)
conn_conf = ArangoConfig.from_docker_env()

# Option 2: Load from environment variables
# Set environment variables:
#   export ARANGO_URI=http://localhost:8529
#   export ARANGO_USERNAME=root
#   export ARANGO_PASSWORD=123
#   export ARANGO_DATABASE=mygraph
conn_conf = ArangoConfig.from_env()

# Option 3: Load with custom prefix (for multiple configs)
# Set environment variables:
#   export USER_ARANGO_URI=http://user-db:8529
#   export USER_ARANGO_USERNAME=user
#   export USER_ARANGO_PASSWORD=pass
#   export USER_ARANGO_DATABASE=usergraph
user_conn_conf = ArangoConfig.from_env(prefix="USER")

# Option 4: Create config directly
# conn_conf = ArangoConfig(
#     uri="http://localhost:8535",
#     username="root",
#     password="123",
#     database="mygraph",  # For ArangoDB, 'database' maps to schema/graph
# )

# Create bindings with file connectors
# FileConnector includes the path (sub_path) where files are located
bindings = Bindings()
people_connector = FileConnector(regex="^people.*\.csv$", sub_path=pathlib.Path("."))
bindings.add_connector(
    people_connector,
)
bindings.bind_resource("people", people_connector)
departments_connector = FileConnector(
    regex="^dep.*\.csv$", sub_path=pathlib.Path(".")
)
bindings.add_connector(
    departments_connector,
)
bindings.bind_resource("departments", departments_connector)

# Or initialize from explicit connector bindings
bindings = Bindings(
    connectors=[
        FileConnector(
            name="people_files",
            regex="^people.*\\.csv$",
            sub_path=pathlib.Path("."),
        ),
        FileConnector(
            name="departments_files",
            regex="^dep.*\\.csv$",
            sub_path=pathlib.Path("."),
        ),
    ],
    resource_connector=[
        {"resource": "people", "connector": "people_files"},
        {"resource": "departments", "connector": "departments_files"},
    ],
)

from graflo.hq.caster import IngestionParams
from graflo.hq import GraphEngine

# Option 1: Use GraphEngine for schema definition and ingestion (recommended)
engine = GraphEngine()
ingestion_params = IngestionParams(
    clear_data=False,
)

# Attach bindings to the manifest before orchestration.
ingest_manifest = manifest.model_copy(update={"bindings": bindings})
ingest_manifest.finish_init()

engine.define_and_ingest(
    manifest=ingest_manifest,
    target_db_config=conn_conf,  # Target database config
    ingestion_params=ingestion_params,
    recreate_schema=False,  # Set to True to drop and redefine schema (script halts if schema exists)
)

# Option 2: Use Caster directly (schema must be defined separately)
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

Here `schema` defines the logical graph, while `ingestion_model` defines resources/transforms and `bindings` maps resources to physical data sources. See [Creating a Manifest](creating_manifest.md) and [Concepts — Schema](../concepts/index.md#schema) for details.

`Bindings` maps resource names (from `IngestionModel`) to their physical data sources:
- **FileConnector**: For file-based resources with `regex` for matching filenames and `sub_path` for the directory to search
- **TableConnector**: For PostgreSQL table resources (table/schema/view metadata on the connector; connection URLs and secrets are **not** stored in the manifest when using **`connector_connection`** — see below)
- **SparqlConnector**: RDF class / SPARQL endpoint wiring (same proxy pattern as SQL when needed)

For SQL and SPARQL sources, add **`connector_connection`**: a list of `{"connector": "<connector name or hash>", "conn_proxy": "<label>"}`. At runtime, register each `conn_proxy` on an `InMemoryConnectionProvider` (or your own `ConnectionProvider`) with `GeneralizedConnConfig`. `GraphEngine` / `ResourceMapper` call `bind_connector_to_conn_proxy` when building bindings from Postgres or RDF workflows so HQ and the manifest stay aligned.

The `ingest()` method takes:
- `target_db_config`: Target graph database configuration (where to write the graph)
- `bindings`: Source data connectors (where to read data from - files or database tables)

## 🚀 Using PostgreSQL Tables as Data Sources

**Automatically infer graph schemas from normalized PostgreSQL databases (3NF)** - No manual schema definition needed! 

**Requirements**: Works best with normalized databases (3NF) that have proper primary keys (PK) and foreign keys (FK) decorated. graflo uses intelligent heuristics to automatically detect vertex-like and edge-like tables, infer relationships from foreign keys, and map PostgreSQL types to graph types.

You can ingest data directly from PostgreSQL tables. First, infer the schema from your PostgreSQL database:

```python
from graflo.hq import GraphEngine
from graflo.db.connection.onto import PostgresConfig

# Connect to PostgreSQL
pg_config = PostgresConfig.from_docker_env()  # Or from_env(), or create directly

# Create GraphEngine and infer schema from PostgreSQL (automatically detects vertices and edges)
# Connection is automatically managed inside infer_manifest()
engine = GraphEngine()
manifest = engine.infer_manifest(pg_config, schema_name="public")

# Create bindings from PostgreSQL tables
engine = GraphEngine()
bindings = engine.create_bindings(pg_config, schema_name="public")

# Or create bindings manually
from graflo.architecture.contract.bindings import Bindings, TableConnector

bindings = Bindings()
users_connector = TableConnector(table_name="users", schema_name="public")
bindings.add_connector(
    users_connector,
)
bindings.bind_resource("users", users_connector)
products_connector = TableConnector(table_name="products", schema_name="public")
bindings.add_connector(
    products_connector,
)
bindings.bind_resource("products", products_connector)

# Ingest
from graflo.db.connection.onto import ArangoConfig
from graflo.hq import GraphEngine

arango_config = ArangoConfig.from_docker_env()  # Target graph database

# Use GraphEngine for schema definition and ingestion
engine = GraphEngine()
ingestion_params = IngestionParams(
    clear_data=False,
    # Optional: restrict to a date range with datetime_after, datetime_before, datetime_column
    # (use with create_bindings(..., datetime_columns={...}) for per-table columns)
)

ingest_manifest = manifest.model_copy(update={"bindings": bindings})
ingest_manifest.finish_init()

engine.define_and_ingest(
    manifest=ingest_manifest,
    target_db_config=arango_config,  # Target graph database
    ingestion_params=ingestion_params,
    recreate_schema=False,  # Set to True to drop and redefine schema (script halts if schema exists)
)
```

## Using API Data Sources

You can also ingest data from REST API endpoints:

```python
from graflo import Caster, DataSourceRegistry, GraphManifest
from graflo.data_source import DataSourceFactory, APIConfig, PaginationConfig

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Create API data source
api_config = APIConfig(
    url="https://api.example.com/users",
    method="GET",
    pagination=PaginationConfig(
        strategy="offset",
        offset_param="offset",
        limit_param="limit",
        page_size=100,
        has_more_path="has_more",
        data_path="data",
    ),
)

api_source = DataSourceFactory.create_api_data_source(api_config)

# Register with resource
registry = DataSourceRegistry()
registry.register(api_source, resource_name="users")

# Ingest
from graflo.hq.caster import IngestionParams
from graflo.hq import GraphEngine

# Define schema first (required before ingestion)
engine = GraphEngine()
engine.define_schema(
    manifest=manifest,
    target_db_config=conn_conf,
    recreate_schema=False,
)

# Then ingest using Caster
caster = Caster(schema=schema, ingestion_model=ingestion_model)
ingestion_params = IngestionParams()  # Use default parameters

import asyncio

asyncio.run(
    caster.ingest_data_sources(
        data_source_registry=registry,
        conn_conf=conn_conf,  # Target database config
        ingestion_params=ingestion_params,
    )
)
```

## Using Configuration Files

You can also use a configuration file to define data sources:

```yaml
# data_sources.yaml
data_sources:
  - source_type: api
    resource_name: users
    config:
      url: https://api.example.com/users
      method: GET
      pagination:
        strategy: offset
        page_size: 100
        data_path: data
  - source_type: file
    resource_name: products
    path: data/products.json
```

Then use it with the CLI:

```bash
uv run ingest \
    --db-config-path config/db.yaml \
    --schema-path config/manifest.yaml \
    --data-source-config-path data_sources.yaml
```

## Database Configuration Options

graflo supports multiple ways to configure database connections:

### Environment Variables

You can configure database connections using environment variables. Each database type has its own prefix:

**ArangoDB:**
```bash
export ARANGO_URI=http://localhost:8529
export ARANGO_USERNAME=root
export ARANGO_PASSWORD=123
export ARANGO_DATABASE=mygraph
```

**Neo4j:**
```bash
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=password
export NEO4J_DATABASE=mydb
```

**TigerGraph:**
```bash
export TIGERGRAPH_URI=http://localhost:9000
export TIGERGRAPH_USERNAME=tigergraph
export TIGERGRAPH_PASSWORD=tigergraph
export TIGERGRAPH_SCHEMA_NAME=mygraph
```

**FalkorDB:**
```bash
export FALKORDB_URI=redis://localhost:6379
export FALKORDB_PASSWORD=
export FALKORDB_DATABASE=mygraph
```

**Memgraph:**
```bash
export MEMGRAPH_URI=bolt://localhost:7687
export MEMGRAPH_USERNAME=
export MEMGRAPH_PASSWORD=
export MEMGRAPH_DATABASE=memgraph
```

**NebulaGraph:**
```bash
export NEBULA_URI=nebula://localhost:9669
export NEBULA_USERNAME=root
export NEBULA_PASSWORD=nebula
export NEBULA_SCHEMA_NAME=mygraph
export NEBULA_VERSION=3  # "3" for v3.x (nGQL) or "5" for v5.x (GQL)
```

**PostgreSQL:**
```bash
export POSTGRES_URI=postgresql://localhost:5432
export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=password
export POSTGRES_DATABASE=mydb
export POSTGRES_SCHEMA_NAME=public
```

Then load the config:

```python
from graflo.db.connection.onto import ArangoConfig, Neo4jConfig, TigergraphConfig, FalkordbConfig, MemgraphConfig, NebulaConfig, PostgresConfig

# Load from default environment variables
arango_conf = ArangoConfig.from_env()
neo4j_conf = Neo4jConfig.from_env()
tg_conf = TigergraphConfig.from_env()
falkordb_conf = FalkordbConfig.from_env()
memgraph_conf = MemgraphConfig.from_env()
nebula_conf = NebulaConfig.from_env()
pg_conf = PostgresConfig.from_env()
```

### Multiple Configurations with Prefixes

For multiple database configurations, use prefixes:

```bash
# User database
export USER_ARANGO_URI=http://user-db:8529
export USER_ARANGO_USERNAME=user
export USER_ARANGO_PASSWORD=pass
export USER_ARANGO_DATABASE=usergraph

# Knowledge graph database
export KG_ARANGO_URI=http://kg-db:8529
export KG_ARANGO_USERNAME=kg
export KG_ARANGO_PASSWORD=secret
export KG_ARANGO_DATABASE=knowledgegraph
```

```python
user_conf = ArangoConfig.from_env(prefix="USER")
kg_conf = ArangoConfig.from_env(prefix="KG")
```

### Docker Environment Files

Load from docker `.env` files:
```python
conn_conf = ArangoConfig.from_docker_env()
```

### Direct Configuration

Create config objects directly:
```python
conn_conf = ArangoConfig(
    uri="http://localhost:8529",
    username="root",
    password="123",
    database="mygraph",
)
```

## Next Steps

- Explore the [API Reference](../reference/index.md) for detailed documentation
- Check out more [Examples](../examples/index.md) for advanced use cases
- Learn main [concepts](../concepts/index.md), such as `Schema` and its constituents
- Read about [Data Sources](../reference/data_source/index.md) for API and SQL integration 