# GraFlo <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

A framework for transforming **tabular** (CSV, SQL) and **hierarchical** data (JSON, XML) into property graphs and ingesting them into graph databases (ArangoDB, Neo4j, **TigerGraph**).

> **⚠️ Package Renamed**: This package was formerly known as `graphcast`.

![Python](https://img.shields.io/badge/python-3.10-blue.svg) 
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
  - Fields can be specified as strings (backward compatible) or typed `Field` objects with types (INT, FLOAT, STRING, DATETIME, BOOL)
  - Type information enables better validation and database-specific optimizations
- **Edge Definitions**: Define relationships between vertices and their properties
  - Weight fields support typed definitions for better type safety
- **Resource Mapping**: describe how data sources map to vertices and edges
- **Transforms**: Modify data during the casting process
- **Automatic Schema Inference**: Generate schemas automatically from PostgreSQL 3NF databases

### Resources
Resources are your data sources that can be:

- **Table-like**: CSV files, database tables
- **JSON-like**: JSON files, nested data structures

## Features

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
- **🚀 PostgreSQL Schema Inference**: **Automatically generate schemas from PostgreSQL 3NF databases** - No manual schema definition needed!
    - Introspect PostgreSQL schemas to identify vertex-like and edge-like tables
    - Automatically map PostgreSQL data types to graflo Field types (INT, FLOAT, STRING, DATETIME, BOOL)
    - Infer vertex configurations from table structures with proper indexes
    - Infer edge configurations from foreign key relationships
    - Create Resource mappings from PostgreSQL tables automatically
    - Direct database access - ingest data without exporting to files first
- **Parallel processing**: Use as many cores as you have
- **Database support**: Ingest into ArangoDB, Neo4j, and **TigerGraph** using the same API (database agnostic). Source data from PostgreSQL and other SQL databases.
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
from suthing import FileHandle

from graflo import Schema, Caster, Patterns
from graflo.db.connection.onto import ArangoConfig

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

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
# Caster will automatically use Schema.general.name as fallback

from graflo.util.onto import FilePattern
import pathlib

# Create Patterns with file patterns
patterns = Patterns()
patterns.add_file_pattern(
    "work",
    FilePattern(regex="\Sjson$", sub_path=pathlib.Path("./data"), resource_name="work")
)

# Or use resource_mapping for simpler initialization
# patterns = Patterns(
#     _resource_mapping={
#         "work": "./data/work.json",
#     }
# )

schema.fetch_resource()

from graflo.caster import IngestionParams

caster = Caster(schema)

ingestion_params = IngestionParams(
    clean_start=False,  # Set to True to wipe existing database
    # max_items=1000,  # Optional: limit number of items to process
    # batch_size=10000,  # Optional: customize batch size
)

caster.ingest(
    output_config=conn_conf,  # Target database config
    patterns=patterns,  # Source data patterns
    ingestion_params=ingestion_params,
)
```

### PostgreSQL Schema Inference

```python
from graflo.db.postgres import PostgresConnection
from graflo.db.postgres.heuristics import infer_schema_from_postgres
from graflo.db.connection.onto import PostgresConfig
from graflo import Caster
from graflo.onto import DBFlavor

# Connect to PostgreSQL
postgres_config = PostgresConfig.from_docker_env()  # or PostgresConfig.from_env()
postgres_conn = PostgresConnection(postgres_config)

# Infer schema from PostgreSQL 3NF database
schema = infer_schema_from_postgres(
    postgres_conn,
    schema_name="public",  # PostgreSQL schema name
    db_flavor=DBFlavor.ARANGO  # Target graph database flavor
)

# Close PostgreSQL connection
postgres_conn.close()

# Use the inferred schema with Caster
caster = Caster(schema)
# ... continue with ingestion
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

- Python 3.10+
- python-arango
- sqlalchemy>=2.0.0 (for PostgreSQL and SQL data sources)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.