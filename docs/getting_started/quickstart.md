# Quick Start Guide

This guide will help you get started with graflo by showing you how to transform data into a graph structure.

## Basic Concepts

- graflo uses `Caster` class to cast data into a property graph representation and eventually graph database. 
- Class `Schema` encodes the representation of vertices, and edges (relations), the transformations the original data undergoes to become a graph and how data sources are mapped onto graph definition.
- `Resource` class defines how data is transformed into a graph (semantic mapping).
- `DataSource` defines where data comes from (files, APIs, SQL databases, in-memory objects).
- In case the data is provided as files, class `Patterns` manages the mapping of the resources to files. 
- `DataSourceRegistry` maps DataSources to Resources (many DataSources can map to the same Resource).
1- Database backend configurations use Pydantic `BaseSettings` with environment variable support. Use `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, or `PostgresConfig` directly, or load from docker `.env` files using `from_docker_env()`. All configs inherit from `DBConfig` and support unified `database`/`schema_name` structure with `effective_database` and `effective_schema` properties for database-agnostic access. If `effective_schema` is not set, `Caster` automatically uses `Schema.general.name` as fallback.

## Basic Example

Here's a simple example of transforming CSV files of two types, `people` and `department` into a graph:

```python
from suthing import FileHandle
from graflo import Caster, Patterns, Schema
from graflo.db.connection.onto import ArangoConfig

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

caster = Caster(schema)

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

patterns = Patterns.from_dict(
    {
        "patterns": {
            "people": {"regex": "^people.*\.csv$"},
            "departments": {"regex": "^dep.*\.csv$"},
        }
    }
)

caster.ingest(
    path=".",
    conn_conf=conn_conf,
    patterns=patterns,
)
```

Here `schema` defines the graph and the mapping the sources to vertices and edges (refer to [Schema](concepts/schema) for details on schema and its components).
In `patterns` the keys `"people"` and `"departments"` correspond to resource names from `Schema`, while `"regex"` contain regex patterns to find the corresponding files.

## Using API Data Sources

You can also ingest data from REST API endpoints:

```python
from graflo import Caster, DataSourceRegistry, Schema
from graflo.data_source import DataSourceFactory, APIConfig, PaginationConfig

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

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
caster = Caster(schema)
caster.ingest_data_sources(
    data_source_registry=registry,
    conn_conf=conn_conf,
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
    --schema-path config/schema.yaml \
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
from graflo.db.connection.onto import ArangoConfig, Neo4jConfig, TigergraphConfig, PostgresConfig

# Load from default environment variables
arango_conf = ArangoConfig.from_env()
neo4j_conf = Neo4jConfig.from_env()
tg_conf = TigergraphConfig.from_env()
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