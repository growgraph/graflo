# Quick Start Guide

This guide will help you get started with graflo by showing you how to transform data into a graph structure.

## Basic Concepts

- graflo uses `Caster` class to cast data into a property graph representation and eventually graph database. 
- Class `Schema` encodes the representation of vertices, and edges (relations), the transformations the original data undergoes to become a graph and how data sources are mapped onto graph definition.
- `Resource` class defines how data is transformed into a graph (semantic mapping).
- `DataSource` defines where data comes from (files, APIs, SQL databases, in-memory objects).
- In case the data is provided as files, class `Patterns` manages the mapping of the resources to files. 
- `DataSourceRegistry` maps DataSources to Resources (many DataSources can map to the same Resource).
- Database backend configurations use Pydantic `BaseSettings` with environment variable support. Use `ArangoConfig`, `Neo4jConfig`, or `TigergraphConfig` directly, or load from docker `.env` files using `from_docker_env()`.

## Basic Example

Here's a simple example of transforming CSV files of two types, `people` and `department` into a graph:

```python
from suthing import FileHandle
from graflo import Caster, Patterns, Schema
from graflo.backend.connection.onto import ArangoConfig

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

caster = Caster(schema)

# Load config from docker/arango/.env (recommended)
conn_conf = ArangoConfig.from_docker_env()

# Or create config directly
# conn_conf = ArangoConfig(
#     uri="http://localhost:8535",
#     username="root",
#     password="123",
#     database="_system",
# )

patterns = Patterns.from_dict(
    {
        "patterns": {
            "people": {"regex": "^people.*\.csv$"},
            "departments": {"regex": "^dep.*\.csv$"},
        }
    }
)

caster.ingest_files(
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
    --backend-config-path config/backend.yaml \
    --schema-path config/schema.yaml \
    --data-source-config-path data_sources.yaml
```

## Next Steps

- Explore the [API Reference](../reference/index.md) for detailed documentation
- Check out more [Examples](../examples/index.md) for advanced use cases
- Learn main [concepts](../concepts/index.md), such as `Schema` and its constituents
- Read about [Data Sources](../reference/data_source/index.md) for API and SQL integration 