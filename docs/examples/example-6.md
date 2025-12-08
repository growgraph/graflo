# Example 6: REST API Data Source [coming]

This example demonstrates how to ingest data from a REST API endpoint into a graph database.

## Scenario

Suppose you have a REST API that provides user data with pagination. You want to ingest this data into a graph database.

## API Response Format

The API returns data in the following format:

```json
{
  "data": [
    {"id": 1, "name": "Alice", "department": "Engineering"},
    {"id": 2, "name": "Bob", "department": "Sales"}
  ],
  "has_more": true,
  "offset": 0,
  "limit": 100,
  "total": 250
}
```

## Schema Definition

Define your schema as usual:

```yaml
general:
  name: users_api

vertices:
  - name: person
    fields:
      - id
      - name
      - department
    indexes:
      - fields:
          - id

resources:
  - resource_name: users
    apply:
      - vertex: person
```

## Using API Data Source

### Python Code

```python
from suthing import FileHandle
from graflo import Caster, DataSourceRegistry, Schema
from graflo.data_source import DataSourceFactory, APIConfig, PaginationConfig
from graflo.db.connection.onto import DBConfig

# Load schema
schema = Schema.from_dict(FileHandle.load("schema.yaml"))

# Create API configuration
api_config = APIConfig(
    url="https://api.example.com/users",
    method="GET",
    headers={"Authorization": "Bearer your-token"},
    pagination=PaginationConfig(
        strategy="offset",
        offset_param="offset",
        limit_param="limit",
        page_size=100,
        has_more_path="has_more",
        data_path="data",
    ),
)

# Create API data source
api_source = DataSourceFactory.create_api_data_source(api_config)

# Register with resource
registry = DataSourceRegistry()
registry.register(api_source, resource_name="users")

# Create caster and ingest
caster = Caster(schema)
# Load config from file
config_data = FileHandle.load("db.yaml")
conn_conf = DBConfig.from_dict(config_data)

caster.ingest_data_sources(
    data_source_registry=registry,
    conn_conf=conn_conf,
    batch_size=1000,
)
```

### Using Configuration File

Create a data source configuration file (`data_sources.yaml`):

```yaml
data_sources:
  - source_type: api
    resource_name: users
    config:
      url: https://api.example.com/users
      method: GET
      headers:
        Authorization: "Bearer your-token"
      pagination:
        strategy: offset
        offset_param: offset
        limit_param: limit
        page_size: 100
        has_more_path: has_more
        data_path: data
```

Then use the CLI:

```bash
uv run ingest \
    --db-config-path config/db.yaml \
    --schema-path config/schema.yaml \
    --data-source-config-path data_sources.yaml
```

## Pagination Strategies

### Offset-based Pagination

```python
pagination = PaginationConfig(
    strategy="offset",
    offset_param="offset",
    limit_param="limit",
    page_size=100,
    has_more_path="has_more",
    data_path="data",
)
```

### Cursor-based Pagination

```python
pagination = PaginationConfig(
    strategy="cursor",
    cursor_param="next_cursor",
    cursor_path="next_cursor",
    page_size=100,
    data_path="items",
)
```

### Page-based Pagination

```python
pagination = PaginationConfig(
    strategy="page",
    page_param="page",
    per_page_param="per_page",
    page_size=50,
    data_path="results",
)
```

## Authentication

### Basic Authentication

```python
api_config = APIConfig(
    url="https://api.example.com/users",
    auth={"type": "basic", "username": "user", "password": "pass"},
)
```

### Bearer Token

```python
api_config = APIConfig(
    url="https://api.example.com/users",
    auth={"type": "bearer", "token": "your-token"},
)
```

### Custom Headers

```python
api_config = APIConfig(
    url="https://api.example.com/users",
    headers={"X-API-Key": "your-api-key"},
)
```

## Combining Multiple Data Sources

You can combine multiple data sources for the same resource:

```python
registry = DataSourceRegistry()

# API source
api_source = DataSourceFactory.create_api_data_source(api_config)
registry.register(api_source, resource_name="users")

# File source
file_source = DataSourceFactory.create_file_data_source(path="users_backup.json")
registry.register(file_source, resource_name="users")

# Both will be processed and combined
caster.ingest_data_sources(data_source_registry=registry, conn_conf=conn_conf)
```

