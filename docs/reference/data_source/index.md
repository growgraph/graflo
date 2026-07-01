# Data Source Reference

This section documents the data source abstraction layer in graflo. Data sources define where data comes from, separate from Resources which define how data is transformed.

## Overview

Data sources handle data retrieval from various sources:

- **File Data Sources**: JSON, JSONL, CSV/TSV files
- **API Data Sources**: REST API endpoints
- **SQL Data Sources**: SQL databases
- **In-Memory Data Sources**: Python objects (lists, DataFrames)

Many data sources can map to the same Resource, allowing flexible data ingestion.

## Core Classes

### AbstractDataSource

Base class for all data sources. Provides:

- Unified batch iteration interface (`iter_batches()`)
- Resource name mapping
- Type information

### DataSourceFactory

Factory for creating data source instances:

- Automatic type detection
- Configuration-based creation
- Support for all data source types

### DataSourceRegistry

Maps data sources to resource names:

- Register multiple data sources per resource
- Retrieve data sources by resource name
- Manage data source lifecycle

## File Data Sources

### FileDataSource

Base class for file-based data sources.

### JsonFileDataSource

For JSON files with hierarchical data structures.

### JsonlFileDataSource

For JSONL (JSON Lines) files - one JSON object per line.

### TableFileDataSource

For CSV/TSV files with configurable separator.

## API Data Sources

REST API ingestion uses **`APIConnector`** in manifest **`bindings`** plus runtime credentials via **`conn_proxy`**. Full guide: **[API connector and pagination](../../concepts/connectors/api_connector.md)**.

### APIConnector

Manifest contract for REST API endpoints:

- **`path`** — relative path appended to runtime **`base_url`**
- **`method`**, **`params`**, **`headers`**, HTTP retry/timeout options
- **`pagination`** — optional **`PaginationConfig`** (see below)

### APIDataSource

Runtime HTTP executor. Built by **`RegistryBuilder`** from **`APIConnector`** + **`RestApiConnConfig`**; not constructed directly.

### PaginationConfig

Contract-level pagination on **`APIConnector`**. Two sub-blocks:

- **`request`** (`PaginationRequestConfig`) — how to build paginated HTTP requests
- **`response`** (`ApiResponseStructure`) — how to parse JSON response envelopes

Three request strategies:

| Strategy | Query params (defaults) | When to use |
| -------- | ----------------------- | ----------- |
| **`offset`** | `offset`, `limit` | Skip/limit APIs (`?offset=0&limit=100`) |
| **`page`** | `page`, `per_page` | Page-index APIs (`?page=1&per_page=25`) |
| **`cursor`** | `cursor` | Opaque next-token APIs |

Key fields:

- **`request.page_size`** — records per HTTP request (overridden by **`IngestionParams.batch_size`** when set)
- **`response.records_path`** — dot path to the JSON record list (e.g. `results`, `data`, or `0.results` for an array-wrapped envelope)
- **`response.next_offset_path`** — server-provided next offset (e.g. `next_offset`)
- **`response.has_more_path`** — boolean stop signal (e.g. `has_more`)
- **`response.cursor_path`** — next cursor token for cursor strategy
- **`response.auto_detect`** — infer unset response paths from the first **object** response body (not array-wrapped `[{...}]` envelopes)

Dot paths support numeric segments for list indexing (e.g. `0.results` when the API returns `[{"results": [...]}]`). See **[Dot paths and response shapes](../../concepts/connectors/api_connector.md#dot-paths-and-response-shapes)** in the API connector guide.

See **[API connector and pagination](../../concepts/connectors/api_connector.md)** for loop behaviour, examples per strategy, and field reference.

### ApiAuth / RestApiConnConfig

Runtime **`base_url`** and credentials (`bearer`, `basic`, `digest`, `api_key`) in **`graflo.hq.connection_provider`**, registered on a **`ConnectionProvider`**.

**Env wiring** — map each `conn_proxy` to env vars (`user_service` → `USER_SERVICE_BASE_URL`, `USER_SERVICE_AUTH_TYPE`, …) and call **`register_all_api_configs_from_env(bindings)`** or **`register_api_config_from_env(conn_proxy)`**. See **[API connector and pagination](../../concepts/connectors/api_connector.md)** and **[Example 14](../../examples/example-14.md)**.

## SQL Data Sources

### SQLDataSource

SQL database connector using SQLAlchemy.

### SQLConfig

SQL configuration:

- Connection string (SQLAlchemy format)
- Query string with parameterized queries
- Pagination support

## In-Memory Data Sources

### InMemoryDataSource

For Python objects already in memory:

- `list[dict]`: List of dictionaries
- `list[list]`: List of lists (requires column names)
- `pd.DataFrame`: Pandas DataFrame

## Usage Examples

### File Data Source

```python
from graflo.data_source import DataSourceFactory

# Automatic type detection
source = DataSourceFactory.create_file_data_source(path="data.json")

# Explicit type with custom separator
source = DataSourceFactory.create_file_data_source(
    path="data.csv",
    file_type="table",
    sep="\t"
)
```

### API Data Source (via bindings)

```python
from graflo.architecture.contract.bindings import (
    APIConnector,
    ApiResponseStructure,
    Bindings,
    PaginationConfig,
    PaginationRequestConfig,
)
from graflo.hq.connection_provider import InMemoryConnectionProvider

connector = APIConnector(
    name="users_api",
    path="/api/users",
    pagination=PaginationConfig(
        request=PaginationRequestConfig(strategy="offset", page_size=100),
        response=ApiResponseStructure(records_path="data"),
    ),
)
bindings = Bindings(
    connectors=[connector],
    resource_connector=[{"resource": "users", "connector": "users_api"}],
    connector_connection=[{"connector": "users_api", "conn_proxy": "api_source"}],
)

# export API_SOURCE_BASE_URL, API_SOURCE_AUTH_TYPE=bearer, API_SOURCE_TOKEN=...
provider = InMemoryConnectionProvider()
provider.register_all_api_configs_from_env(bindings=bindings)
```

Manual registration remains available via **`register_generalized_config`** + **`RestApiConnConfig`** / **`ApiAuth`** — see [API connector and pagination](../../concepts/connectors/api_connector.md).

### SQL Data Source

```python
from graflo.data_source import DataSourceFactory, SQLConfig

config = SQLConfig(
    connection_string="postgresql://user:pass@localhost/db",
    query="SELECT * FROM users WHERE active = :active",
    params={"active": True},
)

source = DataSourceFactory.create_sql_data_source(config)
```

### Using with GraphEngine (API via bindings)

API sources are registered automatically when you call **`GraphEngine.define_and_ingest`** with **`bindings`** that include **`APIConnector`** rows and a **`ConnectionProvider`**. See [Quick Start — Using API Data Sources](../../getting_started/quickstart.md#using-api-data-sources) and [API connector and pagination](../../concepts/connectors/api_connector.md).

For file/SQL sidecar configs and manual **`DataSourceRegistry`** wiring (non-API sources):

```python
import asyncio
from graflo import Caster, DataSourceRegistry
from graflo.hq.caster import IngestionParams

registry = DataSourceRegistry()
registry.register(file_source, resource_name="users")

caster = Caster(schema=schema, ingestion_model=ingestion_model)
ingestion_params = IngestionParams(
    batch_size=1000,
    clear_data=False,
    # resources=["users"],
    # connectors=["users_files"],  # connector name or hash; intersects with resources
)

asyncio.run(
    caster.ingest_data_sources(
        data_source_registry=registry,
        conn_conf=conn_conf,
        ingestion_params=ingestion_params,
    )
)
```

