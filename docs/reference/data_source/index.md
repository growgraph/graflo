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

REST API ingestion uses **`APIConnector`** in manifest **`bindings`** plus runtime credentials via **`conn_proxy`**. Full guide: **[API connector and pagination](../../concepts/api_connector.md)**.

### APIConnector

Manifest contract for REST API endpoints:

- **`path`** — relative path appended to runtime **`base_url`**
- **`method`**, **`params`**, **`headers`**, HTTP retry/timeout options
- **`pagination`** — optional **`PaginationConfig`** (see below)

### APIDataSource

Runtime HTTP executor. Built by **`RegistryBuilder`** from **`APIConnector`** + **`RestApiConnConfig`**; not constructed directly.

### PaginationConfig

Contract-level pagination on **`APIConnector`**. Three strategies:

| Strategy | Query params (defaults) | When to use |
| -------- | ----------------------- | ----------- |
| **`offset`** | `offset`, `limit` | Skip/limit APIs (`?offset=0&limit=100`) |
| **`page`** | `page`, `per_page` | Page-index APIs (`?page=1&per_page=25`) |
| **`cursor`** | `cursor` | Opaque next-token APIs |

Shared fields:

- **`page_size`** — records per HTTP request (overridden by **`IngestionParams.batch_size`** when set)
- **`data_path`** — dot path to the JSON array of records (e.g. `data`, `results.items`)
- **`has_more_path`** — boolean path for offset/page stop condition (e.g. `has_more`)
- **`cursor_path`** — next cursor token for cursor strategy (e.g. `pagination.next_cursor`)
- **`initial_offset`**, **`initial_page`**, **`initial_cursor`** — starting pagination state

See **[API connector and pagination](../../concepts/api_connector.md)** for loop behaviour, examples per strategy, and field reference.

### ApiAuth / RestApiConnConfig

Runtime **`base_url`** and credentials (`bearer`, `basic`, `digest`, `api_key`) in **`graflo.hq.connection_provider`**, registered on a **`ConnectionProvider`**.

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
from graflo.architecture.contract.bindings import APIConnector, Bindings, PaginationConfig
from graflo.hq.connection_provider import (
    ApiAuth,
    ApiGeneralizedConnConfig,
    InMemoryConnectionProvider,
    RestApiConnConfig,
)
from graflo.hq.registry_builder import RegistryBuilder

connector = APIConnector(
    name="users_api",
    path="/api/users",
    pagination=PaginationConfig(strategy="offset", page_size=100, data_path="data"),
)
bindings = Bindings(
    connectors=[connector],
    resource_connector=[{"resource": "users", "connector": "users_api"}],
    connector_connection=[{"connector": "users_api", "conn_proxy": "api_source"}],
)
provider = InMemoryConnectionProvider()
provider.register_generalized_config(
    conn_proxy="api_source",
    config=ApiGeneralizedConnConfig(
        config=RestApiConnConfig(
            base_url="https://api.example.com",
            auth=ApiAuth(auth_type="bearer", token="..."),
        )
    ),
)
provider.bind_from_bindings(bindings=bindings)
# registry = RegistryBuilder(schema, ingestion_model).build(
#     bindings=bindings, ingestion_params=..., connection_provider=provider,
# )
```

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

API sources are registered automatically when you call **`GraphEngine.define_and_ingest`** with **`bindings`** that include **`APIConnector`** rows and a **`ConnectionProvider`**. See [Quick Start — Using API Data Sources](../../getting_started/quickstart.md#using-api-data-sources) and [API connector and pagination](../../concepts/api_connector.md).

For file/SQL sidecar configs and manual **`DataSourceRegistry`** wiring (non-API sources):

```python
import asyncio
from graflo import Caster, DataSourceRegistry
from graflo.hq.caster import IngestionParams

registry = DataSourceRegistry()
registry.register(file_source, resource_name="users")

caster = Caster(schema=schema, ingestion_model=ingestion_model)
ingestion_params = IngestionParams(batch_size=1000, clear_data=False)

asyncio.run(
    caster.ingest_data_sources(
        data_source_registry=registry,
        conn_conf=conn_conf,
        ingestion_params=ingestion_params,
    )
)
```

