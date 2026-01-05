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

### APIDataSource

REST API connector with full HTTP configuration support.

### APIConfig

Configuration for API endpoints:

- URL, method, headers
- Authentication (Basic, Bearer, Digest)
- Query parameters, timeouts, retries
- SSL verification

### PaginationConfig

Pagination configuration:

- Offset-based pagination
- Cursor-based pagination
- Page-based pagination
- JSON path configuration for data extraction

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

### API Data Source

```python
from graflo.data_source import DataSourceFactory, APIConfig, PaginationConfig

config = APIConfig(
    url="https://api.example.com/users",
    method="GET",
    headers={"Authorization": "Bearer token"},
    pagination=PaginationConfig(
        strategy="offset",
        page_size=100,
        data_path="data",
    ),
)

source = DataSourceFactory.create_api_data_source(config)
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

### Using with Caster

```python
from graflo import Caster, DataSourceRegistry
from graflo.caster import IngestionParams

registry = DataSourceRegistry()
registry.register(file_source, resource_name="users")
registry.register(api_source, resource_name="users")  # Multiple sources for same resource

caster = Caster(schema)

ingestion_params = IngestionParams(
    batch_size=1000,  # Process 1000 items per batch
    clean_start=False,  # Set to True to wipe existing database
)

caster.ingest_data_sources(
    data_source_registry=registry,
    conn_conf=conn_conf,
    ingestion_params=ingestion_params,
)
```

