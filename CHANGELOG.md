# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - Unreleased

### Added
- **Data Source Architecture**: Formalized data source types as a separate layer from Resources
  - New `graflo.data_source` package with abstract base classes and implementations
  - `AbstractDataSource`: Base class for all data sources with unified batch iteration interface
  - `DataSourceType` enum: FILE, API, SQL, IN_MEMORY
  - `DataSourceRegistry`: Maps multiple data sources to Resources
  - `DataSourceFactory`: Factory for creating appropriate data source instances

- **File Data Sources**: Refactored file handling into formal data sources
  - `FileDataSource`: Base class for file-based data sources
  - `JsonFileDataSource`: JSON file data source
  - `JsonlFileDataSource`: JSONL (JSON Lines) file data source
  - `TableFileDataSource`: CSV/TSV file data source with configurable separator

- **REST API Data Source**: Full support for REST API endpoints as data sources
  - `APIDataSource`: REST API connector with comprehensive HTTP configuration
  - `APIConfig`: Configuration for API endpoints including:
    - URL, HTTP method, headers
    - Authentication (Basic, Bearer, Digest)
    - Query parameters, timeouts, retries
    - SSL verification
  - `PaginationConfig`: Flexible pagination support
    - Offset-based pagination
    - Cursor-based pagination
    - Page-based pagination
    - Configurable JSON paths for data extraction

- **SQL Data Source**: SQL database support using SQLAlchemy
  - `SQLDataSource`: SQL database connector
  - `SQLConfig`: SQLAlchemy-style configuration
    - Connection string support
    - Parameterized queries
    - Pagination support
    - Database-agnostic query execution

- **In-Memory Data Source**: Support for Python objects as data sources
  - `InMemoryDataSource`: Handles list[dict], list[list], and pd.DataFrame
  - Automatic conversion of list[list] to list[dict] using column names

- **CLI Integration**: Enhanced CLI to support data source configuration
  - `--data-source-config-path`: Load data sources from configuration file
  - Support for API, SQL, and file data sources via configuration
  - Backward compatible with existing file-based ingestion

- **Dependencies**: Added required packages for new features
  - `requests>=2.31.0`: For REST API data sources
  - `sqlalchemy>=2.0.0`: For SQL data sources
  - `urllib3>=2.0.0`: For HTTP retry functionality

### Changed
- **Caster Refactoring**: Updated `Caster` to use data source architecture
  - `process_resource()`: Now accepts configuration dicts, file paths, or in-memory data
  - `ingest_files()`: Wrapper that creates FileDataSource instances internally
  - `ingest_data_sources()`: New method for ingesting from DataSourceRegistry
  - `process_data_source()`: New method for processing individual data sources
  - Maintains full backward compatibility with existing code

- **Resource vs DataSource Separation**: Clear separation of concerns
  - Resources: Define semantic transformations (how data becomes a graph)
  - DataSources: Define data retrieval (where data comes from)
  - Many DataSources can map to the same Resource

- **Backend Configuration Refactoring**: Complete refactor of database connection configuration system
  - **Pydantic-based Configuration**: Replaced dataclass-based configs with Pydantic `BaseSettings`
    - `DBConfig`: Abstract base class with `uri`, `username`, `password` fields
    - `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`: Backend-specific config classes
    - Environment variable support with prefixes (`ARANGO_`, `NEO4J_`, `TIGERGRAPH_`)
    - Automatic default port handling when port is missing from URI
  - **Renamed `ConnectionKind` to `BackendType`**: More accurate naming for database backend types
  - **Removed `ConfigFactory`**: Replaced with direct config instantiation and `DBConfig.from_dict()`
    - Use `ArangoConfig.from_docker_env()` to load from docker `.env` files
    - Use `ArangoConfig()`, `Neo4jConfig()`, `TigergraphConfig()` for direct instantiation
    - Use `DBConfig.from_dict()` for loading from configuration files
  - **Separated WSGI Configuration**: Moved `WSGIConfig` to separate `wsgi.py` module
    - WSGI is not a database backend, so it no longer inherits from `DBConfig`
    - Removed `WSGI` from `BackendType` enum
  - **Backward Compatibility**: `from_dict()` handles old field names (`url` → `uri`, `cred_name` → `username`, etc.)
  - **Breaking Changes**:
    - `ConnectionKind` → `BackendType`
    - `ConfigFactory` removed (use `DBConfig.from_dict()` or direct config classes)
    - `*ConnectionConfig` aliases removed (use `*Config` names directly: `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`)
    - `connection_type` field removed (now a computed property from class type)

### Deprecated
- `ChunkerFactory`: Still functional but now used internally by FileDataSource
- Direct file path processing: Use DataSource configuration for new code

## [1.2.1]

### Added
- **`any_key` parameter for `DescendActor`**: Added support for processing all keys in a dictionary dynamically
  - When `any_key: true` is set, `DescendActor` will iterate over all key-value pairs in the document dictionary
  - Useful for handling nested structures where you want to process all keys without explicitly listing them
  - Simplifies configuration for cases like package dependencies where multiple relationship types exist
  - Automatically displayed in actor tree visualizations

### Changed
- **Python version requirement**: Reduced minimum Python version requirement from 3.11 to 3.10
  - Updated `requires-python` to `~=3.10.0` in `pyproject.toml`
  - Maintains compatibility with Python 3.10 while using modern type hints

## [1.2.0]

### Added
- **TigerGraph backend support**: Full support for TigerGraph as a graph database backend
  - Complete implementation of `TigerGraphConnection` with all core operations
  - Support for vertex and edge creation, deletion, and querying
  - Integration with TigerGraph REST++ API for efficient data operations
  - Support for GSQL queries and schema management
  - Edge fetching with support for complex vertex IDs
  - Graph statistics and metadata operations
  - Server-side filtering and querying using REST++ API
  - Field type-aware filter generation for proper REST++ filter formatting

### Changed
- **API refactoring**: Renamed `delete_collections` to `delete_graph_structure` for better clarity
  - Method now uses `vertex_types` and `graph_names` parameters instead of `cnames` and `gnames`
  - Updated terminology across codebase to use generic "vertex type" and "edge type" instead of database-specific "collection" terminology
  - Added comprehensive documentation about database organization terminology differences between ArangoDB, Neo4j, and TigerGraph

### Documentation
- Added comprehensive database organization terminology documentation explaining:
  - ArangoDB: Database → Collections (vertex/edge) → Graphs
  - Neo4j: Database → Labels (vertex types) → Relationship Types (edge types)
  - TigerGraph: Graph (like database) → Global Vertex/Edge Types → Associated with graphs

## [1.0.1] - 2025-01
### Changed

Package renamed from `graphcast` to `graflo`.


## [1.0.0] - 2025-01
### Changed
- **Major refactoring of Edge class architecture:**
  - Removed `EdgeCastingType` dependency and related casting logic
  - Simplified Edge class by removing complex discriminant handling
  - Renamed fields for clarity:
    - `source_discriminant` → `match_source`
    - `target_discriminant` → `match_target`
    - `source_relation_field` → `relation_field`
    - `target_relation_field` → removed (unified into single `relation_field`)
  - Removed `non_exclusive` field and related logic
  - Simplified weight configuration by removing `source_fields` and `target_fields`
  - Edge casting type is now determined automatically based on match fields

- **Actor system improvements:**
  - Added `LocationIndex` parameter to all actor `__call__` methods
  - Removed `discriminant` parameter from `VertexActor` constructor
  - Enhanced actor initialization with better type hints and validation
  - Improved vertex merging with `merge_doc_basis_closest_preceding`

- **Core architecture changes:**
  - Replaced `EdgeCastingType.PAIR_LIKE`/`PRODUCT_LIKE` with simplified logic
  - Added `VertexRep` class for better vertex representation
  - Enhanced `ABCFields` with `keep_vertex_name` option
  - Improved type annotations using `TypeAlias`

- **Dependency updates:**
  - Updated numpy from 2.2.5 to 2.3.2
  - Updated pandas from 2.2.3 to 2.3.2
  - Updated networkx from 3.4.2 to 3.5
  - Updated pytest from 8.3.5 to 8.4.1
  - Updated python-arango from 8.1.6 to 8.2.2
  - Added pandas-stubs 2.3.0.250703 for better type support
  - Added tabulate 0.9.0 for table formatting
  - Added types-pytz 2025.2.0.20250809 for type annotations

### Removed
- `EdgeCastingType` enum and related casting logic
- Complex discriminant handling in Edge class
- `source_collection` and `target_collection` fields (now private)
- `non_exclusive` field from Edge class
- `source_fields` and `target_fields` from WeightConfig
- `_reset_edges()` method from EdgeConfig

### Added
- `LocationIndex` type for better location handling
- `VertexRep` class for vertex representation
- `keep_vertex_name` option in ABCFields
- Enhanced type annotations throughout the codebase
- Better error handling and validation

## [0.14.0] - 2025-05
### Changes
- Refactored Tree-like and table-like resources to `Resource`, using actors. All schema configs must be adopted.

## [0.13.14] - 2024-08
    `manange_dbs` script accepts parameters parameters `--db-host`, `--db-password` and `--db-user` (defaults to `root`).  

## [0.13.6] - 2024-02

## [0.13.5] - 2024-01

## [0.13.0] - 2023-12

### Changed
- In `Vertex`
  - `index` and `extra_index` are joined into `indexes`
- In VertexConfig
  - `collections` became `vertices`
  - `blanks` became `blank_vertices`
  - it now contains `list[Vertex]` not `dict`
  - each `Vertex` contains field `name` that was previously the key
- In `EdgeConfig`
  - `main` became `edges`
  - `extra` became `extra_edges`
- In `MapperNode` 
  - edge is now defined under `edge` attribute of `MapperNode` instead of being a union with it
  - `maps` key becomes `children`
  - `type`: `dict` becomes `type`: `vertex`
  
    

### Added

- `cli/plot_schema.py` became a standalone script available with the package installation
-  basic `neo4j` ingestion added:
     - create_database
     - delete_database
     - define_vertex_indices
     - define_edge_indices
     - delete_collections
     - init_db
     - upsert_docs_batch
     - insert_edges_batch

### Fixed

- ***



## [0.12.0] - 2023-10

### Added

- `cli/plot_schema.py` became a standalone script available with the package installation
-  basic `neo4j` ingestion added:
     - create_database
     - delete_database
     - define_vertex_indices
     - define_edge_indices
     - delete_collections
     - init_db
     - upsert_docs_batch
     - insert_edges_batch

### Fixed

- ***

### Changed

- in `ingest_json_files`: ncores -> n_threads 
- schema config changes:
    - `type` specification removed in Transform (field mapping) specification, whenever ambiguous, `image` is used   
- `ConnectionConfigType` -> `DBConnectionConfig`

## [0.11.5] - 2023-08-30

### Fixed

- not more info level logging, only debug

### Changed

- in `ingest_json_files`: ncores -> n_threads
- in `ingest_tables`: n_thread -> n_threads
- added a single entry point for file ingestion : `ingest_files`
- added docker-compose config for Arango; all tests talk to it automatically
- `init_db` now is member of `Connection`
- Introduced `InputType` as `Enum` : {`TABLE`, `JSON`}


## [0.11.3] - 2023-06-24

### Fixed

- suthing version

### Changed

- dev dependency were moved to `dev` group, graphviz was moved to extra group

## [0.11.2] - 2023-06-20

### Fixed

- schema plotting for tables and jsons

### Changed

- introduced `DataSourceType` as `Enum` instead of `str`

## [0.11.1] - 2023-06-14

### Added

- versions via tags
- changelog.MD

[//]: # (### Changed)

[//]: # ()
[//]: # (### Fixed)






