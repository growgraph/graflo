# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.6.1] - 2026-02-22

### Added
- **NebulaGraph adapter**: Full support for NebulaGraph as a target graph database, with dual-version support for both v3.x (nGQL via `nebula3-python`, Thrift) and v5.x (ISO GQL via `nebula5-python`, gRPC)
  - `NebulaConnection` implementing the full `Connection` interface: space lifecycle (`create_database`, `delete_database`, `init_db`), schema DDL (`define_schema`, `define_vertex_classes`, `define_edge_classes`), index management with rebuild/retry, batched vertex upserts and edge inserts, `fetch_docs`, `fetch_edges`, `fetch_present_documents`, `keep_absent_documents`, and `aggregate` (COUNT, MAX, MIN, AVG, SORTED_UNIQUE)
  - Version-agnostic adapter layer (`NebulaClientAdapter`, `NebulaV3Adapter`, `NebulaV5Adapter`, `NebulaResultSet`) abstracting driver differences behind a unified interface, with `create_adapter()` factory
  - Pure-function query builders in `graflo.db.nebula.query` for DDL (space/tag/edge/index creation), DML (batch upsert vertices, insert edges), and DQL (fetch docs/edges, aggregation) in both nGQL and GQL dialects
  - Utilities in `graflo.db.nebula.util`: NebulaGraph type mapping (`FieldType` to `int64`/`float`/`string`/`bool`), value serialization, VID generation (composite key support via `::`), filter rendering for nGQL and Cypher flavors, and schema propagation wait helpers
  - `NebulaConfig` (extends `DBConfig`) with fields for `version` (selects v3 or v5), `vid_type`, `partition_num`, `replica_factor`, `storaged_addresses`, `request_timeout`; environment prefix `NEBULA_`; `from_docker_env()` support reading from `docker/nebula/.env`
  - `DBType.NEBULA` enum value; NebulaGraph registered in both `SOURCE_DATABASES` and `TARGET_DATABASES`
  - Docker Compose setup (`docker/nebula/`) with four services: `nebula-metad`, `nebula-storaged`, `nebula-graphd`, and `nebula-graph-studio` (v3.8.0 images); docker management scripts (`start-all.sh`, `stop-all.sh`, `cleanup-all.sh`) updated to include NebulaGraph
  - Test suite with ~76 tests: unit tests for config, query builders, and utilities; integration tests (gated behind `pytest --run-nebula`) covering connection lifecycle, CRUD, fetch, edges, and aggregation

### Documentation
- Added NebulaGraph to all supported-targets lists across README, docs landing page, quickstart, installation, and concepts pages
- Added `NebulaConfig` environment variable examples and `from_docker_env()` usage to quickstart guide
- Added NebulaGraph API reference pages (connection, adapter, query, utilities)

## [1.6.0] - 2026-02-17

### Added
- **SPARQL / RDF resource support**: Ingest data from SPARQL endpoints (e.g. Apache Fuseki) and local RDF files (`.ttl`, `.rdf`, `.n3`, `.jsonld`) into property graphs
  - New `SparqlPattern` for mapping `rdf:Class` instances to resources, alongside existing `FilePattern` and `TablePattern`
  - New `RdfDataSource` abstract parent with shared RDF-to-dict conversion logic; concrete subclasses `RdfFileDataSource` (local files via rdflib) and `SparqlEndpointDataSource` (remote endpoints via SPARQLWrapper)
  - New `SparqlEndpointConfig` (extends `DBConfig`) with `from_docker_env()` for Fuseki containers
  - New `RdfInferenceManager` auto-infers graflo `Schema` from OWL/RDFS ontologies: `owl:Class` to vertices, `owl:DatatypeProperty` to fields, `owl:ObjectProperty` to edges
  - `GraphEngine.infer_schema_from_rdf()` and `GraphEngine.create_patterns_from_rdf()` for the RDF inference workflow
  - `Patterns` class extended with `sparql_patterns` and `sparql_configs` dicts
  - `RegistryBuilder` handles `ResourceType.SPARQL` to create the appropriate data sources
  - `ResourceType.SPARQL`, `DataSourceType.SPARQL`, `DBType.SPARQL` enum values
  - `rdflib` and `SPARQLWrapper` available as the `sparql` optional extra (`pip install graflo[sparql]`)
  - Docker scripts (`start-all.sh`, `stop-all.sh`, `cleanup-all.sh`) updated to include Fuseki
  - Test suite with 22 tests: RDF file parsing, ontology inference, and live Fuseki integration

### Changed
- **Top-level imports optimized**: Key classes are now importable directly from `graflo`:
  - `GraphEngine`, `IngestionParams` promoted to top-level alongside existing `Caster`
  - Architecture classes `Resource`, `Vertex`, `VertexConfig`, `Edge`, `EdgeConfig`, `FieldType` now at top-level
  - `FilterExpression` promoted to top-level (alongside existing `ComparisonOperator`, `LogicalOperator`)
  - `InMemoryDataSource` added to top-level data-source exports
  - Import groups reorganized: orchestration, architecture, data sources, database, filters, enums & utilities
- **`graflo.filter` package exports**: `FilterExpression`, `ComparisonOperator`, and `LogicalOperator` are now re-exported from `graflo.filter.__init__` (previously only available via `graflo.filter.onto`)

### Documentation
- Added data-flow diagram (Pattern -> DataSource -> Resource -> GraphContainer -> Target DB) to Concepts page
- Added **Mermaid class diagrams** to Concepts page showing:
  - `GraphEngine` orchestration: how `GraphEngine` delegates to `InferenceManager`, `ResourceMapper`, `Caster`, and `ConnectionManager`
  - `Schema` architecture: the full hierarchy from `Schema` through `VertexConfig`/`EdgeConfig`, `Resource`, `Actor` subtypes, `Field`, and `FilterExpression`
  - `Caster` ingestion pipeline: how `Caster` coordinates `RegistryBuilder`, `DataSourceRegistry`, `DBWriter`, `GraphContainer`, and `ConnectionManager`
- Enabled Mermaid rendering in mkdocs configuration
- Updated top-level package docstring with modern usage example (`GraphEngine` workflow)

## [1.5.0] - 2026-02-02

### Added
- **Ingestion date range**: `IngestionParams` supports `datetime_after`, `datetime_before`, and `datetime_column` so ingestion can be restricted to a date range
  - Use with `GraphEngine.create_patterns(..., datetime_columns={...})` for per-resource datetime columns, or set `IngestionParams.datetime_column` for a single default column
  - Rows are included when the datetime column value is in `[datetime_after, datetime_before)` (inclusive lower, exclusive upper)
  - Applies to SQL/PostgreSQL table ingestion; enables sampling or incremental loads by time window

### Changed
- **Configs use Pydantic**: Schema and all schema-related configs now use Pydantic `BaseModel` (via `ConfigBaseModel`) instead of dataclasses
  - `Schema`, `SchemaMetadata`, `VertexConfig`, `Vertex`, `EdgeConfig`, `Edge`, `Resource`, `WeightConfig`, `Field`, and actor configs are Pydantic models
  - Validation, YAML/dict loading via `model_validate()` / `from_dict()` / `from_yaml()`, and consistent serialization
  - Backward compatible: `resources` accepts empty dict as empty list; field/weight inputs accept strings, `Field` objects, or dicts

## [1.4.5] - 2026-02-02

### Added
- **Inferencer**: Row count estimates and row samples
- **Discard disconnected vertices**: Option to discard disconnected vertices during graph operations

### Changed
- **clean_start**: Refactored into `recreate_schema` and `clear_data` for clearer separation of schema and data reset
- **output_config**: Renamed to `target_db_config`

## [1.4.3] - 2026-01-25

### Added
- **SchemaSanitizer for TigerGraph**: Added comprehensive schema sanitization for TigerGraph compatibility
  - `SchemaSanitizer` class in `graflo.hq.sanitizer` module for sanitizing schema attributes
  - Sanitizes vertex names and field names to avoid reserved words (appends `_vertex` suffix for vertex names, `_attr` for attributes)
  - Sanitizes edge relation names to avoid reserved words and collisions with vertex names (appends `_relation` suffix)
  - Normalizes vertex indexes for TigerGraph: ensures edges with the same relation have consistent source and target indexes
  - Automatically applies field index mappings to resources when indexes are normalized
  - Handles field name transformations in TransformActor instances to maintain data consistency
- **Vertex `dbname` field**: Added `dbname` field to `Vertex` class for database-specific vertex name mapping
  - Allows specifying a different database name than the logical vertex name
  - Used by SchemaSanitizer to store sanitized vertex names for TigerGraph compatibility
- **Edge `relation_dbname` property**: Added `relation_dbname` property to `Edge` class for database-specific relation name mapping
  - Returns sanitized relation name if set, otherwise falls back to `relation` field
  - Used by SchemaSanitizer to store sanitized relation names for TigerGraph compatibility
  - Supports setter for updating the database-specific relation name
- **GraphEngine orchestrator**: Added `GraphEngine` class as the main orchestrator for graph database operations
  - Coordinates schema inference, pattern creation, and data ingestion workflows
  - Provides unified interface: `infer_schema()`, `create_patterns()`, and `ingest()` methods
  - Integrates `InferenceManager`, `ResourceMapper`, and `Caster` components
  - Supports target database flavor configuration for schema sanitization
  - Located in `graflo.hq.graph_engine` module

## [1.4.0] - 2026-01-15

### Removed
- `pyTigergraph` dependence remove

### Added 
- reserved Tigergraph words are modified during automated schema generation

## [1.3.11] - 2026-01-12

### Added
- **TigerGraph Version 4+ Compatibility Enhancements**: Improved support for TigerGraph 4.1+ and 4.2.x versions
  - **Automatic Version Detection**: Connection now auto-detects TigerGraph version and adjusts behavior accordingly
    - Parses version from various formats returned by `getVersion()` API
    - Supports manual version override via `TigergraphConfig.version` field
    - Handles version strings like "release_4.2.2_09-29-2025", "4.2.1", "v4.2.1"
  - **REST API URL Compatibility**: Automatic URL construction based on TigerGraph version
    - TigerGraph 4.2.2+: Uses direct REST API endpoints (no prefix)
    - TigerGraph 4.2.1 and older: Adds `/restpp` prefix to REST API URLs
    - Fixes production deployment issues with TigerGraph 4.2.1
  - **Token-Based Authentication (Recommended)**: Enhanced token authentication support
    - Automatic API token generation from secrets using `getToken()`
    - Bearer token authentication for REST API calls (prioritized over Basic Auth)
    - Stores and reuses tokens for the connection lifetime
    - Token expiration logging for monitoring
    - Fallback to HTTP Basic Auth if token generation fails
  - **Python 3.11+ Exception Compatibility**: Added `@_wrap_tg_exception` decorator
    - Handles `TigerGraphException` objects that lack `add_note()` method (required by Python 3.11+)
    - Wraps exceptions as `RuntimeError` to avoid attribute errors
    - Applied to all key methods: `init_db()`, `create_database()`, `delete_database()`, `_define_schema_local()`, `define_schema()`, `define_indexes()`, `execute()`, `upsert_docs_batch()`
    - Future-proofs for Python 3.11+ while maintaining Python 3.10 compatibility

### Changed
- **TigerGraph Port Configuration for Version 4+**: Updated default ports to align with TigerGraph 4.1+ architecture
  - **Port 9000 (REST++)**: Marked as internal-only in TG 4.1+ (not publicly accessible)
  - **Port 14240 (GSQL Server)**: Now the primary interface for all API requests in TG 4.1+
  - Changed default `port` from 9000 → 14240 in `TigergraphConfig._get_default_port()`
  - Both `restppPort` and `gsPort` now default to 14240 for TigerGraph 4+ compatibility
  - Docker configurations with custom port mappings continue to work via explicit port settings
  - Added comprehensive documentation about port architecture changes in TG 4+
- **Enhanced TigerGraph Documentation**: Added extensive TigerGraph 4+ integration guide
  - Created `docs/tigergraph_v4_guide.md` with comprehensive TG 4+ usage examples
  - Port configuration best practices for vanilla TG 4+ and Docker deployments
  - Token authentication setup and benefits
  - Version compatibility details and migration guide
  - Environment variable configuration examples
  - Troubleshooting guide for common issues
  - Enhanced class docstrings in `TigerGraphConnection` and `TigergraphConfig` with usage examples

### Documentation
- Added comprehensive TigerGraph 4+ integration guide covering:
  - Port configuration changes (9000 → 14240)
  - Token-based authentication (recommended approach)
  - Version compatibility and auto-detection
  - Migration from older TigerGraph versions
  - Best practices for production deployments
  - Environment variable configuration
  - Troubleshooting common connection issues

## [1.3.10] - 2026-01-07

### Added
- **Docker management scripts**: Added unified docker service management scripts
  - `start-all.sh`: Start all docker compose services at once with automatic SPEC detection from `.env` files
  - `stop-all.sh`: Stop all docker compose services with profile-based management
  - `cleanup-all.sh`: Remove containers, volumes, and optionally images with flexible options
  - Automatic detection of `SPEC` variable from each `.env` file (defaults to `graflo`)
  - Profile-based service management for organized docker orchestration
- **Memgraph documentation**: Added comprehensive Memgraph support documentation
  - Added Memgraph to main README.md database support list
  - Added Memgraph configuration examples to quickstart guide
  - Added Memgraph to docker/README.md with connection details (Bolt port 7687)
  - Added Memgraph to documentation reference index
- **TigerGraph robust schema definition and ingestion**: Enhanced TigerGraph support with improved reliability
  - **Schema Change Job Approach**: Uses SCHEMA_CHANGE jobs for local schema definition within graphs
    - More reliable than global vertex/edge creation approach
    - Better integration with TigerGraph's graph-scoped schema model
    - Automatic schema verification after creation to ensure types were created correctly
  - **Automatic Edge Discriminator Handling**: Automatically adds indexed fields to edge weights when missing
    - Required for TigerGraph discriminators (allows multiple edges of same type between same vertices)
    - Ensures discriminator fields are also edge attributes (TigerGraph requirement)
    - Handles both explicit indexes and relation_field for backward compatibility
  - **Robust Edge Ingestion with Fallback**: Enhanced batch edge insertion with automatic fallback
    - Failed batch payloads automatically retry with individual edge upserts
    - Preserves original edge data for fallback operations
    - Better error recovery and data integrity
  - **Improved Error Handling**: More lenient error detection and better error messages
    - Case-insensitive vertex type comparison (handles TigerGraph capitalization)
    - Better error messages with detailed schema verification results
    - Graceful handling of schema change job errors

### Changed
- **Improved connection typing and signatures**: Enhanced type hints and method signatures across all database connectors
  - Improved type annotations for ArangoDB, Neo4j, TigerGraph, FalkorDB, and Memgraph connection classes
  - Better IDE support and type checking for database connection methods
  - Enhanced method signatures for better developer experience
- **Neo4j Community Edition support**: Improved handling of Neo4j Community Edition limitations
  - Gracefully handles unsupported CREATE DATABASE command in Community Edition
  - Automatically continues with default database when database creation fails
  - Clearer error messages indicating Community Edition limitations

## [1.3.9] - 2026-01-06

### Added
- **FalkorDB documentation**: Added comprehensive FalkorDB support documentation across all documentation files
  - Added FalkorDB to main README.md database support list
  - Added FalkorDB to examples and quickstart guides
  - Added FalkorDB web interface access information (port 3001)
  - Added FalkorDB to documentation reference index

### Changed
- **Enhanced PostgreSQL Schema Inference documentation**: Significantly improved documentation clarity and prominence
  - Added explicit requirements section: normalized databases (3NF) with proper primary keys (PK) and foreign keys (FK) decorated
  - Clarified that intelligent heuristics are used to classify tables as vertices or edges
  - Made PostgreSQL schema inference feature more prominent in main documentation (moved to top of Key Features)
  - Added cross-references to Example 5 from multiple documentation locations
  - Enhanced Example 5 with detailed requirements and heuristics explanation
  - Updated all documentation to consistently mention PK/FK requirements and heuristics

- **Database port information updates**: Updated all documentation with correct port numbers from docker .env files
  - ArangoDB: Updated to port 8535 (from docker/arango/.env, standard port 8529)
  - Neo4j: Updated to port 7475 (from docker/neo4j/.env, standard port 7474)
  - TigerGraph: Updated to port 14241 (from docker/tigergraph/.env, standard port 14240)
  - FalkorDB: Port 3001 (from docker/falkordb/.env)
  - Added notes about standard ports vs. configured ports in docker setup

- **Documentation structure improvements**: Enhanced documentation organization
  - Added "Step 2.5: Choose Target Graph Database" section in Example 5
  - Added "Viewing Results in Graph Database Web Interfaces" section with detailed access information
  - Improved examples index to highlight PostgreSQL schema inference feature
  - Enhanced quickstart guide with PostgreSQL schema inference requirements

## [1.3.6] - 2025-12-17

### Added
- **Database-agnostic terminology**: Renamed database-specific terminology to be more generic
  - `Edge.collection_name` → `Edge.database_name`: More generic field name that works across all database types
    - For ArangoDB, `database_name` corresponds to the edge collection name
    - For TigerGraph, used as fallback identifier when relation is not specified
    - For Neo4j, unused (relation is used instead)
  - Updated all references throughout codebase to use `database_name` instead of `collection_name`
  - Removed ArangoDB-specific "collection" terminology from `Vertex` and `VertexConfig` classes
    - Replaced with generic "vertex" or "vertex class" terminology
    - Updated variable names: `_vcollection_numeric_fields_map` → `_vertex_numeric_fields_map`
    - Updated error messages and documentation to use database-agnostic terms

- **Enhanced PostgreSQL example documentation**: Significantly improved Example 5 documentation
  - Added detailed explanations of schema inference process
  - Added visual diagrams showing graph structure, vertex fields, and resource mappings
  - Explained data flow from PostgreSQL to graph database
  - Added step-by-step breakdown of what happens during each phase
  - Included resource mapping diagrams for all table types

- **Improved schema file discovery**: Enhanced `generate_examples_figs.sh` script
  - Now handles files ending with `schema.yaml` (e.g., `generated-schema.yaml`)
  - Uses pattern matching to find schema files instead of hardcoded filename
  - More flexible for generated or custom-named schema files

## [1.3.5] - 2025-12-16

### Added
- **Unified Database Configuration Architecture**: Simplified database configuration system
  - **Capability-based Design**: Replaced `GraphDBConfig`/`SourceDBConfig` hierarchy with capability sets
    - `SOURCE_DATABASES`: Set of database types that can be used as data sources
    - `TARGET_DATABASES`: Set of database types that can be used as targets
    - `can_be_source()`: Method to check if a database can be used as a source
    - `can_be_target()`: Method to check if a database can be used as a target
  - **Unified Schema/Database Structure**: Added unified internal structure for database hierarchy
    - `database`: Database name (for SQL) or backward compatibility field (for graph DBs)
    - `schema_name`: Schema/graph name (unified internal structure)
    - `effective_database`: Property that returns the effective database name based on DB type
    - `effective_schema`: Property that returns the effective schema/graph name based on DB type
    - Database-specific mapping delegated to concrete config classes:
      - **PostgreSQL**: `database` → effective_database, `schema_name` → effective_schema
      - **ArangoDB**: `database` → effective_schema (no database level)
      - **Neo4j**: `database` → effective_schema (no database level)
      - **TigerGraph**: `schema_name` → effective_schema (no database level)
  - **Automatic Schema Fallback**: `Caster` now automatically uses `Schema.general.name` as fallback
    when `effective_schema` is not set in configuration
  - **Environment Variable Support for Schema**: Added `POSTGRES_SCHEMA_NAME` and `TIGERGRAPH_SCHEMA_NAME`
    environment variables for schema configuration

## [1.3.4] - 2025-12-12

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

- **PostgreSQL Schema Inference**: Automatic schema generation from PostgreSQL 3NF databases
  - `PostgresConnection`: PostgreSQL connection and schema introspection implementation
  - `PostgresSchemaInferencer`: Infers complete graflo Schema from PostgreSQL database schemas
    - Automatically identifies vertex-like and edge-like tables
    - Infers vertex configurations with typed fields from table columns
    - Infers edge configurations from foreign key relationships
    - Maps PostgreSQL data types to graflo Field types
  - `PostgresResourceMapper`: Maps PostgreSQL tables to graflo Resources
  - `PostgresTypeMapper`: Converts PostgreSQL types (INTEGER, VARCHAR, TIMESTAMP, etc.) to graflo Field types
  - `infer_schema_from_postgres()`: Convenience function for one-step schema inference
  - `create_resources_from_postgres()`: Creates Resource mappings from PostgreSQL tables
  - Full support for PostgreSQL schema introspection including:
    - Table and column metadata extraction
    - Foreign key relationship detection
    - Primary key identification
    - Data type mapping

- **Typed Fields for Schema Definitions**: Enhanced field type support throughout the schema system
  - **Vertex Fields**: `Vertex.fields` now supports typed `Field` objects in addition to strings
    - Fields can be specified as strings (backward compatible), `Field` objects, or dicts
    - Type information preserved for databases that require it (e.g., TigerGraph)
    - Automatic normalization to `Field` objects internally while maintaining string-like behavior
  - **Edge Weight Fields**: `WeightConfig.direct` now supports typed `Field` objects
    - Weight fields can specify types (e.g., `Field(name="date", type="DATETIME")`)
    - Supports strings, `Field` objects, or dicts for flexible configuration
    - Type information enables better validation and database-specific optimizations
  - **Field Type System**: Comprehensive type support with `FieldType` enum
    - Supported types: `INT`, `FLOAT`, `BOOL`, `STRING`, `DATETIME`
    - Type validation and normalization from strings to enum values
    - Backward compatible: fields without types default to `None` (suitable for databases like ArangoDB)

### Changed
- **Database Configuration Architecture Simplification**: Unified and simplified database configuration
  - **Renamed `BackendType` to `DBType`**: More accurate naming reflecting unified database configuration
    - Updated all references throughout codebase
    - `BACKEND_TYPE_MAPPING` → `DB_TYPE_MAPPING`
  - **Removed Intermediate Config Classes**: Simplified inheritance hierarchy
    - Removed `GraphDBConfig` abstract class
    - Removed `SourceDBConfig` abstract class
    - All config classes (`ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, `PostgresConfig`) now inherit directly from `DBConfig`
    - `connection_type` property moved to base `DBConfig` class
  - **Fixed Field Shadowing Warning**: Renamed `schema` field to `schema_name` to avoid conflict with Pydantic `BaseSettings.schema`
    - Field accepts both `"schema"` and `"schema_name"` keys in dict/JSON input (via validation alias)
    - Environment variables use `SCHEMA_NAME` suffix (e.g., `POSTGRES_SCHEMA_NAME`, `TIGERGRAPH_SCHEMA_NAME`)
  - **Updated ConnectionManager**: Now uses `can_be_target()` method instead of `isinstance` checks
    - More flexible and extensible design
    - Clear error messages when database type cannot be used as target

- **Caster Refactoring**: Updated `Caster` to use data source architecture
  - `process_resource()`: Now accepts configuration dicts, file paths, or in-memory data
  - `ingest()`: Wrapper that creates FileDataSource instances internally (renamed from `ingest_files()`)
  - `ingest_data_sources()`: New method for ingesting from DataSourceRegistry
  - `process_data_source()`: New method for processing individual data sources
  - **Automatic Schema Fallback**: Uses `Schema.general.name` when `effective_schema` is not set
  - **Ingestion Parameters Consolidation**: Refactored `Caster` to use `IngestionParams` as a single attribute
    - Replaced individual attributes (`clean_start`, `n_cores`, `max_items`, `batch_size`, `dry`) with `ingestion_params: IngestionParams`
    - `Caster.__init__()` now accepts `ingestion_params` parameter (backward compatible with kwargs)
    - All ingestion parameters are now centralized in the `IngestionParams` Pydantic model
    - Improved type safety and consistency across ingestion methods
  - Maintains full backward compatibility with existing code

- **Parallel Processing Simplification**: Consolidated threading and multiprocessing parameters
  - Removed redundant `n_threads` parameter from `IngestionParams` and CLI
  - `n_cores` now controls both multiprocessing (number of processes) and threading (ThreadPoolExecutor workers)
  - Simplified API: single parameter controls all parallel execution
  - Updated CLI: removed `--n-threads` option, `--n-cores` now controls both process and thread counts

- **Resource vs DataSource Separation**: Clear separation of concerns
  - Resources: Define semantic transformations (how data becomes a graph)
  - DataSources: Define data retrieval (where data comes from)
  - Many DataSources can map to the same Resource

- **Package Structure Refactoring**: Renamed `backend` package to `db` for clarity
  - `graflo.backend` → `graflo.db` (all database-related code)
  - `graflo.backend.connection` → `graflo.db.connection` (connection configuration)
  - Updated all imports and references throughout codebase
  - Maintains backward compatibility through import aliases where applicable

- **Backend Configuration Refactoring**: Complete refactor of database connection configuration system
  - **Pydantic-based Configuration**: Replaced dataclass-based configs with Pydantic `BaseSettings`
    - `DBConfig`: Abstract base class with `uri`, `username`, `password` fields
    - `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, `PostgresConfig`: Database-specific config classes
    - Environment variable support with prefixes (`ARANGO_`, `NEO4J_`, `TIGERGRAPH_`, `POSTGRES_`)
    - Automatic default port handling when port is missing from URI
    - Support for custom prefixes via `from_env(prefix="USER")` for multiple configs
  - **Renamed `ConnectionKind` to `DBType`**: More accurate naming for database types (final naming: `ConnectionKind` → `BackendType` → `DBType`)
  - **Removed `ConfigFactory`**: Replaced with direct config instantiation and `DBConfig.from_dict()`
    - Use `ArangoConfig.from_docker_env()` to load from docker `.env` files
    - Use `ArangoConfig()`, `Neo4jConfig()`, `TigergraphConfig()`, `PostgresConfig()` for direct instantiation
    - Use `DBConfig.from_dict()` for loading from configuration files
  - **Separated WSGI Configuration**: Moved `WSGIConfig` to separate `wsgi.py` module
    - WSGI is not a database backend, so it no longer inherits from `DBConfig`
    - Removed `WSGI` from `DBType` enum
  - **Backward Compatibility**: `from_dict()` handles old field names (`url` → `uri`, `cred_name` → `username`, etc.)
  - **Breaking Changes**:
    - `ConnectionKind` → `BackendType` → `DBType` (final naming)
    - `ConfigFactory` removed (use `DBConfig.from_dict()` or direct config classes)
    - `*ConnectionConfig` aliases removed (use `*Config` names directly: `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, `PostgresConfig`)
    - `GraphDBConfig` and `SourceDBConfig` removed (all configs inherit from `DBConfig`)
    - `connection_type` field removed (now a computed property from class type)
    - `schema` field renamed to `schema_name` (accepts `"schema"` key in dict/JSON for backward compatibility via validation alias)

### Deprecated
- `ChunkerFactory`: Still functional but now used internally by FileDataSource
- Direct file path processing: Use DataSource configuration for new code

### Fixed
- **File Discovery Path Bug**: Fixed incorrect path combination in `Caster.discover_files()`
  - Previously combined `fpath` with `pattern.sub_path` again, causing `data/data` errors
  - Now correctly uses `fpath` directly as the search directory
  - Fixes `FileNotFoundError` when using `FilePattern` with `sub_path` in ingestion

## [1.2.1] - 2025-01-XX

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

## [1.2.0] - 2025-01-XX

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
- added a single entry point for file ingestion : `ingest` (renamed from `ingest_files`)
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






