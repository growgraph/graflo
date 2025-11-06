# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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






