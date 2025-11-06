# API Reference

This section provides detailed API documentation for all graflo components and modules.

## Architecture

Core architectural components that define the graflo framework:

- **[Schema](architecture/schema.md)**: Graph schema definition and management
- **[Vertex](architecture/vertex.md)**: Vertex configuration and properties
- **[Edge](architecture/edge.md)**: Edge configuration and relationship management
- **[Resource](architecture/resource.md)**: Data source mapping and transformation
- **[Actor](architecture/actor.md)**: Document processing pipeline components
- **[Transform](architecture/transform.md)**: Data transformation utilities
- **[Ontology](architecture/onto.md)**: Core data structures and types
- **[Utilities](architecture/util.md)**: Common utility functions

## Database Operations

Database connection and management components:

- **[Connection Manager](db/manager.md)**: Database connection lifecycle management
- **[ArangoDB](db/arango/)**:
  - [Connection](db/arango/conn.md): ArangoDB-specific connection implementation
  - [Query](db/arango/query.md): AQL query execution and utilities
  - [Utilities](db/arango/util.md): ArangoDB-specific utility functions
- **[Neo4j](db/neo4j/)**:
  - [Connection](db/neo4j/conn.md): Neo4j-specific connection implementation
- **[TigerGraph](db/tigergraph/)**:
  - [Connection](db/tigergraph/conn.md): TigerGraph-specific connection implementation with REST++ API and GSQL support

## Core Components

Main graflo functionality:

- **[Caster](caster.md)**: Main data ingestion and transformation engine
- **[Ontology](onto.md)**: Core data types and enums

## Utilities

Helper modules and utilities:

- **[Chunker](util/chunker.md)**: Data chunking and batching utilities
- **[Merge](util/merge.md)**: Data merging and deduplication
- **[Transform](util/transform.md)**: Data transformation utilities
- **[Miscellaneous](util/misc.md)**: Other utility functions

## Filtering

Data filtering and query capabilities:

- **[Ontology](filter/onto.md)**: Filter expression system and operators

## Visualization

Graph visualization and plotting:

- **[Plotter](plot/plotter.md)**: Graph visualization and schema plotting utilities

## Command Line Interface

CLI tools for graflo operations:

- **[Ingest](cli/ingest.md)**: Data ingestion commands
- **[Database Management](cli/manage_dbs.md)**: Database administration commands
- **[Schema Visualization](cli/plot_schema.md)**: Schema visualization commands
- **[XML to JSON](cli/xml2json.md)**: XML data conversion utilities

## Getting Started

- [Installation](../getting_started/installation.md)
- [Quick Start Guide](../getting_started/quickstart.md)
- [Concepts](../concepts/index.md)
- [Examples](../examples/index.md)

## Contributing

For information on contributing to graflo, see the [Contributing Guide](../contributing.md).
