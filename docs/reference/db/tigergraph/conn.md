# `graflo.db.tigergraph.conn`

TigerGraph connection implementation for graph database operations.

::: graflo.db.tigergraph.conn

## Overview

The `TigerGraphConnection` class provides a robust implementation for TigerGraph database operations, including schema definition, data ingestion, and query execution. It uses TigerGraph's SCHEMA_CHANGE job system for reliable schema management and REST++ API for efficient batch operations.

## Key Features

### Schema Definition

- **SCHEMA_CHANGE Job Approach**: Uses TigerGraph's SCHEMA_CHANGE jobs for local schema definition within graphs
  - More reliable than global vertex/edge creation
  - Better integration with TigerGraph's graph-scoped schema model
  - Automatic schema verification after creation

- **Automatic Edge Discriminator Handling**: Automatically handles edge discriminators for multiple edges of the same type between the same vertices
  - Automatically adds indexed fields to edge weights when missing (TigerGraph requirement)
  - Ensures discriminator fields are also edge attributes
  - Supports both explicit indexes and relation_field for backward compatibility

### Data Ingestion

- **Robust Batch Operations**: Enhanced batch vertex and edge insertion with automatic fallback
  - Failed batch payloads automatically retry with individual upserts
  - Preserves original edge data for fallback operations
  - Better error recovery and data integrity

- **Composite Primary Keys**: Supports both single-field PRIMARY_ID and composite PRIMARY KEY syntax
  - Single-field indexes use PRIMARY_ID syntax (required by GSQL and GraphStudio)
  - Composite keys use PRIMARY KEY syntax (works in GSQL, not GraphStudio UI)

### Error Handling

- **Improved Error Detection**: More lenient error detection and better error messages
  - Case-insensitive vertex type comparison (handles TigerGraph capitalization)
  - Detailed schema verification results in error messages
  - Graceful handling of schema change job errors

## Usage Example

```python
from graflo import Caster, Schema
from graflo.db.connection.onto import TigergraphConfig

# Load config from docker environment
config = TigergraphConfig.from_docker_env()

# Create connection
conn = TigerGraphConnection(config)

# Initialize database with schema
schema = Schema.from_dict(...)
conn.init_db(schema, clean_start=True)

# Batch upsert vertices
conn.upsert_docs_batch(docs, "User", match_keys=["email"])

# Batch insert edges
conn.insert_edges_batch(
    edges_data,
    source_class="User",
    target_class="Company",
    relation_name="works_at",
    match_keys_source=("email",),
    match_keys_target=("name",)
)
```

## Schema Definition Details

### Vertex Types

Vertices are created using `ADD VERTEX` statements in SCHEMA_CHANGE jobs:

- **Single-field primary key**: Uses `PRIMARY_ID` syntax with `PRIMARY_ID_AS_ATTRIBUTE="true"` for REST++ API compatibility
- **Composite primary key**: Uses `PRIMARY KEY` syntax (note: GraphStudio UI doesn't support composite keys)

### Edge Types

Edges are created using `ADD DIRECTED EDGE` statements with automatic discriminator handling:

- **Discriminators**: Automatically added for all indexed fields to support multiple edges of the same type between the same vertices
- **Edge Attributes**: Discriminator fields are automatically added to edge weights if missing
- **Format**: `DISCRIMINATOR(field1 TYPE1, field2 TYPE2)` clause included in edge definition

## API Reference

See the auto-generated API documentation below for complete method signatures and parameters.
