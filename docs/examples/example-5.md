# Example 5: Ingesting Data from PostgreSQL Tables

This example demonstrates how to automatically infer a graph schema from a PostgreSQL database and ingest data directly from PostgreSQL tables into a graph database.

## Overview

Instead of manually defining schemas and loading data from files, this example shows how to:
- Automatically detect vertex-like and edge-like tables in PostgreSQL
- Infer the graph schema from the database structure
- Create patterns that map PostgreSQL tables to graph resources
- Ingest data directly from PostgreSQL into a graph database

## PostgreSQL Database Structure

The example uses a PostgreSQL database with a typical 3NF (Third Normal Form) schema:

- **Vertex tables** (entities with primary keys):
  - `users`: User information with id, name, email, created_at
  - `products`: Product information with id, name, price, description

- **Edge tables** (relationships with foreign keys):
  - `purchases`: Links users to products (user_id → product_id) with quantity and price
  - `follows`: Links users to users (follower_id → followed_id) with created_at

## Automatic Schema Inference

The `infer_schema_from_postgres()` function automatically:

1. **Detects vertex tables**: Tables with a primary key and descriptive columns
2. **Detects edge tables**: Tables with 2+ foreign keys (representing relationships)
3. **Maps field types**: Converts PostgreSQL types (INT, VARCHAR, TIMESTAMP, DECIMAL) to graflo Field types
4. **Creates resources**: Automatically generates Resource configurations with appropriate actors

### Inferred Schema Structure

```python
from graflo.db.postgres import PostgresConnection, infer_schema_from_postgres
from graflo.db.connection.onto import PostgresConfig

# Connect to PostgreSQL
postgres_conf = PostgresConfig.from_docker_env()
postgres_conn = PostgresConnection(postgres_conf)

# Infer schema automatically
schema = infer_schema_from_postgres(postgres_conn, schema_name="public")
```

The inferred schema will have:
- **Vertices**: `users`, `products`
- **Edges**: `users → products` (purchases), `users → users` (follows)
- **Resources**: Automatically created for each table with appropriate actors

## Pattern Creation

The `create_patterns_from_postgres()` function creates `Patterns` that map PostgreSQL tables to resources:

```python
from graflo.db.postgres import create_patterns_from_postgres

# Create patterns from PostgreSQL tables
patterns = create_patterns_from_postgres(postgres_conn, schema_name="public")
```

This creates `TablePattern` instances for each table, which:
- Map table names to resource names
- Store PostgreSQL connection configuration
- Enable the Caster to query data directly from PostgreSQL

## Complete Example

```python
import logging
from pathlib import Path

from graflo import Caster
from graflo.db.postgres import (
    PostgresConnection,
    create_patterns_from_postgres,
    infer_schema_from_postgres,
)
from graflo.db.connection.onto import ArangoConfig, PostgresConfig

logger = logging.getLogger(__name__)

# Step 1: Connect to PostgreSQL (source database)
postgres_conf = PostgresConfig.from_docker_env()
postgres_conn = PostgresConnection(postgres_conf)

# Step 1.5: Initialize database with mock schema if needed
docker_dir = Path(__file__).parent.parent.parent / "docker" / "postgres"
schema_file = docker_dir / "mock_schema.sql"

if schema_file.exists():
    logger.info(f"Loading mock schema from {schema_file}")
    # ... (load and execute SQL schema) ...

# Step 2: Infer Schema from PostgreSQL database structure
schema = infer_schema_from_postgres(postgres_conn, schema_name="public")

# Step 3: Create Patterns from PostgreSQL tables
patterns = create_patterns_from_postgres(postgres_conn, schema_name="public")

# Step 4: Connect to target graph database (ArangoDB)
conn_conf = ArangoConfig.from_docker_env()

# Step 5: Create Caster and ingest data
caster = Caster(schema)
caster.ingest(output_config=conn_conf, patterns=patterns, clean_start=True)

# Cleanup
postgres_conn.close()
```

## Key Features

### Automatic Type Mapping

PostgreSQL types are automatically mapped to graflo Field types:
- `INTEGER`, `BIGINT` → `INT`
- `VARCHAR`, `TEXT` → `STRING`
- `TIMESTAMP`, `DATE`, `TIME` → `DATETIME`
- `DECIMAL`, `NUMERIC` → `FLOAT` (converted to float when reading)

### Vertex/Edge Detection Heuristics

The system uses heuristics to classify tables:

**Vertex tables:**
- Have a primary key
- Have descriptive columns (not just foreign keys)
- Represent entities in the domain

**Edge tables:**
- Have 2+ foreign keys
- Represent relationships between entities
- May have additional attributes (weights, timestamps)

### Data Type Handling

- **Decimal/Numeric**: Automatically converted to `float` when reading from PostgreSQL
- **DateTime**: Preserved as `datetime` objects during processing, serialized to ISO format for JSON
- **Type preservation**: Original types are preserved in `pick_unique_dict` for accurate duplicate detection

## Benefits

1. **No manual schema definition**: Schema is inferred from existing database structure
2. **Direct database access**: No need to export data to files first
3. **Automatic resource mapping**: Tables are automatically mapped to graph resources
4. **Type safety**: Proper handling of PostgreSQL-specific types (Decimal, DateTime)
5. **Flexible**: Works with any 3NF PostgreSQL schema

## Use Cases

- Migrating relational data to graph databases
- Creating graph views of existing PostgreSQL databases
- Analyzing relationships in normalized database schemas
- Building graph analytics on top of transactional databases
