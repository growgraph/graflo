# Example 5: PostgreSQL Schema Inference and Ingestion

This example demonstrates how to automatically infer a graph schema from a PostgreSQL database and ingest data directly from PostgreSQL tables into a graph database. This is particularly useful for migrating relational data to graph databases or creating graph views of existing PostgreSQL databases.

## Overview

Instead of manually defining schemas and exporting data to files, this example shows how to:

- **Automatically detect** vertex-like and edge-like tables in PostgreSQL
- **Infer the graph schema** from the database structure
- **Map PostgreSQL types** to graflo Field types automatically
- **Create patterns** that map PostgreSQL tables to graph resources
- **Ingest data directly** from PostgreSQL into a graph database

## PostgreSQL Database Structure

The example uses a PostgreSQL database with a typical 3NF (Third Normal Form) schema:

### Vertex Tables (Entities)

**`users`** - User accounts:
- `id` (SERIAL PRIMARY KEY) - Unique user identifier
- `name` (VARCHAR) - User full name
- `email` (VARCHAR, UNIQUE) - User email address
- `created_at` (TIMESTAMP) - Account creation timestamp

**`products`** - Product catalog:
- `id` (SERIAL PRIMARY KEY) - Unique product identifier
- `name` (VARCHAR) - Product name
- `price` (DECIMAL) - Product price
- `description` (TEXT) - Product description
- `created_at` (TIMESTAMP) - Product creation timestamp

### Edge Tables (Relationships)

**`purchases`** - Purchase transactions linking users to products:
- `id` (SERIAL PRIMARY KEY) - Unique purchase identifier
- `user_id` (INTEGER, FOREIGN KEY → users.id) - Purchasing user
- `product_id` (INTEGER, FOREIGN KEY → products.id) - Purchased product
- `purchase_date` (TIMESTAMP) - Date and time of purchase
- `quantity` (INTEGER) - Number of items purchased
- `total_amount` (DECIMAL) - Total purchase amount

**`follows`** - User follow relationships (self-referential):
- `id` (SERIAL PRIMARY KEY) - Unique follow relationship identifier
- `follower_id` (INTEGER, FOREIGN KEY → users.id) - User who is following
- `followed_id` (INTEGER, FOREIGN KEY → users.id) - User being followed
- `created_at` (TIMESTAMP) - When the follow relationship was created

## Automatic Schema Inference

The `infer_schema_from_postgres()` function automatically analyzes your PostgreSQL database and creates a complete graflo Schema:

### Detection Heuristics

**Vertex Tables:**
- Have a primary key
- Have descriptive columns (not just foreign keys)
- Represent entities in the domain

**Edge Tables:**
- Have 2+ foreign keys (representing relationships)
- May have additional attributes (weights, timestamps)
- Represent relationships between entities

### Automatic Type Mapping

PostgreSQL types are automatically mapped to graflo Field types:

| PostgreSQL Type | graflo Field Type |
|----------------|-------------------|
| `INTEGER`, `BIGINT`, `SERIAL` | `INT` |
| `VARCHAR`, `TEXT`, `CHAR` | `STRING` |
| `TIMESTAMP`, `DATE`, `TIME` | `DATETIME` |
| `DECIMAL`, `NUMERIC`, `REAL`, `DOUBLE PRECISION` | `FLOAT` |
| `BOOLEAN` | `BOOL` |

### Inferred Schema Structure

The inferred schema automatically includes:

- **Vertices**: `users`, `products` (with typed fields)
- **Edges**: 
  - `users → products` (purchases relationship)
  - `users → users` (follows relationship)
- **Resources**: Automatically created for each table with appropriate actors
- **Indexes**: Primary keys become vertex indexes, foreign keys become edge indexes
- **Weights**: Additional columns in edge tables become edge weight properties

## Step-by-Step Guide

### Step 1: Connect to PostgreSQL

First, establish a connection to your PostgreSQL database:

```python
from graflo.db.postgres import PostgresConnection
from graflo.db.connection.onto import PostgresConfig

# Option 1: Load from docker/postgres/.env (recommended)
postgres_conf = PostgresConfig.from_docker_env()

# Option 2: Load from environment variables
# Set: POSTGRES_URI, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_DATABASE, POSTGRES_SCHEMA_NAME
# postgres_conf = PostgresConfig.from_env()

# Option 3: Create config directly
# postgres_conf = PostgresConfig(
#     uri="postgresql://localhost:5432",
#     username="postgres",
#     password="postgres",
#     database="mydb",
#     schema_name="public"
# )

postgres_conn = PostgresConnection(postgres_conf)
```

### Step 2: Initialize Database (Optional)

If you need to set up the database schema, you can load it from a SQL file:

```python
from pathlib import Path

def load_mock_schema_if_needed(postgres_conn: PostgresConnection) -> None:
    """Load mock schema SQL file into PostgreSQL database if it exists."""
    schema_file = Path("mock_schema.sql")
    
    if not schema_file.exists():
        logger.warning("Mock schema file not found. Assuming database is already initialized.")
        return
    
    logger.info(f"Loading mock schema from {schema_file}")
    with open(schema_file, "r") as f:
        sql_content = f.read()
    
    # Execute SQL statements
    with postgres_conn.conn.cursor() as cursor:
        # Parse and execute statements...
        cursor.execute(sql_content)
        postgres_conn.conn.commit()

load_mock_schema_if_needed(postgres_conn)
```

### Step 3: Infer Schema from PostgreSQL

Automatically generate a graflo Schema from your PostgreSQL database:

```python
from graflo.db.postgres import infer_schema_from_postgres
from graflo.onto import DBFlavor
from graflo.db.connection.onto import ArangoConfig, Neo4jConfig

# Connect to target graph database to determine flavor
target_config = ArangoConfig.from_docker_env()  # or Neo4jConfig, TigergraphConfig

# Determine db_flavor from target config
from graflo.db import DBType
db_type = target_config.connection_type
db_flavor = DBFlavor(db_type.value) if db_type in (DBType.ARANGO, DBType.NEO4J, DBType.TIGERGRAPH) else DBFlavor.ARANGO

# Infer schema automatically
schema = infer_schema_from_postgres(
    postgres_conn,
    schema_name="public",  # PostgreSQL schema name
    db_flavor=db_flavor     # Target graph database flavor
)
```

The inferred schema will have:
- **Vertices**: `users`, `products` with typed fields
- **Edges**: 
  - `users → products` (from `purchases` table)
  - `users → users` (from `follows` table)
- **Resources**: Automatically created for each table

### Step 4: Save Inferred Schema (Optional)

You can save the inferred schema to a YAML file for inspection or modification:

```python
import yaml
from pathlib import Path

schema_output_file = Path("generated-schema.yaml")
schema_dict = schema.to_dict()

with open(schema_output_file, "w") as f:
    yaml.safe_dump(schema_dict, f, default_flow_style=False, sort_keys=False)

logger.info(f"Inferred schema saved to {schema_output_file}")
```

### Step 5: Create Patterns from PostgreSQL Tables

Create `Patterns` that map PostgreSQL tables to resources:

```python
from graflo.db.postgres import create_patterns_from_postgres

# Create patterns from PostgreSQL tables
patterns = create_patterns_from_postgres(
    postgres_conn,
    schema_name="public"
)
```

This creates `TablePattern` instances for each table, which:
- Map table names to resource names
- Store PostgreSQL connection configuration
- Enable the Caster to query data directly from PostgreSQL

### Step 6: Ingest Data into Graph Database

Finally, ingest the data from PostgreSQL into your target graph database:

```python
from graflo import Caster

# Create Caster with inferred schema
caster = Caster(schema)

# Ingest data from PostgreSQL into graph database
from graflo.caster import IngestionParams

ingestion_params = IngestionParams(
    clean_start=True,  # Clear existing data first
)

caster.ingest(
    output_config=target_config,  # Target graph database config
    patterns=patterns,             # PostgreSQL table patterns
    ingestion_params=ingestion_params,
)

# Cleanup
postgres_conn.close()
```

## Complete Example

Here's the complete example combining all steps:

```python
import logging
from pathlib import Path
import yaml

from graflo import Caster
from graflo.onto import DBFlavor
from graflo.db import DBType
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

# Step 2: Initialize database with mock schema if needed
# (Implementation details omitted - see full example in examples/5-ingest-postgres/ingest.py)

# Step 3: Connect to target graph database
target_config = ArangoConfig.from_docker_env()  # or Neo4jConfig, TigergraphConfig

# Step 4: Infer Schema from PostgreSQL database structure
db_type = target_config.connection_type
db_flavor = (
    DBFlavor(db_type.value)
    if db_type in (DBType.ARANGO, DBType.NEO4J, DBType.TIGERGRAPH)
    else DBFlavor.ARANGO
)

schema = infer_schema_from_postgres(
    postgres_conn,
    schema_name="public",
    db_flavor=db_flavor
)

# Step 5: Save inferred schema to YAML (optional)
schema_output_file = Path("generated-schema.yaml")
with open(schema_output_file, "w") as f:
    yaml.safe_dump(schema.to_dict(), f, default_flow_style=False, sort_keys=False)
logger.info(f"Inferred schema saved to {schema_output_file}")

# Step 6: Create Patterns from PostgreSQL tables
patterns = create_patterns_from_postgres(postgres_conn, schema_name="public")

# Step 7: Create Caster and ingest data
from graflo.caster import IngestionParams

caster = Caster(schema)

ingestion_params = IngestionParams(
    clean_start=True,  # Clear existing data first
)

caster.ingest(
    output_config=target_config,
    patterns=patterns,
    ingestion_params=ingestion_params,
)

# Cleanup
postgres_conn.close()

print("\n" + "=" * 80)
print("Ingestion complete!")
print("=" * 80)
print(f"Schema: {schema.general.name}")
print(f"Vertices: {len(schema.vertex_config.vertices)}")
print(f"Edges: {len(list(schema.edge_config.edges_list()))}")
print(f"Resources: {len(schema.resources)}")
print("=" * 80)
```

## Generated Schema Example

The inferred schema will look like this:

```yaml
general:
    name: public
vertex_config:
    vertices:
    -   name: products
        fields:
        -   name: id
            type: INT
        -   name: name
            type: STRING
        -   name: price
            type: FLOAT
        -   name: description
            type: STRING
        -   name: created_at
            type: DATETIME
        indexes:
        -   fields: [id]
    -   name: users
        fields:
        -   name: id
            type: INT
        -   name: name
            type: STRING
        -   name: email
            type: STRING
        -   name: created_at
            type: DATETIME
        indexes:
        -   fields: [id]
edge_config:
    edges:
    -   source: users
        target: products
        weights:
            direct:
            -   name: purchase_date
            -   name: quantity
            -   name: total_amount
    -   source: users
        target: users
        weights:
            direct:
            -   name: created_at
resources:
-   resource_name: products
    apply:
    -   vertex: products
-   resource_name: users
    apply:
    -   vertex: users
-   resource_name: purchases
    apply:
    -   target_vertex: users
        map:
            user_id: id
    -   target_vertex: products
        map:
            product_id: id
-   resource_name: follows
    apply:
    -   target_vertex: users
        map:
            follower_id: id
    -   target_vertex: users
        map:
            followed_id: id
```

## Key Features

### Automatic Type Mapping

PostgreSQL types are automatically converted to graflo Field types with proper type information:

- **Integer types** (`INTEGER`, `BIGINT`, `SERIAL`) → `INT`
- **String types** (`VARCHAR`, `TEXT`, `CHAR`) → `STRING`
- **Numeric types** (`DECIMAL`, `NUMERIC`, `REAL`) → `FLOAT`
- **Date/Time types** (`TIMESTAMP`, `DATE`, `TIME`) → `DATETIME`
- **Boolean types** → `BOOL`

### Intelligent Table Classification

The system automatically classifies tables as:

- **Vertex tables**: Tables with primary keys and descriptive columns
- **Edge tables**: Tables with 2+ foreign keys representing relationships

### Automatic Resource Creation

Resources are automatically created for each table with appropriate actors:

- **Vertex tables**: Create `VertexActor` to map rows to vertices
- **Edge tables**: Create `EdgeActor` with proper field mappings for source and target vertices

### Type-Safe Field Definitions

All fields in the inferred schema include type information, enabling:

- Better validation during ingestion
- Database-specific optimizations
- Type-aware filtering and querying

### Data Type Handling

Special handling for PostgreSQL-specific types:

- **Decimal/Numeric**: Automatically converted to `float` when reading from PostgreSQL
- **DateTime**: Preserved as `datetime` objects during processing, serialized to ISO format for JSON
- **Type preservation**: Original types are preserved for accurate duplicate detection

## Graph Structure

The resulting graph structure shows:

- **Vertices**: `users` and `products` with their properties
- **Edges**: 
  - `users → products` (purchases) with weight properties: `purchase_date`, `quantity`, `total_amount`
  - `users → users` (follows) with weight property: `created_at`

## Benefits

1. **No manual schema definition**: Schema is automatically inferred from existing database structure
2. **Direct database access**: No need to export data to files first
3. **Automatic resource mapping**: Tables are automatically mapped to graph resources
4. **Type safety**: Proper handling of PostgreSQL-specific types with automatic conversion
5. **Flexible**: Works with any 3NF PostgreSQL schema
6. **Time-saving**: Reduces manual configuration significantly
7. **Maintainable**: Schema can be regenerated when database structure changes

## Use Cases

This pattern is particularly useful for:

- **Data Migration**: Migrating relational data to graph databases
- **Graph Views**: Creating graph views of existing PostgreSQL databases
- **Relationship Analysis**: Analyzing relationships in normalized database schemas
- **Graph Analytics**: Building graph analytics on top of transactional databases
- **Legacy System Integration**: Integrating legacy relational systems with modern graph databases
- **Data Warehousing**: Transforming relational data warehouses into graph structures

## Customization

### Modifying the Inferred Schema

After inference, you can modify the schema:

```python
# Infer schema
schema = infer_schema_from_postgres(postgres_conn, schema_name="public")

# Modify schema as needed
# Add custom transforms, filters, or additional edges
schema.vertex_config.vertices[0].filters.append(...)

# Use modified schema
caster = Caster(schema)
```

### Manual Pattern Creation

You can also create patterns manually for more control:

```python
from graflo.util.onto import Patterns, TablePattern

patterns = Patterns(
    _resource_mapping={
        "users": ("db1", "users"),      # (config_key, table_name)
        "products": ("db1", "products"),
    },
    _postgres_connections={
        "db1": postgres_conf,  # Maps config_key to PostgresConfig
    }
)
```

## Key Takeaways

1. **Automatic schema inference** eliminates manual schema definition for 3NF databases
2. **Type mapping** ensures proper type handling across PostgreSQL and graph databases
3. **Direct database access** enables efficient data ingestion without intermediate files
4. **Flexible heuristics** automatically detect vertices and edges from table structure
5. **Type-safe fields** provide better validation and database-specific optimizations
6. **Resource generation** automatically creates appropriate actors for each table
7. **Schema customization** allows modifications after inference for specific use cases

## Next Steps

- Explore the [PostgreSQL Schema Inference API](../reference/db/postgres/schema_inference.md) for advanced usage
- Learn about [PostgreSQL Type Mapping](../reference/db/postgres/types.md) for custom type conversions
- Check out [Resource Mapping](../reference/db/postgres/resource_mapping.md) for custom resource creation
- See the [full example code](https://github.com/growgraph/graflo/tree/main/examples/5-ingest-postgres) for complete implementation

For more examples and detailed explanations, refer to the [API Reference](../reference/index.md).
