"""Example 5: Ingest data from PostgreSQL tables into a graph database.

This example demonstrates:
- Inferring a Schema from PostgreSQL database structure
- Creating Patterns from PostgreSQL tables
- Ingesting data from PostgreSQL into a graph database (ArangoDB/Neo4j)

Prerequisites:
- PostgreSQL database with tables (see mock_schema.sql in this directory for example)
- Target graph database (ArangoDB or Neo4j) running
- Environment variables or .env files configured for both databases
"""

import logging
from pathlib import Path
from suthing import FileHandle

from graflo.onto import DBFlavor
from graflo.db import DBType
from graflo import Caster
from graflo.db.postgres import (
    PostgresConnection,
)
from graflo.db.inferencer import (
    infer_schema_from_postgres,
    create_patterns_from_postgres,
)
from graflo.db.postgres.util import load_schema_from_sql_file
from graflo.db.connection.onto import PostgresConfig, TigergraphConfig
from graflo.caster import IngestionParams

logger = logging.getLogger(__name__)

# Configure logging: INFO level for graflo module, WARNING for others
logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
# Set graflo module to INFO level
logging.getLogger("graflo").setLevel(logging.INFO)

# Step 1: Connect to PostgreSQL (source database)
# Load PostgreSQL config from docker/postgres/.env (recommended)
# This automatically reads POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, etc.
postgres_conf = PostgresConfig.from_docker_env()

# Alternative: Create config directly or use environment variables
# Set POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE env vars
# postgres_conf = PostgresConfig()  # Reads from environment variables
# Or specify directly:
# postgres_conf = PostgresConfig(
#     host="localhost",
#     port=5432,
#     user="postgres",
#     password="postgres",
#     database="postgres",
# )

# Step 2: Connect to target graph database
# You can try different graph databases by uncommenting the desired config below.
# Make sure the corresponding database is running (e.g., via docker-compose).
#
# After ingestion, you can view the results in each database's web interface:
# - ArangoDB: http://localhost:8535 (check ARANGO_PORT in docker/arango/.env, standard port is 8529)
# - Neo4j: http://localhost:7475 (check NEO4J_PORT in docker/neo4j/.env, standard port is 7474)
# - TigerGraph: http://localhost:14241 (check TG_WEB in docker/tigergraph/.env, standard port is 14240)
# - FalkorDB: http://localhost:3001 (check FALKORDB_BROWSER_PORT in docker/falkordb/.env)
#
# Load config from docker/*/.env files (recommended):
# from graflo.db.connection.onto import ArangoConfig, Neo4jConfig, TigergraphConfig, FalkordbConfig
# conn_conf = ArangoConfig.from_docker_env()      # ArangoDB
# conn_conf = Neo4jConfig.from_docker_env()       # Neo4j
conn_conf = TigergraphConfig.from_docker_env()  # TigerGraph
# conn_conf = FalkordbConfig.from_docker_env()    # FalkorDB

# Determine db_flavor from target config
db_type = conn_conf.connection_type
# Map DBType to DBFlavor (they have the same values)
db_flavor = (
    DBFlavor(db_type.value)
    if db_type in (DBType.ARANGO, DBType.NEO4J, DBType.TIGERGRAPH)
    else DBFlavor.ARANGO
)

# Step 1.5: Initialize PostgreSQL database with mock schema if needed
# This ensures the database has the required tables (users, products, purchases, follows)
# Look for mock_schema.sql in the same directory as this script
schema_file = Path(__file__).parent / "data" / "mock_schema.sql"

if schema_file.exists():
    # Connection is automatically managed by load_schema_from_sql_file
    load_schema_from_sql_file(
        config=postgres_conf,
        schema_file=schema_file,
        continue_on_error=True,  # Continue even if some statements fail (e.g., DROP TABLE IF EXISTS)
    )
else:
    logger.warning(f"Mock schema file not found: {schema_file}")
    logger.warning("Assuming PostgreSQL database is already initialized")

# Step 3: Infer Schema from PostgreSQL database structure
# This automatically detects vertex-like and edge-like tables based on:
# - Vertex tables: Have a primary key and descriptive columns
# - Edge tables: Have 2+ foreign keys (representing relationships)
# Connection is automatically closed when exiting the context
with PostgresConnection(postgres_conf) as postgres_conn:
    schema = infer_schema_from_postgres(
        postgres_conn, schema_name="public", db_flavor=db_flavor
    )


schema.general.name = "accounting"
# Step 3.5: Dump inferred schema to YAML file
schema_output_file = Path(__file__).parent / "generated-schema.yaml"

# Convert schema to dict (enums are automatically converted to strings by BaseDataclass.to_dict())
FileHandle.dump(schema.to_dict(), schema_output_file)

logger.info(f"Inferred schema saved to {schema_output_file}")

# Step 4: Create Patterns from PostgreSQL tables
# This maps PostgreSQL tables to resource patterns that Caster can use
# Connection is automatically closed when exiting the context
with PostgresConnection(postgres_conf) as postgres_conn:
    patterns = create_patterns_from_postgres(postgres_conn, schema_name="public")

# Step 5: Create Caster and ingest data
# Note: caster.ingest() will create its own PostgreSQL connections per table internally

caster = Caster(schema)
ingestion_params = IngestionParams(clean_start=True)
caster.ingest(
    output_config=conn_conf, patterns=patterns, ingestion_params=ingestion_params
)

print("\n" + "=" * 80)
print("Ingestion complete!")
print("=" * 80)
print(f"\nSchema: {schema.general.name}")
print(f"Vertices: {len(schema.vertex_config.vertices)}")
print(f"Edges: {len(list(schema.edge_config.edges_list()))}")
print(f"Resources: {len(schema.resources)}")
print("=" * 80)

# View the ingested data in your graph database's web interface:
# - ArangoDB: http://localhost:8535 (check ARANGO_PORT in docker/arango/.env, standard port is 8529)
# - Neo4j: http://localhost:7475 (check NEO4J_PORT in docker/neo4j/.env, standard port is 7474)
# - TigerGraph: http://localhost:14241 (check TG_WEB in docker/tigergraph/.env, standard port is 14240)
# - FalkorDB: http://localhost:3001 (check FALKORDB_BROWSER_PORT in docker/falkordb/.env)
