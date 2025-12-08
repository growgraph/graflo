"""Example 5: Ingest data from PostgreSQL tables into a graph database.

This example demonstrates:
- Inferring a Schema from PostgreSQL database structure
- Creating Patterns from PostgreSQL tables
- Ingesting data from PostgreSQL into a graph database (ArangoDB/Neo4j)

Prerequisites:
- PostgreSQL database with tables (see docker/postgres/mock_schema.sql for example)
- Target graph database (ArangoDB or Neo4j) running
- Environment variables or .env files configured for both databases
"""

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

# Create PostgreSQL connection
postgres_conn = PostgresConnection(postgres_conf)

# Step 1.5: Initialize PostgreSQL database with mock schema if needed
# This ensures the database has the required tables (users, products, purchases, follows)
docker_dir = Path(__file__).parent.parent.parent / "docker" / "postgres"
schema_file = docker_dir / "mock_schema.sql"

if schema_file.exists():
    logger.info(f"Loading mock schema from {schema_file}")
    with open(schema_file, "r") as f:
        sql_content = f.read()

    # Execute the SQL statements one by one
    with postgres_conn.conn.cursor() as cursor:
        # Parse SQL content into individual statements
        statements = []
        current_statement = []
        for line in sql_content.split("\n"):
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith("--"):
                continue
            current_statement.append(line)
            # Check if line ends with semicolon (end of statement)
            if line.endswith(";"):
                statement = " ".join(current_statement).rstrip(";").strip()
                if statement:
                    statements.append(statement)
                current_statement = []

        # Execute remaining statement if any
        if current_statement:
            statement = " ".join(current_statement).strip()
            if statement:
                statements.append(statement)

        # Execute each statement
        for statement in statements:
            if statement:
                try:
                    cursor.execute(statement)
                except Exception as exec_error:
                    # Some statements might fail (like DROP TABLE IF EXISTS when tables don't exist)
                    # or duplicate constraints - log but continue
                    logger.debug(f"Statement execution note: {exec_error}")

        postgres_conn.conn.commit()
    logger.info("Mock schema loaded successfully")
else:
    logger.warning(f"Mock schema file not found: {schema_file}")
    logger.warning("Assuming PostgreSQL database is already initialized")

# Step 2: Infer Schema from PostgreSQL database structure
# This automatically detects vertex-like and edge-like tables based on:
# - Vertex tables: Have a primary key and descriptive columns
# - Edge tables: Have 2+ foreign keys (representing relationships)
schema = infer_schema_from_postgres(postgres_conn, schema_name="public")

# Step 3: Create Patterns from PostgreSQL tables
# This maps PostgreSQL tables to resource patterns that Caster can use
patterns = create_patterns_from_postgres(postgres_conn, schema_name="public")

# Step 4: Connect to target graph database (ArangoDB)
# Load config from docker/arango/.env (recommended)
conn_conf = ArangoConfig.from_docker_env()

# Alternative: Use Neo4j instead
# from graflo.db.connection.onto import Neo4jConfig
# conn_conf = Neo4jConfig.from_docker_env()

# Step 5: Create Caster and ingest data
caster = Caster(schema)
caster.ingest(output_config=conn_conf, patterns=patterns, clean_start=True)

# Cleanup: Close PostgreSQL connection
postgres_conn.close()

print("\n" + "=" * 80)
print("Ingestion complete!")
print("=" * 80)
print(f"\nSchema: {schema.general.name}")
print(f"Vertices: {len(schema.vertex_config.vertices)}")
print(f"Edges: {len(list(schema.edge_config.edges_list()))}")
print(f"Resources: {len(schema.resources)}")
print("=" * 80)
