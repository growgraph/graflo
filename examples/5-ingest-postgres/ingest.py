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
import yaml

from graflo.onto import DBFlavor
from graflo.db import DBType
from graflo import Caster
from graflo.db.postgres import (
    PostgresConnection,
    create_patterns_from_postgres,
    infer_schema_from_postgres,
)
from graflo.db.connection.onto import PostgresConfig, Neo4jConfig

logger = logging.getLogger(__name__)


def load_mock_schema_if_needed(postgres_conn: PostgresConnection) -> None:
    """Load mock schema SQL file into PostgreSQL database if it exists.

    Args:
        postgres_conn: PostgreSQL connection instance
    """
    # Look for mock_schema.sql in the same directory as this script
    schema_file = Path(__file__).parent / "mock_schema.sql"

    if not schema_file.exists():
        logger.warning(f"Mock schema file not found: {schema_file}")
        logger.warning("Assuming PostgreSQL database is already initialized")
        return

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
load_mock_schema_if_needed(postgres_conn)

# Step 2: Connect to target graph database (Neo4j or ArangoDB)
# Load config from docker/neo4j/.env or docker/arango/.env (recommended)
conn_conf = Neo4jConfig.from_docker_env()

# Alternative: Use ArangoDB instead
# from graflo.db.connection.onto import ArangoConfig
# conn_conf = ArangoConfig.from_docker_env()

# Step 3: Infer Schema from PostgreSQL database structure
# This automatically detects vertex-like and edge-like tables based on:
# - Vertex tables: Have a primary key and descriptive columns
# - Edge tables: Have 2+ foreign keys (representing relationships)
# Get db_flavor from target database config

# Determine db_flavor from target config
db_type = conn_conf.connection_type
# Map DBType to DBFlavor (they have the same values)
db_flavor = (
    DBFlavor(db_type.value)
    if db_type in (DBType.ARANGO, DBType.NEO4J, DBType.TIGERGRAPH)
    else DBFlavor.ARANGO
)

schema = infer_schema_from_postgres(
    postgres_conn, schema_name="public", db_flavor=db_flavor
)

# Step 3.5: Dump inferred schema to YAML file
schema_output_file = Path(__file__).parent / "generated-schema.yaml"

# Convert schema to dict (enums are automatically converted to strings by BaseDataclass.to_dict())
schema_dict = schema.to_dict()

# Write to YAML file
with open(schema_output_file, "w") as f:
    yaml.safe_dump(schema_dict, f, default_flow_style=False, sort_keys=False)

logger.info(f"Inferred schema saved to {schema_output_file}")

# Step 4: Create Patterns from PostgreSQL tables
# This maps PostgreSQL tables to resource patterns that Caster can use
patterns = create_patterns_from_postgres(postgres_conn, schema_name="public")

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
