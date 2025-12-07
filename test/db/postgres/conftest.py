"""Pytest fixtures for PostgreSQL tests."""

import logging
from pathlib import Path

import pytest

from graflo.db.postgres import PostgresConnection
from graflo.db.connection.onto import PostgresConfig

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def conn_conf():
    """Load PostgreSQL config from docker/postgres/.env file."""
    conn_conf = PostgresConfig.from_docker_env()
    # Ensure database is set
    if not conn_conf.database:
        conn_conf.database = "postgres"
    return conn_conf


@pytest.fixture(scope="function")
def postgres_conn(conn_conf):
    """Create a PostgreSQL connection for testing."""
    conn = PostgresConnection(conn_conf)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def load_mock_schema(postgres_conn):
    """Load the mock schema SQL file into the database."""
    # Get the path to mock_schema.sql
    docker_dir = Path(__file__).parent.parent.parent.parent / "docker" / "postgres"
    schema_file = docker_dir / "mock_schema.sql"

    if not schema_file.exists():
        pytest.skip(f"Mock schema file not found: {schema_file}")

    # Read and execute the SQL file
    with open(schema_file, "r") as f:
        sql_content = f.read()

    # Execute the SQL statements one by one
    # psycopg2 doesn't support multiple statements in a single execute() call
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

        # Execute remaining statement if any (in case file doesn't end with semicolon)
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

    yield

    # Cleanup: Drop all tables
    with postgres_conn.conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS follows CASCADE")
        cursor.execute("DROP TABLE IF EXISTS purchases CASCADE")
        cursor.execute("DROP TABLE IF EXISTS products CASCADE")
        cursor.execute("DROP TABLE IF EXISTS users CASCADE")
        postgres_conn.conn.commit()

    logger.info("Mock schema cleaned up")
