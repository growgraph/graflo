from pathlib import Path

from graflo.db import PostgresConnection
from graflo.db.connection.onto import PostgresConfig
from graflo.db.postgres.heuristics import logger


def load_schema_from_sql_file(
    config: PostgresConfig,
    schema_file: str | Path,
    continue_on_error: bool = True,
) -> None:
    """Load SQL schema file into PostgreSQL database.

    Parses a SQL file and executes all statements sequentially. Useful for
    initializing a database with tables, constraints, and initial data.

    Args:
        config: PostgreSQL connection configuration
        schema_file: Path to SQL file to execute
        continue_on_error: If True, continue executing remaining statements
                          even if one fails. If False, raise exception on first error.

    Raises:
        FileNotFoundError: If schema_file does not exist
        Exception: If continue_on_error is False and a statement fails
    """
    schema_path = Path(schema_file)

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    logger.info(f"Loading schema from {schema_path}")

    # Read SQL file
    with open(schema_path, "r") as f:
        sql_content = f.read()

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

    if not statements:
        logger.warning(f"No SQL statements found in {schema_path}")
        return

    # Execute statements using a connection context manager
    with PostgresConnection(config) as conn:
        with conn.conn.cursor() as cursor:
            for statement in statements:
                if statement:
                    try:
                        cursor.execute(statement)
                    except Exception as exec_error:
                        if continue_on_error:
                            # Some statements might fail (like DROP TABLE IF EXISTS when tables don't exist)
                            # or duplicate constraints - log but continue
                            logger.debug(f"Statement execution note: {exec_error}")
                        else:
                            logger.error(
                                f"Failed to execute statement: {statement[:100]}... Error: {exec_error}"
                            )
                            raise

            conn.conn.commit()

    logger.info(
        f"Successfully loaded schema from {schema_path} ({len(statements)} statements)"
    )
