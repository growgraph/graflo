"""PostgreSQL connection implementation for schema introspection.

This module implements PostgreSQL connection and schema introspection functionality,
specifically designed to analyze 3NF schemas and identify vertex-like and edge-like tables.

Key Features:
    - Connection management using psycopg2
    - Schema introspection (tables, columns, constraints)
    - Vertex/edge table detection heuristics
    - Structured schema information extraction

Example:
    >>> from graflo.db.postgres import PostgresConnection
    >>> from graflo.db.connection.onto import PostgresConfig
    >>> config = PostgresConfig.from_docker_env()
    >>> conn = PostgresConnection(config)
    >>> schema_info = conn.introspect_schema()
    >>> print(schema_info["vertex_tables"])
    >>> conn.close()
"""

import logging
from typing import Any
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor

from graflo.db.connection.onto import PostgresConfig

logger = logging.getLogger(__name__)


class PostgresConnection:
    """PostgreSQL connection for schema introspection.

    This class provides PostgreSQL-specific functionality for connecting to databases
    and introspecting 3NF schemas to identify vertex-like and edge-like tables.

    Attributes:
        config: PostgreSQL connection configuration
        conn: psycopg2 connection instance
    """

    def __init__(self, config: PostgresConfig):
        """Initialize PostgreSQL connection.

        Args:
            config: PostgreSQL connection configuration containing URI and credentials
        """
        self.config = config

        # Parse URI to extract connection parameters
        if config.uri is None:
            raise ValueError("PostgreSQL connection requires a URI to be configured")

        parsed = urlparse(config.uri)

        # Extract connection parameters
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        database = config.database or parsed.path.lstrip("/") or "postgres"
        user = config.username or parsed.username or "postgres"
        password = config.password or parsed.password

        # Build connection parameters dict
        conn_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
        }

        if password:
            conn_params["password"] = password

        try:
            self.conn = psycopg2.connect(**conn_params)
            logger.info(f"Successfully connected to PostgreSQL database '{database}'")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
            raise

    def read(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        """Execute a SELECT query and return results as a list of dictionaries.

        Args:
            query: SQL SELECT query to execute
            params: Optional tuple of parameters for parameterized queries

        Returns:
            List of dictionaries, where each dictionary represents a row with column names as keys
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    def close(self):
        """Close the PostgreSQL connection."""
        if hasattr(self, "conn") and self.conn:
            try:
                self.conn.close()
                logger.debug("PostgreSQL connection closed")
            except Exception as e:
                logger.warning(
                    f"Error closing PostgreSQL connection: {e}", exc_info=True
                )

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        """Get all tables in the specified schema.

        Args:
            schema_name: Schema name to query. If None, uses 'public' or config schema_name.

        Returns:
            List of table information dictionaries with keys: table_name, table_schema
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        query = """
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (schema_name,))
            return [dict(row) for row in cursor.fetchall()]

    def get_table_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Get columns for a specific table with types and descriptions.

        Args:
            table_name: Name of the table
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of column information dictionaries with keys:
            name, type, description, is_nullable, column_default
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        query = """
            SELECT
                c.column_name as name,
                c.data_type as type,
                c.udt_name as udt_name,
                c.character_maximum_length,
                c.is_nullable,
                c.column_default,
                COALESCE(d.description, '') as description
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_statio_all_tables st
                ON st.schemaname = c.table_schema
                AND st.relname = c.table_name
            LEFT JOIN pg_catalog.pg_description d
                ON d.objoid = st.relid
                AND d.objsubid = c.ordinal_position
            WHERE c.table_schema = %s
              AND c.table_name = %s
            ORDER BY c.ordinal_position;
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (schema_name, table_name))
            columns = []
            for row in cursor.fetchall():
                col_dict = dict(row)
                # Format type with length if applicable
                if col_dict["character_maximum_length"]:
                    col_dict["type"] = (
                        f"{col_dict['type']}({col_dict['character_maximum_length']})"
                    )
                # Use udt_name if it's more specific (e.g., varchar, int4)
                if col_dict["udt_name"] and col_dict["udt_name"] != col_dict["type"]:
                    col_dict["type"] = col_dict["udt_name"]
                # Remove helper fields
                col_dict.pop("character_maximum_length", None)
                col_dict.pop("udt_name", None)
                columns.append(col_dict)
            return columns

    def get_primary_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        """Get primary key columns for a table.

        Args:
            table_name: Name of the table
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of primary key column names
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position;
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema_name, table_name))
            return [row[0] for row in cursor.fetchall()]

    def get_foreign_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Get foreign key relationships for a table.

        Args:
            table_name: Name of the table
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of foreign key dictionaries with keys:
            column, references_table, references_column, constraint_name
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        query = """
            SELECT
                kcu.column_name as column,
                ccu.table_name as references_table,
                ccu.column_name as references_column,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position;
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (schema_name, table_name))
            return [dict(row) for row in cursor.fetchall()]

    def detect_vertex_tables(
        self, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Detect vertex-like tables in the schema.

        Heuristic: Tables with a primary key and descriptive columns
        (not just foreign keys). These represent entities.

        Note: Tables with exactly 2 foreign keys are considered edge tables
        and are excluded from vertex tables.

        Args:
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of vertex table information dictionaries
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        tables = self.get_tables(schema_name)
        vertex_tables = []

        for table_info in tables:
            table_name = table_info["table_name"]
            pk_columns = self.get_primary_keys(table_name, schema_name)
            fk_columns = self.get_foreign_keys(table_name, schema_name)
            all_columns = self.get_table_columns(table_name, schema_name)

            # Vertex-like tables have:
            # 1. A primary key
            # 2. Not exactly 2 foreign keys (those are edge tables)
            # 3. Descriptive columns beyond just foreign keys

            if not pk_columns:
                continue  # Skip tables without primary keys

            # Skip tables with exactly 2 FKs (these are edge tables)
            if len(fk_columns) == 2:
                continue

            # Count non-FK, non-PK columns (descriptive columns)
            fk_column_names = {fk["column"] for fk in fk_columns}
            pk_column_names = set(pk_columns)
            descriptive_columns = [
                col
                for col in all_columns
                if col["name"] not in fk_column_names
                and col["name"] not in pk_column_names
            ]

            # If table has descriptive columns, consider it vertex-like
            if descriptive_columns:
                # Mark primary key columns
                pk_set = set(pk_columns)
                for col in all_columns:
                    col["is_pk"] = col["name"] in pk_set

                vertex_tables.append(
                    {
                        "name": table_name,
                        "schema": schema_name,
                        "columns": all_columns,
                        "primary_key": pk_columns,
                        "foreign_keys": fk_columns,
                    }
                )

        return vertex_tables

    def detect_edge_tables(
        self, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Detect edge-like tables in the schema.

        Heuristic: Junction tables with exactly 2 foreign keys pointing to other tables.
        These represent relationships between entities.

        Args:
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of edge table information dictionaries with source_table and target_table
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        tables = self.get_tables(schema_name)
        edge_tables = []

        for table_info in tables:
            table_name = table_info["table_name"]
            fk_columns = self.get_foreign_keys(table_name, schema_name)

            # Edge-like tables have exactly 2 foreign keys
            if len(fk_columns) == 2:
                all_columns = self.get_table_columns(table_name, schema_name)
                pk_columns = self.get_primary_keys(table_name, schema_name)

                # Mark primary key columns
                pk_set = set(pk_columns)
                for col in all_columns:
                    col["is_pk"] = col["name"] in pk_set

                # Determine source and target tables
                source_fk = fk_columns[0]
                target_fk = fk_columns[1]

                edge_tables.append(
                    {
                        "name": table_name,
                        "schema": schema_name,
                        "columns": all_columns,
                        "primary_key": pk_columns,
                        "foreign_keys": fk_columns,
                        "source_table": source_fk["references_table"],
                        "target_table": target_fk["references_table"],
                        "source_column": source_fk["column"],
                        "target_column": target_fk["column"],
                    }
                )

        return edge_tables

    def introspect_schema(self, schema_name: str | None = None) -> dict[str, Any]:
        """Introspect the database schema and return structured information.

        This is the main method that analyzes the schema and returns information
        about vertex-like and edge-like tables.

        Args:
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            Dictionary with keys:
            - vertex_tables: List of vertex table information
            - edge_tables: List of edge table information
            - schema_name: The schema name that was analyzed
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        logger.info(f"Introspecting PostgreSQL schema '{schema_name}'")

        vertex_tables = self.detect_vertex_tables(schema_name)
        edge_tables = self.detect_edge_tables(schema_name)

        # Mark primary key columns in column lists
        for table_info in vertex_tables + edge_tables:
            pk_set = set(table_info["primary_key"])
            for col in table_info["columns"]:
                col["is_pk"] = col["name"] in pk_set

        result = {
            "vertex_tables": vertex_tables,
            "edge_tables": edge_tables,
            "schema_name": schema_name,
        }

        logger.info(
            f"Found {len(vertex_tables)} vertex-like tables and {len(edge_tables)} edge-like tables"
        )

        return result
