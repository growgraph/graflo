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
    >>> from graflo.connections.onto import PostgresConfig
    >>> config = PostgresConfig.from_docker_env()
    >>> conn = PostgresConnection(config)
    >>> schema_info = conn.introspect_schema()
    >>> print(schema_info.vertex_tables)
    >>> conn.close()
"""

import logging
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from graflo.architecture.onto_sql import (
    EdgeTableInfo,
    RawTableInfo,
    SchemaIntrospectionResult,
    VertexTableInfo,
)
from graflo.connections.onto import PostgresConfig
from graflo.db import sql as sql_introspect
from graflo.db.conn import Connection
from graflo.db.postgres.target_write import PostgresTargetWriteMixin

logger = logging.getLogger(__name__)


class PostgresConnection(PostgresTargetWriteMixin, Connection):
    """PostgreSQL connection for schema introspection and graph target writes.

    This class provides PostgreSQL-specific functionality for connecting to databases
    and introspecting 3NF schemas to identify vertex-like and edge-like tables.
    It also supports writing graph data to vertex and edge junction tables.

    Attributes:
        config: PostgreSQL connection configuration
        conn: psycopg2 connection instance
    """

    def __init__(self, config: PostgresConfig):
        """Initialize PostgreSQL connection.

        Args:
            config: PostgreSQL connection configuration containing URI and credentials
        """
        super().__init__()
        self.config = config

        # Validate required config values
        if config.uri is None:
            raise ValueError("PostgreSQL connection requires a URI to be configured")
        if config.database is None:
            raise ValueError(
                "PostgreSQL connection requires a database name to be configured"
            )

        # Use config properties directly - all fallbacks are handled in PostgresConfig
        host = config.hostname or "localhost"
        port = int(config.port) if config.port else 5432
        database = config.database
        user = config.username or "postgres"
        password = config.password

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

    def read(
        self, query: str, params: tuple | dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a SELECT query and return results as a list of dictionaries.

        Args:
            query: SQL SELECT query to execute
            params: Optional parameters — a tuple for ``%s`` placeholders, or a
                mapping for named ``%(name)s`` ones.

        Returns:
            List of dictionaries, where each dictionary represents a row with column names as keys.
            Decimal values are converted to float for compatibility with graph databases.
        """
        from decimal import Decimal

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Convert rows to dictionaries and convert Decimal to float
            results = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                # Convert Decimal to float for JSON/graph database compatibility
                for key, value in row_dict.items():
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                results.append(row_dict)

            return results

    def __enter__(self):
        """Enter the context manager.

        Returns:
            PostgresConnection: Self for use in 'with' statements
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit the context manager.

        Ensures the connection is properly closed when exiting the context.

        Args:
            exc_type: Exception type if an exception occurred
            exc_value: Exception value if an exception occurred
            exc_traceback: Exception traceback if an exception occurred
        """
        self.close()
        return False  # Don't suppress exceptions

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

    def _check_information_schema_reliable(self, schema_name: str) -> bool:
        """Check if information_schema is reliable for the given schema.

        Args:
            schema_name: Schema name to check

        Returns:
            True if information_schema appears reliable, False otherwise
        """
        try:
            # Try to query information_schema.tables
            query = """
                SELECT COUNT(*) as count
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
            """
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (schema_name,))
                result = cursor.fetchone()
                # If query succeeds, check if we can also query constraints
                pk_query = """
                    SELECT COUNT(*) as count
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = %s
                """
                cursor.execute(pk_query, (schema_name,))
                pk_result = cursor.fetchone()
                # If both queries work, information_schema seems reliable
                return result is not None and pk_result is not None
        except Exception as e:
            logger.debug(f"information_schema check failed: {e}")
            return False

    def _get_tables_pg_catalog(self, schema_name: str) -> list[dict[str, Any]]:
        """Get all tables using pg_catalog (fallback method).

        Args:
            schema_name: Schema name to query

        Returns:
            List of table information dictionaries with keys: table_name, table_schema
        """
        query = """
            SELECT
                c.relname as table_name,
                n.nspname as table_schema
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relkind = 'r'
              AND NOT c.relispartition
            ORDER BY c.relname;
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (schema_name,))
            return [dict(row) for row in cursor.fetchall()]

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        """Get all tables in the specified schema.

        Tries information_schema first, falls back to pg_catalog if needed.

        Args:
            schema_name: Schema name to query. If None, uses 'public' or config schema_name.

        Returns:
            List of table information dictionaries with keys: table_name, table_schema
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        # Try information_schema first
        try:
            query = """
                SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """

            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (schema_name,))
                results = [dict(row) for row in cursor.fetchall()]
                # If we got results, check if information_schema is reliable
                if results and self._check_information_schema_reliable(schema_name):
                    return results
                # If no results or unreliable, fall back to pg_catalog
                logger.debug(
                    f"information_schema returned no results or is unreliable, "
                    f"falling back to pg_catalog for schema '{schema_name}'"
                )
        except Exception as e:
            logger.debug(
                f"information_schema query failed: {e}, falling back to pg_catalog"
            )

        # Fallback to pg_catalog
        return self._get_tables_pg_catalog(schema_name)

    def _get_table_columns_pg_catalog(
        self, table_name: str, schema_name: str
    ) -> list[dict[str, Any]]:
        """Get columns using pg_catalog (fallback method).

        Args:
            table_name: Name of the table
            schema_name: Schema name

        Returns:
            List of column information dictionaries with keys:
            name, type, description, is_nullable, column_default
        """
        query = """
            SELECT
                a.attname as name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable,
                pg_catalog.pg_get_expr(d.adbin, d.adrelid) as column_default,
                COALESCE(dsc.description, '') as description,
                a.attnum as ordinal_position
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            LEFT JOIN pg_catalog.pg_description dsc ON dsc.objoid = a.attrelid AND dsc.objsubid = a.attnum
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum;
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (schema_name, table_name))
            columns = []
            for row in cursor.fetchall():
                col_dict = dict(row)
                # Normalize type format
                if col_dict["type"]:
                    # Remove length info from type if present (e.g., "character varying(255)" -> "varchar")
                    type_str = col_dict["type"]
                    if "(" in type_str:
                        base_type = type_str.split("(")[0]
                        # Map common types
                        type_mapping = {
                            "character varying": "varchar",
                            "character": "char",
                            "double precision": "float8",
                            "real": "float4",
                            "integer": "int4",
                            "bigint": "int8",
                            "smallint": "int2",
                        }
                        col_dict["type"] = type_mapping.get(
                            base_type.lower(), base_type.lower()
                        )
                    else:
                        type_mapping = {
                            "character varying": "varchar",
                            "character": "char",
                            "double precision": "float8",
                            "real": "float4",
                            "integer": "int4",
                            "bigint": "int8",
                            "smallint": "int2",
                        }
                        col_dict["type"] = type_mapping.get(
                            type_str.lower(), type_str.lower()
                        )
                columns.append(col_dict)
            return columns

    def get_table_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Get columns for a specific table with types and descriptions.

        Tries information_schema first, falls back to pg_catalog if needed.

        Args:
            table_name: Name of the table
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of column information dictionaries with keys:
            name, type, description, is_nullable, column_default
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        # Try information_schema first
        try:
            query = """
                SELECT
                    c.column_name as name,
                    c.data_type as type,
                    c.udt_name as udt_name,
                    c.character_maximum_length,
                    c.is_nullable,
                    c.column_default,
                    COALESCE(d.description, '') as description,
                    c.ordinal_position as ordinal_position
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
                    if (
                        col_dict["udt_name"]
                        and col_dict["udt_name"] != col_dict["type"]
                    ):
                        col_dict["type"] = col_dict["udt_name"]
                    # Remove helper fields
                    col_dict.pop("character_maximum_length", None)
                    col_dict.pop("udt_name", None)
                    columns.append(col_dict)

                # If we got results and information_schema is reliable, return them
                if columns and self._check_information_schema_reliable(schema_name):
                    return columns
                # Otherwise fall back to pg_catalog
                logger.debug(
                    f"information_schema returned no results or is unreliable, "
                    f"falling back to pg_catalog for table '{schema_name}.{table_name}'"
                )
        except Exception as e:
            logger.debug(
                f"information_schema query failed: {e}, falling back to pg_catalog"
            )

        # Fallback to pg_catalog
        return self._get_table_columns_pg_catalog(table_name, schema_name)

    def _get_primary_keys_pg_catalog(
        self, table_name: str, schema_name: str
    ) -> list[str]:
        """Get primary key columns using pg_catalog (fallback method).

        Args:
            table_name: Name of the table
            schema_name: Schema name

        Returns:
            List of primary key column names
        """
        query = """
            SELECT a.attname
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
            WHERE n.nspname = %s
              AND c.relname = %s
              AND con.contype = 'p'
            ORDER BY array_position(con.conkey, a.attnum);
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema_name, table_name))
            return [row[0] for row in cursor.fetchall()]

    def get_primary_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        """Get primary key columns for a table.

        Tries information_schema first, falls back to pg_catalog if needed.

        Args:
            table_name: Name of the table
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of primary key column names
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        # Try information_schema first
        try:
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
                results = [row[0] for row in cursor.fetchall()]
                # If we got results and information_schema is reliable, return them
                if results and self._check_information_schema_reliable(schema_name):
                    return results
                # Otherwise fall back to pg_catalog
                logger.debug(
                    f"information_schema returned no results or is unreliable, "
                    f"falling back to pg_catalog for primary keys of '{schema_name}.{table_name}'"
                )
        except Exception as e:
            logger.debug(
                f"information_schema query failed: {e}, falling back to pg_catalog"
            )

        # Fallback to pg_catalog
        return self._get_primary_keys_pg_catalog(table_name, schema_name)

    def _get_unique_columns_pg_catalog(
        self, table_name: str, schema_name: str
    ) -> list[str]:
        """Get columns in UNIQUE constraints using pg_catalog (fallback method)."""
        query = """
            SELECT a.attname
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
            WHERE n.nspname = %s
              AND c.relname = %s
              AND con.contype = 'u'
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY array_position(con.conkey, a.attnum);
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema_name, table_name))
            return list(dict.fromkeys(row[0] for row in cursor.fetchall()))

    def get_unique_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        """Get column names that participate in any UNIQUE constraint.

        Tries information_schema first, falls back to pg_catalog if needed.
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        try:
            query = """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'UNIQUE'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s
                ORDER BY kcu.ordinal_position;
            """
            with self.conn.cursor() as cursor:
                cursor.execute(query, (schema_name, table_name))
                results = list(dict.fromkeys(row[0] for row in cursor.fetchall()))
                if results and self._check_information_schema_reliable(schema_name):
                    return results
        except Exception as e:
            logger.debug(
                f"information_schema query failed for unique columns: {e}, "
                "falling back to pg_catalog"
            )
        return self._get_unique_columns_pg_catalog(table_name, schema_name)

    def _get_foreign_keys_pg_catalog(
        self, table_name: str, schema_name: str
    ) -> list[dict[str, Any]]:
        """Get foreign key relationships using pg_catalog (fallback method).

        Handles both single-column and multi-column foreign keys.
        For multi-column foreign keys, returns one row per column.

        Args:
            table_name: Name of the table
            schema_name: Schema name

        Returns:
            List of foreign key dictionaries with keys:
            column, references_table, references_column, constraint_name
        """
        # Use generate_subscripts for better compatibility with older PostgreSQL versions
        query = """
            SELECT
                a.attname as column,
                ref_c.relname as references_table,
                ref_a.attname as references_column,
                con.conname as constraint_name
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_class ref_c ON ref_c.oid = con.confrelid
            JOIN generate_subscripts(con.conkey, 1) AS i ON true
            JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = con.conkey[i]
            JOIN pg_catalog.pg_attribute ref_a ON ref_a.attrelid = con.confrelid AND ref_a.attnum = con.confkey[i]
            WHERE n.nspname = %s
              AND c.relname = %s
              AND con.contype = 'f'
            ORDER BY con.conname, i;
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (schema_name, table_name))
            return [dict(row) for row in cursor.fetchall()]

    def get_foreign_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Get foreign key relationships for a table.

        Tries information_schema first, falls back to pg_catalog if needed.

        Args:
            table_name: Name of the table
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            List of foreign key dictionaries with keys:
            column, references_table, references_column, constraint_name
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        # Try information_schema first
        try:
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
                results = [dict(row) for row in cursor.fetchall()]
                # If we got results and information_schema is reliable, return them
                if self._check_information_schema_reliable(schema_name):
                    return results
                # Otherwise fall back to pg_catalog
                logger.debug(
                    f"information_schema returned no results or is unreliable, "
                    f"falling back to pg_catalog for foreign keys of '{schema_name}.{table_name}'"
                )
        except Exception as e:
            logger.debug(
                f"information_schema query failed: {e}, falling back to pg_catalog"
            )

        # Fallback to pg_catalog
        return self._get_foreign_keys_pg_catalog(table_name, schema_name)

    def get_table_row_count_estimate(
        self, table_name: str, schema_name: str | None = None
    ) -> int | None:
        """Return approximate row count from pg_class.reltuples (updated by ANALYZE).

        Avoids full table scan; may be stale until next ANALYZE.
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"
        query = """
            SELECT c.reltuples::bigint AS estimate
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r';
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema_name, table_name))
            row = cursor.fetchone()
            if row is None:
                return None
            val = row[0]
            return int(val) if val is not None else None

    def get_table_sample_rows(
        self,
        table_name: str,
        schema_name: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Return first `limit` rows from the table (no ORDER BY for speed)."""
        if schema_name is None:
            schema_name = self.config.schema_name or "public"
        query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.debug(
                f"Could not fetch sample rows for '{schema_name}.{table_name}': {e}"
            )
            return []

    # ------------------------------------------------------------------
    # Schema introspection
    #
    # The classification below is dialect-neutral and lives in graflo.db.sql;
    # this class contributes the `pg_catalog` implementation of the metadata
    # primitives it reads. These wrappers stay because they are public API and
    # because `self` satisfies SqlMetadataProvider structurally.
    # ------------------------------------------------------------------

    def _default_schema(self) -> str:
        return self.config.schema_name or "public"

    def _is_edge_like_table(
        self, table_name: str, pk_columns: list[str], fk_columns: list[dict[str, Any]]
    ) -> bool:
        """Whether *table_name* looks like a relationship rather than an entity."""
        return sql_introspect.is_edge_like_table(table_name, pk_columns, fk_columns)

    def detect_vertex_tables(
        self, schema_name: str | None = None
    ) -> list[VertexTableInfo]:
        """Entity-like tables in the schema."""
        return sql_introspect.detect_vertex_tables(
            self, schema_name, default_schema=self._default_schema()
        )

    def detect_edge_tables(
        self,
        schema_name: str | None = None,
        vertex_table_names: list[str] | None = None,
    ) -> list[EdgeTableInfo]:
        """Relationship-like tables, with their recovered endpoints."""
        return sql_introspect.detect_edge_tables(
            self,
            schema_name,
            vertex_table_names,
            default_schema=self._default_schema(),
        )

    def _build_raw_tables(self, schema_name: str) -> list[RawTableInfo]:
        """Unclassified metadata for every table, including sampled values."""
        return sql_introspect.build_raw_tables(self, schema_name)

    def introspect_schema(
        self,
        schema_name: str | None = None,
        include_raw_tables: bool = False,
    ) -> SchemaIntrospectionResult:
        """Classify the schema into vertex-like and edge-like tables."""
        return sql_introspect.introspect_schema(
            self,
            schema_name,
            include_raw_tables,
            default_schema=self._default_schema(),
        )
