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
    >>> print(schema_info.vertex_tables)
    >>> conn.close()
"""

import logging
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from graflo.architecture.onto_sql import (
    ColumnInfo,
    ForeignKeyInfo,
    VertexTableInfo,
    EdgeTableInfo,
    SchemaIntrospectionResult,
)
from graflo.db.connection.onto import PostgresConfig

from .inference_utils import (
    FuzzyMatchCache,
    infer_edge_vertices_from_table_name,
    infer_vertex_from_column_name,
)

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

    def read(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        """Execute a SELECT query and return results as a list of dictionaries.

        Args:
            query: SQL SELECT query to execute
            params: Optional tuple of parameters for parameterized queries

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
                COALESCE(dsc.description, '') as description
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

    def _is_edge_like_table(
        self, table_name: str, pk_columns: list[str], fk_columns: list[dict[str, Any]]
    ) -> bool:
        """Determine if a table is edge-like based on heuristics.

        Heuristics:
        1. Tables with 2 or more primary keys are likely edge tables
        2. Tables with exactly 2 foreign keys are likely edge tables
        3. Tables with names starting with 'rel_' are likely edge tables
        4. Tables where primary key columns match foreign key columns are likely edge tables

        Args:
            table_name: Name of the table
            pk_columns: List of primary key column names
            fk_columns: List of foreign key dictionaries

        Returns:
            True if table appears to be edge-like, False otherwise
        """
        # Heuristic 1: Tables with 2 or more primary keys are likely edge tables
        if len(pk_columns) >= 2:
            return True

        # Heuristic 2: Tables with exactly 2 foreign keys are likely edge tables
        if len(fk_columns) == 2:
            return True

        # Heuristic 3: Tables with names starting with 'rel_' are likely edge tables
        if table_name.startswith("rel_"):
            return True

        # Heuristic 4: If primary key columns match foreign key columns, it's likely an edge table
        fk_column_names = {fk["column"] for fk in fk_columns}
        pk_set = set(pk_columns)
        # If all PK columns are FK columns and we have at least 2 FKs, it's likely an edge table
        if pk_set.issubset(fk_column_names) and len(fk_columns) >= 2:
            return True

        return False

    def detect_vertex_tables(
        self, schema_name: str | None = None
    ) -> list[VertexTableInfo]:
        """Detect vertex-like tables in the schema.

        Heuristic: Tables with a primary key and descriptive columns
        (not just foreign keys). These represent entities.

        Note: Tables identified as edge-like are excluded from vertex tables.

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
            # 2. Not identified as edge-like tables
            # 3. Descriptive columns beyond just foreign keys

            if not pk_columns:
                continue  # Skip tables without primary keys

            # Skip edge-like tables
            if self._is_edge_like_table(table_name, pk_columns, fk_columns):
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
                # Mark primary key columns and convert to ColumnInfo
                pk_set = set(pk_columns)
                column_infos = []
                for col in all_columns:
                    column_infos.append(
                        ColumnInfo(
                            name=col["name"],
                            type=col["type"],
                            description=col.get("description", ""),
                            is_nullable=col.get("is_nullable", "YES"),
                            column_default=col.get("column_default"),
                            is_pk=col["name"] in pk_set,
                        )
                    )

                # Convert foreign keys to ForeignKeyInfo
                fk_infos = []
                for fk in fk_columns:
                    fk_infos.append(
                        ForeignKeyInfo(
                            column=fk["column"],
                            references_table=fk["references_table"],
                            references_column=fk.get("references_column"),
                            constraint_name=fk.get("constraint_name"),
                        )
                    )

                vertex_tables.append(
                    VertexTableInfo(
                        name=table_name,
                        schema_name=schema_name,
                        columns=column_infos,
                        primary_key=pk_columns,
                        foreign_keys=fk_infos,
                    )
                )

        return vertex_tables

    def detect_edge_tables(
        self,
        schema_name: str | None = None,
        vertex_table_names: list[str] | None = None,
    ) -> list[EdgeTableInfo]:
        """Detect edge-like tables in the schema.

        Heuristic: Tables with 2 or more primary keys, or exactly 2 foreign keys,
        or names starting with 'rel_'. These represent relationships between entities.

        Args:
            schema_name: Schema name. If None, uses 'public' or config schema_name.
            vertex_table_names: Optional list of vertex table names for fuzzy matching.
                              If None, will be inferred from detect_vertex_tables().

        Returns:
            List of edge table information dictionaries with source_table and target_table
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        # Get vertex table names if not provided
        if vertex_table_names is None:
            vertex_tables = self.detect_vertex_tables(schema_name)
            vertex_table_names = [vt.name for vt in vertex_tables]

        # Create fuzzy match cache once for all tables (significant performance improvement)
        match_cache = FuzzyMatchCache(vertex_table_names)

        tables = self.get_tables(schema_name)
        edge_tables = []

        for table_info in tables:
            table_name = table_info["table_name"]
            pk_columns = self.get_primary_keys(table_name, schema_name)
            fk_columns = self.get_foreign_keys(table_name, schema_name)

            # Skip tables without primary keys
            if not pk_columns:
                continue

            # Check if table is edge-like
            if not self._is_edge_like_table(table_name, pk_columns, fk_columns):
                continue

            all_columns = self.get_table_columns(table_name, schema_name)

            # Mark primary key columns and convert to ColumnInfo
            pk_set = set(pk_columns)
            column_infos = []
            for col in all_columns:
                column_infos.append(
                    ColumnInfo(
                        name=col["name"],
                        type=col["type"],
                        description=col.get("description", ""),
                        is_nullable=col.get("is_nullable", "YES"),
                        column_default=col.get("column_default"),
                        is_pk=col["name"] in pk_set,
                    )
                )

            # Convert foreign keys to ForeignKeyInfo
            fk_infos = []
            for fk in fk_columns:
                fk_infos.append(
                    ForeignKeyInfo(
                        column=fk["column"],
                        references_table=fk["references_table"],
                        references_column=fk.get("references_column"),
                        constraint_name=fk.get("constraint_name"),
                    )
                )

            # Determine source and target tables
            source_table = None
            target_table = None
            source_column = None
            target_column = None
            relation_name = None

            # If we have exactly 2 foreign keys, use them directly
            if len(fk_infos) == 2:
                source_fk = fk_infos[0]
                target_fk = fk_infos[1]
                source_table = source_fk.references_table
                target_table = target_fk.references_table
                source_column = source_fk.column
                target_column = target_fk.column
                # Still try to infer relation from table name
                fk_dicts = [
                    {
                        "column": fk.column,
                        "references_table": fk.references_table,
                    }
                    for fk in fk_infos
                ]
                _, _, relation_name = infer_edge_vertices_from_table_name(
                    table_name, pk_columns, fk_dicts, vertex_table_names, match_cache
                )
            # If we have 2 or more primary keys, try to infer from table name and structure
            elif len(pk_columns) >= 2:
                # Convert fk_infos to dicts for _infer_edge_vertices_from_table_name
                fk_dicts = [
                    {
                        "column": fk.column,
                        "references_table": fk.references_table,
                    }
                    for fk in fk_infos
                ]

                # Try to infer from table name pattern
                inferred_source, inferred_target, relation_name = (
                    infer_edge_vertices_from_table_name(
                        table_name,
                        pk_columns,
                        fk_dicts,
                        vertex_table_names,
                        match_cache,
                    )
                )

                if inferred_source and inferred_target:
                    source_table = inferred_source
                    target_table = inferred_target
                    # Try to match PK columns to FK columns for source/target columns
                    if fk_infos:
                        # Use first FK for source, second for target if available
                        if len(fk_infos) >= 2:
                            source_column = fk_infos[0].column
                            target_column = fk_infos[1].column
                        elif len(fk_infos) == 1:
                            # Self-reference case
                            source_column = fk_infos[0].column
                            target_column = fk_infos[0].column
                    else:
                        # Use PK columns as source/target columns
                        source_column = pk_columns[0]
                        target_column = (
                            pk_columns[1] if len(pk_columns) > 1 else pk_columns[0]
                        )
                elif fk_infos:
                    # Fallback: use FK references if available
                    if len(fk_infos) >= 2:
                        source_table = fk_infos[0].references_table
                        target_table = fk_infos[1].references_table
                        source_column = fk_infos[0].column
                        target_column = fk_infos[1].column
                    elif len(fk_infos) == 1:
                        source_table = fk_infos[0].references_table
                        target_table = fk_infos[0].references_table
                        source_column = fk_infos[0].column
                        target_column = fk_infos[0].column
                else:
                    # Last resort: use PK columns and infer table names from column names
                    source_column = pk_columns[0]
                    target_column = (
                        pk_columns[1] if len(pk_columns) > 1 else pk_columns[0]
                    )
                    # Use robust inference logic to extract vertex names from column names
                    source_table = infer_vertex_from_column_name(
                        source_column, vertex_table_names, match_cache
                    )
                    target_table = infer_vertex_from_column_name(
                        target_column, vertex_table_names, match_cache
                    )

            # Only add if we have source and target information
            if source_table and target_table:
                edge_tables.append(
                    EdgeTableInfo(
                        name=table_name,
                        schema_name=schema_name,
                        columns=column_infos,
                        primary_key=pk_columns,
                        foreign_keys=fk_infos,
                        source_table=source_table,
                        target_table=target_table,
                        source_column=source_column or pk_columns[0],
                        target_column=target_column
                        or (pk_columns[1] if len(pk_columns) > 1 else pk_columns[0]),
                        relation=relation_name,
                    )
                )
            else:
                logger.warning(
                    f"Could not determine source/target tables for edge-like table '{table_name}'. "
                    f"Skipping."
                )

        return edge_tables

    def introspect_schema(
        self, schema_name: str | None = None
    ) -> SchemaIntrospectionResult:
        """Introspect the database schema and return structured information.

        This is the main method that analyzes the schema and returns information
        about vertex-like and edge-like tables.

        Args:
            schema_name: Schema name. If None, uses 'public' or config schema_name.

        Returns:
            SchemaIntrospectionResult with vertex_tables, edge_tables, and schema_name
        """
        if schema_name is None:
            schema_name = self.config.schema_name or "public"

        logger.info(f"Introspecting PostgreSQL schema '{schema_name}'")

        vertex_tables = self.detect_vertex_tables(schema_name)
        edge_tables = self.detect_edge_tables(schema_name)

        result = SchemaIntrospectionResult(
            vertex_tables=vertex_tables,
            edge_tables=edge_tables,
            schema_name=schema_name,
        )

        logger.info(
            f"Found {len(vertex_tables)} vertex-like tables and {len(edge_tables)} edge-like tables"
        )

        return result
