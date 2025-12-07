"""Type mapping utilities for PostgreSQL to graflo type conversion.

This module provides utilities for mapping PostgreSQL data types to graflo Field types,
enabling automatic schema inference from PostgreSQL database schemas.
"""

import logging

logger = logging.getLogger(__name__)


class PostgresTypeMapper:
    """Maps PostgreSQL data types to graflo Field types.

    This class provides static methods for converting PostgreSQL type names
    (from information_schema or pg_catalog) to graflo Field type strings.
    """

    # Mapping of PostgreSQL types to graflo Field types
    TYPE_MAPPING = {
        # Integer types
        "integer": "INT",
        "int": "INT",
        "int4": "INT",
        "smallint": "INT",
        "int2": "INT",
        "bigint": "INT",
        "int8": "INT",
        "serial": "INT",
        "bigserial": "INT",
        "smallserial": "INT",
        # Floating point types
        "real": "FLOAT",
        "float4": "FLOAT",
        "double precision": "FLOAT",
        "float8": "FLOAT",
        "numeric": "FLOAT",
        "decimal": "FLOAT",
        # Boolean
        "boolean": "BOOL",
        "bool": "BOOL",
        # String types
        "character varying": "STRING",
        "varchar": "STRING",
        "character": "STRING",
        "char": "STRING",
        "text": "STRING",
        # Date/time types (mapped to DATETIME)
        "timestamp": "DATETIME",
        "timestamp without time zone": "DATETIME",
        "timestamp with time zone": "DATETIME",
        "timestamptz": "DATETIME",
        "date": "DATETIME",
        "time": "DATETIME",
        "time without time zone": "DATETIME",
        "time with time zone": "DATETIME",
        "timetz": "DATETIME",
        "interval": "STRING",  # Interval is duration, keep as STRING
        # JSON types
        "json": "STRING",
        "jsonb": "STRING",
        # Binary types
        "bytea": "STRING",
        # UUID
        "uuid": "STRING",
    }

    @classmethod
    def map_type(cls, postgres_type: str) -> str:
        """Map PostgreSQL type to graflo Field type.

        Args:
            postgres_type: PostgreSQL type name (e.g., 'int4', 'varchar', 'timestamp')

        Returns:
            str: graflo Field type (INT, FLOAT, BOOL, or STRING)
        """
        # Normalize type name: lowercase and remove length specifications
        normalized = postgres_type.lower().strip()

        # Remove length specifications like (255) or (10,2)
        if "(" in normalized:
            normalized = normalized.split("(")[0].strip()

        # Check direct mapping
        if normalized in cls.TYPE_MAPPING:
            return cls.TYPE_MAPPING[normalized]

        # Check for partial matches (e.g., "character varying" might be stored as "varying")
        for pg_type, graflo_type in cls.TYPE_MAPPING.items():
            if pg_type in normalized or normalized in pg_type:
                logger.debug(
                    f"Mapped PostgreSQL type '{postgres_type}' to graflo type '{graflo_type}' "
                    f"(partial match with '{pg_type}')"
                )
                return graflo_type

        # Default to STRING for unknown types
        logger.warning(
            f"Unknown PostgreSQL type '{postgres_type}', defaulting to STRING"
        )
        return "STRING"

    @classmethod
    def is_datetime_type(cls, postgres_type: str) -> bool:
        """Check if a PostgreSQL type is a datetime type.

        Args:
            postgres_type: PostgreSQL type name

        Returns:
            bool: True if the type is a datetime-related type
        """
        normalized = postgres_type.lower().strip()
        datetime_types = [
            "timestamp",
            "date",
            "time",
            "interval",
            "timestamptz",
            "timetz",
        ]
        return any(dt_type in normalized for dt_type in datetime_types)

    @classmethod
    def is_numeric_type(cls, postgres_type: str) -> bool:
        """Check if a PostgreSQL type is a numeric type.

        Args:
            postgres_type: PostgreSQL type name

        Returns:
            bool: True if the type is numeric
        """
        normalized = postgres_type.lower().strip()
        numeric_types = [
            "integer",
            "int",
            "bigint",
            "smallint",
            "serial",
            "real",
            "double precision",
            "numeric",
            "decimal",
            "float",
        ]
        return any(nt_type in normalized for nt_type in numeric_types)
