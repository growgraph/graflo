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

        Array types (``integer[]``, ``text[]``, …) map to ``LIST``; use
        :meth:`map_field` when ``item_type`` is needed.

        Args:
            postgres_type: PostgreSQL type name (e.g., 'int4', 'varchar', 'timestamp')

        Returns:
            str: graflo Field type (INT, FLOAT, BOOL, STRING, LIST, …)
        """
        field_type, _item_type = cls.map_field(postgres_type)
        return field_type

    @classmethod
    def map_field(cls, postgres_type: str) -> tuple[str, str | None]:
        """Map PostgreSQL type to ``(FieldType, item_type|None)``.

        Homogeneous SQL arrays become ``("LIST", <scalar>)`` when the element
        type is known; otherwise ``("LIST", None)`` is avoided — unknown element
        types fall through to STRING rather than inventing a wrong item_type.
        """
        normalized = postgres_type.lower().strip()

        if "(" in normalized:
            normalized = normalized.split("(")[0].strip()

        is_array = False
        if normalized.endswith("[]"):
            is_array = True
            normalized = normalized[:-2].strip()
        elif normalized.startswith("_") and normalized[1:] in cls.TYPE_MAPPING:
            # pg catalog array aliases like _int4, _text
            is_array = True
            normalized = normalized[1:]

        mapped: str | None = None
        if normalized in cls.TYPE_MAPPING:
            mapped = cls.TYPE_MAPPING[normalized]
        else:
            for pg_type, graflo_type in cls.TYPE_MAPPING.items():
                if pg_type in normalized or normalized in pg_type:
                    logger.debug(
                        f"Mapped PostgreSQL type '{postgres_type}' to graflo type "
                        f"'{graflo_type}' (partial match with '{pg_type}')"
                    )
                    mapped = graflo_type
                    break

        if mapped is None:
            logger.warning(
                f"Unknown PostgreSQL type '{postgres_type}', defaulting to STRING"
            )
            mapped = "STRING"

        if is_array:
            if mapped in {
                "INT",
                "FLOAT",
                "BOOL",
                "STRING",
                "DATETIME",
            }:
                return "LIST", mapped
            # Do not invent a wrong scalar item_type
            logger.debug(
                "PostgreSQL array type '%s' mapped without reliable item_type",
                postgres_type,
            )
            return "LIST", None

        return mapped, None

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
