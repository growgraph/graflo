"""Map SQL column types onto graflo ``FieldType`` values.

Type *names* are dialect-specific, but the mapping is mostly not: ``integer``,
``varchar`` and ``timestamp`` mean the same thing everywhere they appear, and
the engines that spell them differently (``INT64``, ``FLOAT64``, ``NVARCHAR``)
are spelling the same handful of concepts.

So one table carries every spelling, and lookup is exact-match first, then
substring. An unrecognised type falls back to ``STRING`` with a warning rather
than raising: inference produces a *draft* schema for a modeller to refine, and
refusing the whole database over one exotic column would be the wrong trade.
That is the opposite of the write-side policy in
:mod:`graflo.db.field_type_support`, which does raise — there, a wrong type
silently corrupts data.
"""

import logging

logger = logging.getLogger(__name__)


class SqlTypeMapper:
    """Maps SQL data type names to graflo Field types.

    Subclass and extend :attr:`TYPE_MAPPING` for a dialect with spellings this
    does not cover; entries are matched exactly before any substring fallback,
    so additions cannot change how an existing name resolves.
    """

    # Type name -> graflo Field type. Ordered: exact match wins, and the
    # substring fallback walks this in insertion order.
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
        # --- Other dialects -------------------------------------------------
        # Appended deliberately: exact matches take priority and the substring
        # fallback is ordered, so adding here cannot change an existing result.
        # BigQuery / Snowflake / warehouse spellings
        "int64": "INT",
        "float64": "FLOAT",
        "bignumeric": "FLOAT",
        "number": "FLOAT",
        "bytes": "STRING",
        "geography": "STRING",
        "variant": "STRING",
        "object": "STRING",
        "struct": "STRING",
        "record": "STRING",
        # MySQL / SQL Server / SQLite spellings
        "tinyint": "INT",
        "mediumint": "INT",
        "nvarchar": "STRING",
        "nchar": "STRING",
        "longtext": "STRING",
        "mediumtext": "STRING",
        "tinytext": "STRING",
        "datetime2": "DATETIME",
        "smalldatetime": "DATETIME",
        "bit": "BOOL",
        "blob": "STRING",
        "clob": "STRING",
    }

    @classmethod
    def map_type(cls, sql_type: str) -> str:
        """Map a SQL type name to a graflo Field type.

        Array types (``integer[]``, ``text[]``, …) map to ``LIST``; use
        :meth:`map_field` when ``item_type`` is needed.

        Args:
            sql_type: SQL type name (e.g., 'int4', 'varchar', 'timestamp')

        Returns:
            str: graflo Field type (INT, FLOAT, BOOL, STRING, LIST, …)
        """
        field_type, _item_type = cls.map_field(sql_type)
        return field_type

    @classmethod
    def map_field(cls, sql_type: str) -> tuple[str, str | None]:
        """Map a SQL type name to ``(FieldType, item_type|None)``.

        Homogeneous SQL arrays become ``("LIST", <scalar>)`` when the element
        type is known; otherwise ``("LIST", None)`` is avoided — unknown element
        types fall through to STRING rather than inventing a wrong item_type.
        """
        normalized = sql_type.lower().strip()

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
            for known_type, graflo_type in cls.TYPE_MAPPING.items():
                if known_type in normalized or normalized in known_type:
                    logger.debug(
                        f"Mapped SQL type '{sql_type}' to graflo type "
                        f"'{graflo_type}' (partial match with '{known_type}')"
                    )
                    mapped = graflo_type
                    break

        if mapped is None:
            logger.warning(f"Unknown SQL type '{sql_type}', defaulting to STRING")
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
                "SQL array type '%s' mapped without reliable item_type",
                sql_type,
            )
            return "LIST", None

        return mapped, None

    @classmethod
    def is_datetime_type(cls, sql_type: str) -> bool:
        """Check if a PostgreSQL type is a datetime type.

        Args:
            sql_type: SQL type name

        Returns:
            bool: True if the type is a datetime-related type
        """
        normalized = sql_type.lower().strip()
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
    def is_numeric_type(cls, sql_type: str) -> bool:
        """Check if a PostgreSQL type is a numeric type.

        Args:
            sql_type: SQL type name

        Returns:
            bool: True if the type is numeric
        """
        normalized = sql_type.lower().strip()
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
