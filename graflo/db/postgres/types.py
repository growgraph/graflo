"""PostgreSQL type mapping.

The table itself is dialect-neutral and lives in :mod:`graflo.db.sql.types`;
this is the PostgreSQL name for it, kept because it is public API and because
a PostgreSQL-only spelling belongs here if one is ever needed.
"""

from graflo.db.sql.types import SqlTypeMapper


class PostgresTypeMapper(SqlTypeMapper):
    """Maps PostgreSQL data types to graflo Field types."""


__all__ = ["PostgresTypeMapper"]
