"""Core ontology and base classes for graph database operations.

This module provides the fundamental data structures and base classes used throughout
the graph database system. It includes base classes for enums and
database-specific configurations.

Key Components:
    - BaseEnum: Base class for string-based enumerations with flexible membership testing
    - ExpressionFlavor: Enum for expression language types
    - AggregationType: Enum for supported aggregation operations

Example:
    >>> class MyEnum(BaseEnum):
    ...     VALUE1 = "value1"
    ...     VALUE2 = "value2"
    >>> "value1" in MyEnum  # True
    >>> "invalid" in MyEnum  # False
"""

from enum import EnumMeta
from strenum import StrEnum


class MetaEnum(EnumMeta):
    """Metaclass for flexible enumeration membership testing.

    This metaclass allows checking if a value is a valid member of an enum
    using the `in` operator, even if the value hasn't been instantiated as
    an enum member.

    Example:
        >>> class MyEnum(BaseEnum):
        ...     VALUE = "value"
        >>> "value" in MyEnum  # True
        >>> "invalid" in MyEnum  # False
    """

    def __contains__(self, member: object) -> bool:
        """Check if an item is a valid member of the enum.

        Args:
            item: Value to check for membership

        Returns:
            bool: True if the item is a valid enum member, False otherwise
        """
        if isinstance(member, self):
            return True
        try:
            self(member)
            return True
        except ValueError:
            return False


class BaseEnum(StrEnum, metaclass=MetaEnum):
    """Base class for string-based enumerations.

    This class provides a foundation for string-based enums with flexible
    membership testing through the MetaEnum metaclass.
    """

    def __str__(self) -> str:
        """Return the enum value as string for proper serialization."""
        return self.value

    def __repr__(self) -> str:
        """Return the enum value as string for proper serialization."""
        return self.value


# Register custom YAML representer for BaseEnum to serialize as string values
def _register_yaml_representer():
    """Register YAML representer for BaseEnum and all its subclasses to serialize as string values."""
    try:
        import yaml

        def base_enum_representer(dumper, data):
            """Custom YAML representer for BaseEnum - serializes as string value."""
            return dumper.represent_scalar("tag:yaml.org,2002:str", str(data.value))

        # Register for BaseEnum and use multi_representer for all subclasses
        yaml.add_representer(BaseEnum, base_enum_representer)
        yaml.add_multi_representer(BaseEnum, base_enum_representer)
    except ImportError:
        # yaml not available, skip registration
        pass


# Register the representer at module import time (after BaseEnum is defined)
_register_yaml_representer()


class ExpressionFlavor(BaseEnum):
    """Supported expression language types for filter/query rendering.

    Uses the actual query language names: AQL (ArangoDB), CYPHER (Neo4j,
    FalkorDB, Memgraph), GSQL (TigerGraph), SQL for WHERE clauses, PYTHON for in-memory evaluation.

    Attributes:
        AQL: ArangoDB AQL expressions
        CYPHER: OpenCypher expressions (Neo4j, FalkorDB, Memgraph)
        GSQL: TigerGraph GSQL expressions (including REST++ filter format)
        SQL: SQL WHERE clause fragments (column names, single-quoted values)
        PYTHON: Python expression evaluation
    """

    AQL = "aql"
    CYPHER = "cypher"
    GSQL = "gsql"
    SQL = "sql"
    PYTHON = "python"


class AggregationType(BaseEnum):
    """Supported aggregation operations.

    This enum defines the supported aggregation operations for data analysis.

    Attributes:
        COUNT: Count operation
        MAX: Maximum value
        MIN: Minimum value
        AVERAGE: Average value
        SORTED_UNIQUE: Sorted unique values
    """

    COUNT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    AVERAGE = "AVERAGE"
    SORTED_UNIQUE = "SORTED_UNIQUE"


class DBType(StrEnum, metaclass=MetaEnum):
    """Enum representing different types of databases.

    Includes both graph databases and source databases (SQL, NoSQL, etc.).
    """

    # Graph databases
    ARANGO = "arango"
    NEO4J = "neo4j"
    TIGERGRAPH = "tigergraph"
    FALKORDB = "falkordb"
    MEMGRAPH = "memgraph"
    NEBULA = "nebula"

    # Source databases (SQL, NoSQL, RDF)
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MONGODB = "mongodb"
    SQLITE = "sqlite"
    SPARQL = "sparql"


# Mapping from graph DB type to expression flavor for filter rendering.
# Used by Connection subclasses so filters are rendered in the correct language.
DB_TYPE_TO_EXPRESSION_FLAVOR: dict[DBType, ExpressionFlavor] = {
    DBType.ARANGO: ExpressionFlavor.AQL,
    DBType.NEO4J: ExpressionFlavor.CYPHER,
    DBType.FALKORDB: ExpressionFlavor.CYPHER,
    DBType.MEMGRAPH: ExpressionFlavor.CYPHER,
    DBType.TIGERGRAPH: ExpressionFlavor.GSQL,
}
