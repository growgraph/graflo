"""Core ontology and base classes for graph database operations.

This module provides the fundamental data structures and base classes used throughout
the graph database system. It includes base classes for enums, dataclasses, and
database-specific configurations.

Key Components:
    - BaseEnum: Base class for string-based enumerations with flexible membership testing
    - BaseDataclass: Base class for dataclasses with JSON/YAML serialization support
    - DBFlavor: Enum for supported database types (ArangoDB, Neo4j)
    - ExpressionFlavor: Enum for expression language types
    - AggregationType: Enum for supported aggregation operations

Example:
    >>> class MyEnum(BaseEnum):
    ...     VALUE1 = "value1"
    ...     VALUE2 = "value2"
    >>> "value1" in MyEnum  # True
    >>> "invalid" in MyEnum  # False
"""

import dataclasses
from copy import deepcopy
from enum import EnumMeta
from strenum import StrEnum
from dataclass_wizard import JSONWizard, YAMLWizard
from dataclass_wizard.enums import DateTimeTo


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

    def __contains__(cls, item, **kwargs):
        """Check if an item is a valid member of the enum.

        Args:
            item: Value to check for membership
            **kwargs: Additional keyword arguments

        Returns:
            bool: True if the item is a valid enum member, False otherwise
        """
        try:
            cls(item, **kwargs)
        except ValueError:
            return False
        return True


class BaseEnum(StrEnum, metaclass=MetaEnum):
    """Base class for string-based enumerations.

    This class provides a foundation for string-based enums with flexible
    membership testing through the MetaEnum metaclass.
    """

    pass


class DBFlavor(BaseEnum):
    """Supported database types.

    This enum defines the supported graph database types in the system.

    Attributes:
        ARANGO: ArangoDB database
        NEO4J: Neo4j database
        TIGERGRAPH: TigerGraph database
    """

    ARANGO = "arango"
    NEO4J = "neo4j"
    TIGERGRAPH = "tigergraph"


class ExpressionFlavor(BaseEnum):
    """Supported expression language types.

    This enum defines the supported expression languages for querying and
    filtering data.

    Attributes:
        ARANGO: ArangoDB AQL expressions
        NEO4J: Neo4j Cypher expressions
        TIGERGRAPH: TigerGraph GSQL expressions
        PYTHON: Python expressions
    """

    ARANGO = "arango"
    NEO4J = "neo4j"
    TIGERGRAPH = "tigergraph"
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


@dataclasses.dataclass
class BaseDataclass(JSONWizard, JSONWizard.Meta, YAMLWizard):
    """Base class for dataclasses with serialization support.

    This class provides a foundation for dataclasses with JSON and YAML
    serialization capabilities. It includes methods for updating instances
    and accessing field members.

    Attributes:
        marshal_date_time_as: Format for datetime serialization
        key_transform_with_dump: Key transformation style for serialization
    """

    marshal_date_time_as = DateTimeTo.ISO_FORMAT
    key_transform_with_dump = "SNAKE"
    # skip_defaults = True

    def update(self, other):
        """Update this instance with values from another instance.

        This method performs a deep update of the instance's attributes using
        values from another instance of the same type. It handles different
        types of attributes (sets, lists, dicts, dataclasses) appropriately.

        Args:
            other: Another instance of the same type to update from

        Raises:
            TypeError: If other is not an instance of the same type
        """
        if not isinstance(other, type(self)):
            raise TypeError(
                f"Expected {type(self).__name__} instance, got {type(other).__name__}"
            )

        for field in dataclasses.fields(self):
            name = field.name
            current_value = getattr(self, name)
            other_value = getattr(other, name)

            if other_value is None:
                pass
            elif isinstance(other_value, set):
                setattr(self, name, current_value | deepcopy(other_value))
            elif isinstance(other_value, list):
                setattr(self, name, current_value + deepcopy(other_value))
            elif isinstance(other_value, dict):
                setattr(self, name, {**current_value, **deepcopy(other_value)})
            elif dataclasses.is_dataclass(type(other_value)):
                if current_value is not None:
                    current_value.update(other_value)
                else:
                    setattr(self, name, deepcopy(other_value))
            else:
                if current_value is None:
                    setattr(self, name, other_value)

    @classmethod
    def get_fields_members(cls):
        """Get list of field members excluding private ones.

        Returns:
            list[str]: List of public field names
        """
        return [k for k in cls.__annotations__ if not k.startswith("_")]
