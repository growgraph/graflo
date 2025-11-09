"""Vertex configuration and management for graph databases.

This module provides classes and utilities for managing vertices in graph databases.
It handles vertex configuration, field management, indexing, and filtering operations.
The module supports both ArangoDB and Neo4j through the DBFlavor enum.

Key Components:
    - Vertex: Represents a vertex with its fields and indexes
    - VertexConfig: Manages collections of vertices and their configurations

Example:
    >>> vertex = Vertex(name="user", fields=["id", "name"])
    >>> config = VertexConfig(vertices=[vertex])
    >>> fields = config.fields("user", with_aux=True)
"""

import dataclasses
import logging
from typing import TYPE_CHECKING

from graflo.architecture.onto import Index
from graflo.filter.onto import Expression
from graflo.onto import BaseDataclass, BaseEnum, DBFlavor

logger = logging.getLogger(__name__)


class FieldType(BaseEnum):
    """Supported field types for graph databases.

    These types are primarily used for TigerGraph, which requires explicit field types.
    Other databases (ArangoDB, Neo4j) may use different type systems or not require types.

    Attributes:
        INT: Integer type
        UINT: Unsigned integer type
        FLOAT: Floating point type
        DOUBLE: Double precision floating point type
        BOOL: Boolean type
        STRING: String type
        DATETIME: DateTime type
    """

    INT = "INT"
    UINT = "UINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOL = "BOOL"
    STRING = "STRING"
    DATETIME = "DATETIME"


if TYPE_CHECKING:
    # For type checking: after __post_init__, fields is always list[Field]
    # Using string literal to avoid forward reference issues
    _FieldsType = list["Field"]
    # For type checking: after __post_init__, type is always FieldType | None
    _FieldTypeType = FieldType | None
else:
    # For runtime: accept flexible input types, will be normalized in __post_init__
    _FieldsType = list[str] | list["Field"] | list[dict]
    # For runtime: accept FieldType, str, or None (strings converted in __post_init__)
    _FieldTypeType = FieldType | str | None


@dataclasses.dataclass
class Field(BaseDataclass):
    """Represents a typed field in a vertex.

    Field objects behave like strings for backward compatibility. They can be used
    in sets, as dictionary keys, and in string comparisons. The type information
    is preserved for databases that need it (like TigerGraph).

    Attributes:
        name: Name of the field
        type: Optional type of the field. Can be FieldType enum, str, or None at construction.
              Strings are converted to FieldType enum in __post_init__.
              After initialization, this is always FieldType | None (type checker sees this).
              None is allowed (most databases like ArangoDB don't require types).
              Defaults to None.
    """

    name: str
    type: _FieldTypeType = None

    def __post_init__(self):
        """Validate and normalize type if specified."""
        if self.type is not None:
            # Convert string to FieldType enum if it's a string
            if isinstance(self.type, str):
                type_upper = self.type.upper()
                # Validate and convert to FieldType enum
                if type_upper not in FieldType:
                    allowed_types = sorted(ft.value for ft in FieldType)
                    raise ValueError(
                        f"Field type '{self.type}' is not allowed. "
                        f"Allowed types are: {', '.join(allowed_types)}"
                    )
                self.type = FieldType(type_upper)
            # If it's already a FieldType, validate it's a valid enum member
            elif isinstance(self.type, FieldType):
                # Already a FieldType enum, no conversion needed
                pass
            else:
                allowed_types = sorted(ft.value for ft in FieldType)
                raise ValueError(
                    f"Field type must be FieldType enum, str, or None, got {type(self.type)}. "
                    f"Allowed types are: {', '.join(allowed_types)}"
                )

    def __str__(self) -> str:
        """Return field name as string for backward compatibility."""
        return self.name

    def __repr__(self) -> str:
        """Return representation including type information."""
        if self.type:
            return f"Field(name='{self.name}', type='{self.type}')"
        return f"Field(name='{self.name}')"

    def __hash__(self) -> int:
        """Hash by name only, allowing Field objects to work in sets and as dict keys."""
        return hash(self.name)

    def __eq__(self, other) -> bool:
        """Compare equal to strings with same name, or other Field objects with same name."""
        if isinstance(other, Field):
            return self.name == other.name
        if isinstance(other, str):
            return self.name == other
        return False

    def __ne__(self, other) -> bool:
        """Compare not equal."""
        return not self.__eq__(other)

    # Field objects are hashable (via __hash__) and comparable to strings (via __eq__)
    # This allows them to work in sets, as dict keys, and in membership tests


@dataclasses.dataclass
class Vertex(BaseDataclass):
    """Represents a vertex in the graph database.

    A vertex is a fundamental unit in the graph that can have fields, indexes,
    and filters. Fields can be specified as strings, Field objects, or dicts.
    Internally, fields are stored as Field objects but behave like strings
    for backward compatibility.

    Attributes:
        name: Name of the vertex
        fields: List of field names (str), Field objects, or dicts.
               Will be normalized to Field objects internally in __post_init__.
               After initialization, this is always list[Field] (type checker sees this).
        fields_aux: List of auxiliary field names for weight passing
        indexes: List of indexes for the vertex
        filters: List of filter expressions
        dbname: Optional database name (defaults to vertex name)

    Examples:
        >>> # Backward compatible: list of strings
        >>> v1 = Vertex(name="user", fields=["id", "name"])

        >>> # Typed fields: list of Field objects
        >>> v2 = Vertex(name="user", fields=[
        ...     Field(name="id", type="INT"),
        ...     Field(name="name", type="STRING")
        ... ])

        >>> # From dicts (e.g., from YAML/JSON)
        >>> v3 = Vertex(name="user", fields=[
        ...     {"name": "id", "type": "INT"},
        ...     {"name": "name"}  # defaults to None type
        ... ])
    """

    name: str
    fields: _FieldsType = dataclasses.field(default_factory=list)
    fields_aux: list[str] = dataclasses.field(
        default_factory=list
    )  # temporary field necessary to pass weights to edges
    indexes: list[Index] = dataclasses.field(default_factory=list)
    filters: list[Expression] = dataclasses.field(default_factory=list)
    dbname: str | None = None

    def _normalize_fields(
        self, fields: list[str] | list[Field] | list[dict]
    ) -> list[Field]:
        """Normalize fields to Field objects.

        Converts strings, Field objects, or dicts to Field objects.
        Field objects behave like strings for backward compatibility.

        Args:
            fields: List of strings, Field objects, or dicts

        Returns:
            list[Field]: Normalized list of Field objects (preserving order)
        """
        normalized = []
        for field in fields:
            if isinstance(field, Field):
                normalized.append(field)
            elif isinstance(field, str):
                # Backward compatibility: string becomes Field with None type
                # (most databases like ArangoDB don't require types)
                normalized.append(Field(name=field, type=None))
            elif isinstance(field, dict):
                # From dict (e.g., from YAML/JSON)
                # Extract name and optional type
                name = field.get("name")
                if name is None:
                    raise ValueError(f"Field dict must have 'name' key: {field}")
                field_type = field.get("type")
                normalized.append(Field(name=name, type=field_type))
            else:
                raise TypeError(f"Field must be str, Field, or dict, got {type(field)}")
        return normalized

    @property
    def field_names(self) -> list[str]:
        """Get list of field names (as strings).

        Returns:
            list[str]: List of field names
        """
        return [field.name for field in self.fields]

    @property
    def fields_all(self):
        """Get all fields including auxiliary fields.

        Returns:
            list[Field]: Combined list of regular and auxiliary fields.
                        Field objects behave like strings, so this is backward compatible.
        """
        # fields_aux are still strings, convert to Field objects with None type
        aux_fields = [Field(name=name, type=None) for name in self.fields_aux]
        return self.fields + aux_fields

    def get_fields_with_defaults(
        self, db_flavor: DBFlavor | None = None, with_aux: bool = False
    ) -> list[Field]:
        """Get fields with default types applied based on database flavor.

        For TigerGraph, fields with None type will default to "STRING".
        Other databases keep None types as-is.

        Args:
            db_flavor: Optional database flavor. If None, returns fields as-is.
            with_aux: Whether to include auxiliary fields

        Returns:
            list[Field]: List of Field objects with default types applied
        """
        fields = self.fields_all if with_aux else self.fields

        if db_flavor == DBFlavor.TIGERGRAPH:
            # For TigerGraph, default None types to STRING
            return [
                Field(name=f.name, type=f.type if f.type is not None else "STRING")
                for f in fields
            ]

        # For other databases or None, return fields as-is
        return fields

    def __post_init__(self):
        """Initialize the vertex after dataclass initialization.

        Sets the database name if not provided, normalizes fields to Field objects,
        and updates fields based on indexes. Field objects behave like strings,
        maintaining backward compatibility.
        """
        if self.dbname is None:
            self.dbname = self.name

        # Normalize fields to Field objects (preserve order)
        self.fields = self._normalize_fields(self.fields)

        # Normalize indexes to Index objects if they're dicts
        normalized_indexes = []
        for idx in self.indexes:
            if isinstance(idx, dict):
                normalized_indexes.append(Index.from_dict(idx))
            else:
                normalized_indexes.append(idx)
        self.indexes = normalized_indexes

        if not self.indexes:
            # Index expects list[str], but Field objects convert to strings automatically
            # via __str__, so we extract names
            self.indexes = [Index(fields=self.field_names)]

        # Collect field names from existing fields (preserve order)
        seen_names = {f.name for f in self.fields}
        # Add index fields that aren't already present (preserve original order, append new)
        for idx in self.indexes:
            for field_name in idx.fields:
                if field_name not in seen_names:
                    # Add new field, preserving order by adding to end
                    self.fields.append(Field(name=field_name, type=None))
                    seen_names.add(field_name)

    def update_aux_fields(self, fields_aux: list):
        """Update auxiliary fields.

        Args:
            fields_aux: List of new auxiliary fields to add

        Returns:
            Vertex: Self for method chaining
        """
        self.fields_aux = list(set(self.fields_aux) | set(fields_aux))
        return self

    @classmethod
    def from_dict(cls, data: dict):
        """Create Vertex from dictionary, handling field normalization.

        Overrides parent to properly handle fields that may be strings, dicts, or Field objects.
        JSONWizard may incorrectly deserialize dicts in fields, so we need to handle them manually.

        Args:
            data: Dictionary containing vertex data

        Returns:
            Vertex: New Vertex instance
        """
        # Extract and preserve fields before JSONWizard processes them
        fields_data = data.get("fields", [])
        # Create a copy without fields to let JSONWizard handle the rest
        data_copy = {k: v for k, v in data.items() if k != "fields"}

        # Call parent from_dict (JSONWizard)
        instance = super().from_dict(data_copy)

        # Now manually set fields (could be strings, dicts, or already Field objects)
        # __post_init__ will normalize them properly
        instance.fields = fields_data
        # Trigger normalization again
        instance.fields = instance._normalize_fields(instance.fields)
        return instance


@dataclasses.dataclass
class VertexConfig(BaseDataclass):
    """Configuration for managing collections of vertices.

    This class manages a collection of vertices, providing methods for accessing
    and manipulating vertex configurations.

    Attributes:
        vertices: List of vertex configurations
        blank_vertices: List of blank vertex names
        force_types: Dictionary mapping vertex names to type lists
        db_flavor: Database flavor (ARANGO or NEO4J)
    """

    vertices: list[Vertex]
    blank_vertices: list[str] = dataclasses.field(default_factory=list)
    force_types: dict[str, list] = dataclasses.field(default_factory=dict)
    db_flavor: DBFlavor = DBFlavor.ARANGO

    def __post_init__(self):
        """Initialize the vertex configuration.

        Creates internal mappings and validates blank vertices.

        Raises:
            ValueError: If blank vertices are not defined in the configuration
        """
        self._vertices_map: dict[str, Vertex] = {
            item.name: item for item in self.vertices
        }

        # TODO replace by types
        # vertex_collection_name -> [numeric fields]
        self._vcollection_numeric_fields_map = {}

        if set(self.blank_vertices) - set(self.vertex_set):
            raise ValueError(
                f" Blank collections {self.blank_vertices} are not defined"
                " as vertex collections"
            )

    @property
    def vertex_set(self):
        """Get set of vertex names.

        Returns:
            set[str]: Set of vertex names
        """
        return set(self._vertices_map.keys())

    @property
    def vertex_list(self):
        """Get list of vertex configurations.

        Returns:
            list[Vertex]: List of vertex configurations
        """
        return list(self._vertices_map.values())

    def _get_vertex_by_name_or_dbname(self, identifier: str) -> Vertex:
        """Get vertex by name or dbname.

        Args:
            identifier: Vertex name or dbname

        Returns:
            Vertex: The vertex object

        Raises:
            KeyError: If vertex is not found by name or dbname
        """
        # First try by name (most common case)
        if identifier in self._vertices_map:
            return self._vertices_map[identifier]

        # Try by dbname
        for vertex in self._vertices_map.values():
            if vertex.dbname == identifier:
                return vertex

        # Not found
        available_names = list(self._vertices_map.keys())
        available_dbnames = [v.dbname for v in self._vertices_map.values()]
        raise KeyError(
            f"Vertex '{identifier}' not found by name or dbname. "
            f"Available names: {available_names}, "
            f"Available dbnames: {available_dbnames}"
        )

    def vertex_dbname(self, vertex_name):
        """Get database name for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            str: Database name for the vertex

        Raises:
            KeyError: If vertex is not found
        """
        try:
            value = self._vertices_map[vertex_name].dbname
        except KeyError as e:
            logger.error(
                "Available vertex collections :"
                f" {self._vertices_map.keys()}; vertex collection"
                f" requested : {vertex_name}"
            )
            raise e
        return value

    def index(self, vertex_name) -> Index:
        """Get primary index for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            Index: Primary index for the vertex
        """
        return self._vertices_map[vertex_name].indexes[0]

    def indexes(self, vertex_name) -> list[Index]:
        """Get all indexes for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            list[Index]: List of indexes for the vertex
        """
        return self._vertices_map[vertex_name].indexes

    def fields(
        self,
        vertex_name: str,
        with_aux=False,
        as_names=True,
        db_flavor: DBFlavor | None = None,
    ) -> list[Field]:
        """Get fields for a vertex.

        Args:
            vertex_name: Name of the vertex or dbname
            with_aux: Whether to include auxiliary fields
            as_names: If True (default), return field names as strings for backward compatibility.
                     If False, return Field objects.
            db_flavor: Optional database flavor. If provided, applies default types
                      (e.g., TigerGraph defaults None types to "STRING").

        Returns:
            list[str] | list[Field]: List of field names or Field objects
        """
        # Get vertex by name or dbname
        vertex = self._get_vertex_by_name_or_dbname(vertex_name)

        # Get fields with defaults applied if db_flavor is provided
        if db_flavor is not None:
            fields = vertex.get_fields_with_defaults(db_flavor, with_aux=with_aux)
        elif with_aux:
            fields = vertex.fields_all
        else:
            fields = vertex.fields

        if as_names:
            # Return as strings for backward compatibility
            return [field.name for field in fields]
        # Return Field objects
        return fields

    def numeric_fields_list(self, vertex_name):
        """Get list of numeric fields for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            tuple: Tuple of numeric field names

        Raises:
            ValueError: If vertex is not defined in config
        """
        if vertex_name in self.vertex_set:
            if vertex_name in self._vcollection_numeric_fields_map:
                return self._vcollection_numeric_fields_map[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                " Accessing vertex collection numeric fields: vertex"
                f" collection {vertex_name} was not defined in config"
            )

    def filters(self, vertex_name) -> list[Expression]:
        """Get filter expressions for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            list[Expression]: List of filter expressions
        """
        if vertex_name in self._vertices_map:
            return self._vertices_map[vertex_name].filters
        else:
            return []

    def update_vertex(self, v: Vertex):
        """Update vertex configuration.

        Args:
            v: Vertex configuration to update
        """
        self._vertices_map[v.name] = v

    def __getitem__(self, key: str):
        """Get vertex configuration by name.

        Args:
            key: Vertex name

        Returns:
            Vertex: Vertex configuration

        Raises:
            KeyError: If vertex is not found
        """
        if key in self._vertices_map:
            return self._vertices_map[key]
        else:
            raise KeyError(f"Vertex {key} absent")

    def __setitem__(self, key: str, value: Vertex):
        """Set vertex configuration by name.

        Args:
            key: Vertex name
            value: Vertex configuration
        """
        self._vertices_map[key] = value
