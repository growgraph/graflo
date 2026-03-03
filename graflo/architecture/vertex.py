"""Vertex configuration and management for graph databases.

This module provides classes and utilities for managing vertices in graph databases.
It handles vertex configuration, field management, identity, and filtering operations.
The module supports both ArangoDB and Neo4j through the DBType enum.

Key Components:
    - Vertex: Represents a vertex with its fields and identity
    - VertexConfig: Manages vertices and their configurations

Example:
    >>> vertex = Vertex(name="user", fields=["id", "name"])
    >>> config = VertexConfig(vertices=[vertex])
    >>> fields = config.fields("user")  # Returns list[Field]
    >>> field_names = config.fields_names("user")  # Returns list[str]
"""

from __future__ import annotations

import ast
import json
import logging
from typing import Any

from pydantic import (
    ConfigDict,
    Field as PydanticField,
    PrivateAttr,
    field_validator,
    model_validator,
)

from graflo.architecture.base import ConfigBaseModel
from graflo.filter.onto import FilterExpression
from graflo.onto import BaseEnum

logger = logging.getLogger(__name__)

# Type accepted for fields before normalization (for use by Edge/WeightConfig)
FieldsInputType = list[str] | list["Field"] | list[dict[str, Any]]


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


class Field(ConfigBaseModel):
    """Represents a typed field in a vertex.

    Field objects behave like strings for backward compatibility. They can be used
    in sets, as dictionary keys, and in string comparisons. The type information
    is preserved for databases that need it (like TigerGraph).

    Attributes:
        name: Name of the field
        type: Optional type of the field. Can be FieldType enum, str, or None at construction.
              Strings are converted to FieldType enum by the validator.
              None is allowed (most databases like ArangoDB don't require types).
              Defaults to None.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = PydanticField(
        ...,
        description="Name of the field (e.g. column or attribute name).",
    )
    type: FieldType | None = PydanticField(
        default=None,
        description="Optional field type for databases that require it (e.g. TigerGraph: INT, STRING). None for schema-agnostic backends.",
    )

    @field_validator("type", mode="before")
    @classmethod
    def normalize_type(cls, v: Any) -> FieldType | None:
        if v is None:
            return None
        if isinstance(v, FieldType):
            return v
        if isinstance(v, str):
            type_upper = v.upper()
            if type_upper not in FieldType:
                allowed_types = sorted(ft.value for ft in FieldType)
                raise ValueError(
                    f"Field type '{v}' is not allowed. "
                    f"Allowed types are: {', '.join(allowed_types)}"
                )
            return FieldType(type_upper)
        allowed_types = sorted(ft.value for ft in FieldType)
        raise ValueError(
            f"Field type must be FieldType enum, str, or None, got {type(v)}. "
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

    def __eq__(self, other: object) -> bool:
        """Compare equal to strings with same name, or other Field objects with same name."""
        if isinstance(other, Field):
            return self.name == other.name
        if isinstance(other, str):
            return self.name == other
        return False

    def __ne__(self, other: object) -> bool:
        """Compare not equal."""
        return not self.__eq__(other)


def _parse_string_to_dict(field_str: str) -> dict | None:
    """Parse a string that might be a JSON or Python dict representation."""
    try:
        parsed = json.loads(field_str)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    try:
        parsed = ast.literal_eval(field_str)
        return parsed if isinstance(parsed, dict) else None
    except (ValueError, SyntaxError):
        return None


def _dict_to_field(field_dict: dict[str, Any]) -> Field:
    """Convert a dict to a Field object."""
    name = field_dict.get("name")
    if name is None:
        raise ValueError(f"Field dict must have 'name' key: {field_dict}")
    return Field(name=name, type=field_dict.get("type"))


def _normalize_fields_item(item: str | Field | dict[str, Any]) -> Field:
    """Convert a single field item (str, Field, or dict) to Field."""
    if isinstance(item, Field):
        return item
    if isinstance(item, dict):
        return _dict_to_field(item)
    if isinstance(item, str):
        parsed_dict = _parse_string_to_dict(item)
        if parsed_dict:
            return _dict_to_field(parsed_dict)
        return Field(name=item, type=None)
    raise TypeError(f"Field must be str, Field, or dict, got {type(item)}")


class Vertex(ConfigBaseModel):
    """Represents a vertex in the graph database.

    A vertex is a fundamental unit in the graph that can have fields, identity,
    and filters. Fields can be specified as strings, Field objects, or dicts.
    Internally, fields are stored as Field objects but behave like strings
    for backward compatibility.

    Attributes:
        name: Name of the vertex
        fields: List of field names (str), Field objects, or dicts.
               Will be normalized to Field objects by the validator.
        identity: List of fields forming logical primary identity
        filters: List of filter expressions

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

    # Allow extra keys when loading from YAML (e.g. transforms, other runtime keys)
    model_config = ConfigDict(extra="ignore")

    name: str = PydanticField(
        ...,
        description="Name of the vertex type (e.g. user, post, company).",
    )
    fields: list[Field] = PydanticField(
        default_factory=list,
        description="List of fields (names, Field objects, or dicts). Normalized to Field objects.",
    )
    identity: list[str] = PydanticField(
        default_factory=list,
        description="Logical identity fields (primary key semantics for matching/upserts).",
    )
    filters: list[FilterExpression] = PydanticField(
        default_factory=list,
        description="Filter expressions (logical formulae) applied when querying this vertex.",
    )

    @field_validator("fields", mode="before")
    @classmethod
    def convert_to_fields(cls, v: Any) -> Any:
        if not isinstance(v, list):
            raise ValueError("fields must be a list")
        return [_normalize_fields_item(item) for item in v]

    @field_validator("filters", mode="before")
    @classmethod
    def convert_to_expressions(cls, v: Any) -> Any:
        if not isinstance(v, list):
            return v
        result: list[FilterExpression] = []
        for item in v:
            if isinstance(item, FilterExpression):
                result.append(item)
            elif isinstance(item, (dict, list)):
                result.append(FilterExpression.from_dict(item))
            else:
                raise ValueError(
                    "each filter must be a FilterExpression instance or a dict/list (parsed as FilterExpression)"
                )
        return result

    @field_validator("identity", mode="before")
    @classmethod
    def convert_identity(cls, v: Any) -> Any:
        if v is None:
            return []
        if isinstance(v, tuple):
            return list(v)
        if isinstance(v, list):
            return v
        raise ValueError("identity must be a list[str]")

    @model_validator(mode="after")
    def set_identity(self) -> "Vertex":
        identity_fields = list(self.identity)
        if not identity_fields:
            identity_fields = [f.name for f in self.fields]
        object.__setattr__(self, "identity", identity_fields)

        seen_names = {f.name for f in self.fields}
        new_fields = list(self.fields)
        for field_name in identity_fields:
            if field_name not in seen_names:
                new_fields.append(Field(name=field_name, type=None))
                seen_names.add(field_name)
        object.__setattr__(self, "fields", new_fields)
        return self

    @property
    def field_names(self) -> list[str]:
        """Get list of field names (as strings)."""
        return [field.name for field in self.fields]

    def get_fields(self) -> list[Field]:
        return self.fields

    def finish_init(self):
        """Complete logical initialization for vertex."""
        return None


class VertexConfig(ConfigBaseModel):
    """Configuration for managing vertices.

    This class manages vertices, providing methods for accessing
    and manipulating vertex configurations.

    Attributes:
        vertices: List of vertex configurations
        blank_vertices: List of blank vertex names
        force_types: Dictionary mapping vertex names to type lists
    """

    # Allow extra keys when loading from YAML (e.g. vertex_config wrapper key)
    model_config = ConfigDict(extra="ignore")

    vertices: list[Vertex] = PydanticField(
        ...,
        description="List of vertex type definitions (name, fields, identity, filters).",
    )
    blank_vertices: list[str] = PydanticField(
        default_factory=list,
        description="Vertex names that may be created without explicit data (e.g. placeholders).",
    )
    force_types: dict[str, list] = PydanticField(
        default_factory=dict,
        description="Override mapping: vertex name -> list of field type names for type inference.",
    )
    _vertices_map: dict[str, Vertex] | None = PrivateAttr(default=None)
    _vertex_numeric_fields_map: dict[str, object] | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def build_vertices_map_and_validate_blank(self) -> "VertexConfig":
        object.__setattr__(
            self,
            "_vertices_map",
            {item.name: item for item in self.vertices},
        )
        object.__setattr__(self, "_vertex_numeric_fields_map", {})
        if set(self.blank_vertices) - set(self.vertex_set):
            raise ValueError(
                f" Blank vertices {self.blank_vertices} are not defined as vertices"
            )
        self._normalize_vertex_identities()
        return self

    def _normalize_vertex_identities(
        self,
    ) -> None:
        blank_id_field = "id"
        for vertex in self.vertices:
            if vertex.name in self.blank_vertices and not vertex.identity:
                vertex.identity = [blank_id_field]
            if not vertex.identity:
                raise ValueError(f"Vertex '{vertex.name}' must define identity fields")
            missing = [f for f in vertex.identity if f not in vertex.field_names]
            for field_name in missing:
                vertex.fields.append(Field(name=field_name, type=None))

    def _get_vertices_map(self) -> dict[str, Vertex]:
        """Return the vertices map (set by model validator)."""
        assert self._vertices_map is not None, "VertexConfig not fully initialized"
        return self._vertices_map

    @property
    def vertex_set(self):
        """Get set of vertex names.

        Returns:
            set[str]: Set of vertex names
        """
        return set(self._get_vertices_map().keys())

    @property
    def vertex_list(self):
        """Get list of vertex configurations.

        Returns:
            list[Vertex]: List of vertex configurations
        """
        return list(self._get_vertices_map().values())

    def _get_vertex_by_name(self, identifier: str) -> Vertex:
        """Get vertex by logical vertex name."""
        m = self._get_vertices_map()
        if identifier in m:
            return m[identifier]
        available_names = list(m.keys())
        raise KeyError(
            f"Vertex '{identifier}' not found by logical name. "
            f"Available names: {available_names}"
        )

    def identity_fields(self, vertex_name: str) -> list[str]:
        """Get identity fields for a vertex."""
        return list(self._get_vertices_map()[vertex_name].identity)

    def fields(self, vertex_name: str) -> list[Field]:
        """Get fields for a vertex.

        Args:
            vertex_name: Name of the vertex or storage name

        Returns:
            list[Field]: List of Field objects
        """
        vertex = self._get_vertex_by_name(vertex_name)

        return vertex.fields

    def fields_names(
        self,
        vertex_name: str,
    ) -> list[str]:
        """Get field names for a vertex as strings.

        Args:
            vertex_name: Name of the vertex or storage name

        Returns:
            list[str]: List of field names as strings
        """
        vertex = self._get_vertex_by_name(vertex_name)
        return vertex.field_names

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
            nmap = self._vertex_numeric_fields_map
            if nmap is not None and vertex_name in nmap:
                return nmap[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                " Accessing vertex numeric fields: vertex"
                f" {vertex_name} was not defined in config"
            )

    def filters(self, vertex_name) -> list[FilterExpression]:
        """Get filter clauses for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            list[FilterExpression]: List of filter expressions
        """
        m = self._get_vertices_map()
        if vertex_name in m:
            return m[vertex_name].filters
        else:
            return []

    def remove_vertices(self, names: set[str]) -> None:
        """Remove vertices by name.

        Removes vertices from the configuration and from blank_vertices
        when present. Mutates the instance in place.

        Args:
            names: Set of vertex names to remove
        """
        if not names:
            return
        self.vertices[:] = [v for v in self.vertices if v.name not in names]
        m = self._get_vertices_map()
        for n in names:
            m.pop(n, None)
        self.blank_vertices[:] = [b for b in self.blank_vertices if b not in names]

    def update_vertex(self, v: Vertex):
        """Update vertex configuration.

        Args:
            v: Vertex configuration to update
        """
        self._get_vertices_map()[v.name] = v

    def __getitem__(self, key: str):
        """Get vertex configuration by name.

        Args:
            key: Vertex name

        Returns:
            Vertex: Vertex configuration

        Raises:
            KeyError: If vertex is not found
        """
        m = self._get_vertices_map()
        if key in m:
            return m[key]
        else:
            raise KeyError(f"Vertex {key} absent")

    def __setitem__(self, key: str, value: Vertex):
        """Set vertex configuration by name.

        Args:
            key: Vertex name
            value: Vertex configuration
        """
        self._get_vertices_map()[key] = value

    def finish_init(self):
        """Complete logical initialization of vertices."""
        self._normalize_vertex_identities()
        for v in self.vertices:
            v.finish_init()
