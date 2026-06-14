"""Index and weight configuration models."""

from __future__ import annotations

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.enums import IndexType
from graflo.onto import DBType


class ABCFields(ConfigBaseModel):
    """Base model for entities that have fields.

    Attributes:
        name: Optional name of the entity
        fields: List of field names
    """

    name: str | None = Field(
        default=None,
        description="Optional name of the entity (e.g. vertex name for composite field prefix).",
    )
    fields: list[str] = Field(
        default_factory=list,
        description="List of field names for this entity.",
    )
    keep_vertex_name: bool = Field(
        default=True,
        description="If True, composite field names use entity@field format; otherwise use field only.",
    )

    def cfield(self, x: str) -> str:
        """Creates a composite field name by combining the entity name with a field name.

        Args:
            x: Field name to combine with entity name

        Returns:
            Composite field name in format "entity@field"
        """
        return f"{self.name}@{x}" if self.keep_vertex_name else x


class Weight(ABCFields):
    """Defines weight configuration for edges.

    Attributes:
        map: Dictionary mapping field values to weights
        filter: Dictionary of filter conditions for weights
    """

    map: dict = Field(
        default_factory=dict,
        description="Mapping of field values to weight values for vertex-based edge attributes.",
    )
    filter: dict = Field(
        default_factory=dict,
        description="Filter conditions applied when resolving vertex-based weights.",
    )


class Index(ConfigBaseModel):
    """Configuration for database indexes.

    Attributes:
        name: Optional name of the index
        fields: List of fields to index
        unique: Whether the index enforces uniqueness
        type: Type of index to create
        deduplicate: Whether to deduplicate index entries
        sparse: Whether to create a sparse index
        exclude_edge_endpoints: Whether to exclude edge endpoints from index
    """

    name: str | None = Field(
        default=None,
        description="Optional index name. For edges, can reference a vertex name for composite fields.",
    )
    fields: list[str] = Field(
        default_factory=list,
        description="List of field names included in this index.",
    )
    unique: bool = Field(
        default=True,
        description="If True, index enforces uniqueness on the field combination.",
    )
    type: IndexType = Field(
        default=IndexType.PERSISTENT,
        description="Index type (PERSISTENT, HASH, SKIPLIST, FULLTEXT).",
    )
    deduplicate: bool = Field(
        default=True,
        description="Whether to deduplicate index entries (e.g. ArangoDB).",
    )
    sparse: bool = Field(
        default=False,
        description="If True, create a sparse index (exclude null/missing values).",
    )
    exclude_edge_endpoints: bool = Field(
        default=False,
        description="If True, do not add _from/_to to edge index (e.g. ArangoDB).",
    )

    def __iter__(self):
        """Iterate over the indexed fields."""
        return iter(self.fields)

    def db_form(self, db_type: DBType) -> dict:
        """Convert index configuration to database-specific format.

        Args:
            db_type: Type of database (ARANGO or NEO4J)

        Returns:
            Dictionary of index configuration in database-specific format

        Raises:
            ValueError: If db_type is not supported
        """
        r = dict(self.to_dict())
        if db_type == DBType.ARANGO:
            r.pop("name", None)
            r.pop("exclude_edge_endpoints", None)
        return r
