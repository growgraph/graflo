"""Database-specific schema features.

This module stores physical DB features that are separate from logical graph identity.
"""

from __future__ import annotations

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.onto import EdgeId, Index


class EdgeIndexSpec(ConfigBaseModel):
    """Secondary indexes for one edge definition."""

    source: str = PydanticField(..., description="Edge source vertex name.")
    target: str = PydanticField(..., description="Edge target vertex name.")
    purpose: str | None = PydanticField(
        default=None, description="Optional edge purpose identifier."
    )
    indexes: list[Index] = PydanticField(
        default_factory=list, description="Secondary physical indexes."
    )

    @property
    def edge_id(self) -> EdgeId:
        return (self.source, self.target, self.purpose)


class DatabaseFeatures(ConfigBaseModel):
    """Container for DB-only physical features such as secondary indexes."""

    vertex_storage_names: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Physical vertex collection/label names keyed by logical vertex name.",
    )
    vertex_indexes: dict[str, list[Index]] = PydanticField(
        default_factory=dict,
        description="Secondary indexes per vertex name (identity excluded).",
    )
    edge_indexes: list[EdgeIndexSpec] = PydanticField(
        default_factory=list,
        description="Secondary indexes per edge identity.",
    )

    def vertex_secondary_indexes(self, vertex_name: str) -> list[Index]:
        return list(self.vertex_indexes.get(vertex_name, []))

    def vertex_storage_name(self, vertex_name: str) -> str:
        return self.vertex_storage_names.get(vertex_name, vertex_name)

    def edge_secondary_indexes(self, edge_id: EdgeId) -> list[Index]:
        for item in self.edge_indexes:
            if item.edge_id == edge_id:
                return list(item.indexes)
        return []
