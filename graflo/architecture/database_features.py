"""Database-specific schema features.

This module stores physical DB features that are separate from logical graph identity.
"""

from __future__ import annotations

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.onto import EdgeId, Index
from graflo.onto import DBType


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


class EdgeNameSpec(ConfigBaseModel):
    """Physical naming overrides for one edge definition."""

    source: str = PydanticField(..., description="Edge source vertex name.")
    target: str = PydanticField(..., description="Edge target vertex name.")
    purpose: str | None = PydanticField(
        default=None, description="Optional edge purpose identifier."
    )
    logical_relation: str | None = PydanticField(
        default=None,
        description="Logical relation label used to disambiguate edges sharing edge_id.",
    )
    relation_name: str | None = PydanticField(
        default=None,
        description="Database-specific relation/type name for the edge.",
    )
    storage_name: str | None = PydanticField(
        default=None,
        description="Physical edge storage/collection name override.",
    )
    graph_name: str | None = PydanticField(
        default=None,
        description="Physical graph/container name override.",
    )

    @property
    def edge_id(self) -> EdgeId:
        return (self.source, self.target, self.purpose)


class DatabaseFeatures(ConfigBaseModel):
    """Container for DB-only physical features such as secondary indexes."""

    db_flavor: DBType = PydanticField(
        default=DBType.ARANGO,
        description="Target DB flavor used for physical naming and defaults.",
    )
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
    edge_names: list[EdgeNameSpec] = PydanticField(
        default_factory=list,
        description="Physical naming overrides for edges keyed by edge identity.",
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

    def edge_name_spec(
        self, edge_id: EdgeId, logical_relation: str | None = None
    ) -> EdgeNameSpec | None:
        fallback: EdgeNameSpec | None = None
        for item in self.edge_names:
            if item.edge_id != edge_id:
                continue
            if item.logical_relation == logical_relation:
                return item
            if item.logical_relation is None:
                fallback = item
        return fallback

    def set_edge_name_spec(
        self,
        edge_id: EdgeId,
        *,
        logical_relation: str | None = None,
        relation_name: str | None = None,
        storage_name: str | None = None,
        graph_name: str | None = None,
    ) -> None:
        spec = self.edge_name_spec(edge_id, logical_relation)
        if spec is None:
            source, target, purpose = edge_id
            spec = EdgeNameSpec(
                source=source,
                target=target,
                purpose=purpose,
                logical_relation=logical_relation,
            )
            self.edge_names.append(spec)
        if relation_name is not None:
            spec.relation_name = relation_name
        if storage_name is not None:
            spec.storage_name = storage_name
        if graph_name is not None:
            spec.graph_name = graph_name

    def edge_relation_name(
        self,
        edge_id: EdgeId,
        default_relation: str | None = None,
        logical_relation: str | None = None,
    ) -> str | None:
        spec = self.edge_name_spec(edge_id, logical_relation)
        if spec is not None and spec.relation_name is not None:
            return spec.relation_name
        return default_relation

    def edge_storage_name(
        self,
        edge_id: EdgeId,
        *,
        source_storage: str,
        target_storage: str,
    ) -> str | None:
        spec = self.edge_name_spec(edge_id)
        if spec is not None and spec.storage_name is not None:
            return spec.storage_name
        if self.db_flavor != DBType.ARANGO:
            return None
        source, target, purpose = edge_id
        tokens = [source_storage, target_storage]
        if purpose is not None:
            tokens.append(purpose)
        return "_".join(tokens + ["edges"])

    def edge_graph_name(
        self,
        edge_id: EdgeId,
        *,
        source_storage: str,
        target_storage: str,
    ) -> str | None:
        spec = self.edge_name_spec(edge_id)
        if spec is not None and spec.graph_name is not None:
            return spec.graph_name
        if self.db_flavor != DBType.ARANGO:
            return None
        _, _, purpose = edge_id
        tokens = [source_storage, target_storage]
        if purpose is not None:
            tokens.append(purpose)
        return "_".join(tokens + ["graph"])
