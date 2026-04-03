"""Edge configuration and management for graph databases.

This module provides classes and utilities for managing edges in graph databases.
It handles edge configuration, weight management, indexing, and relationship operations.
The module supports both ArangoDB and Neo4j through the DBType enum.

Key Components:
    - Edge: Abstract graph edge kind (schema / ``edge_config`` only)
    - EdgeDerivation: Ingestion wiring (see ``graflo.architecture.edge_derivation``)
    - EdgeConfig: Manages collections of edges and their configurations
    - WeightConfig: DTO for DB projection helpers (e.g. effective weights); schema uses ``properties``

Example:
    >>> edge = Edge(source="user", target="post")
    >>> config = EdgeConfig(edges=[edge])
    >>> edge.finish_init(vertex_config=vertex_config)
"""

from __future__ import annotations

from typing import Any, Iterator, cast

from pydantic import (
    Field as PydanticField,
    PrivateAttr,
    field_validator,
    model_validator,
)

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import (
    EdgeId,
    EdgeType,
)
from graflo.architecture.schema.vertex import Field, VertexConfig


# Default relation name for TigerGraph edges when relation is not specified
DEFAULT_TIGERGRAPH_RELATION = "relates"

# Default field name for storing extracted relations in TigerGraph weights
DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME = "relation"


def _normalize_direct_item(item: str | Field | dict[str, Any]) -> Field:
    """Convert a single direct field item (str, Field, or dict) to Field."""
    if isinstance(item, Field):
        return item
    if isinstance(item, str):
        return Field(name=item, type=None)
    if isinstance(item, dict):
        name = item.get("name")
        if name is None:
            raise ValueError(f"Field dict must have 'name' key: {item}")
        return Field(
            name=name,
            type=item.get("type"),
            description=item.get("description"),
        )
    raise TypeError(f"Field must be str, Field, or dict, got {type(item)}")


class Edge(ConfigBaseModel):
    """Abstract graph edge kind (schema / ``edge_config`` only).

    Ingestion-only behavior (location filters, relation column, relation from
    key, etc.) belongs on :class:`~graflo.architecture.edge_derivation.EdgeDerivation`
    in pipeline edge steps, not on this model.
    """

    source: str = PydanticField(
        ...,
        description="Source vertex type name (e.g. user, company).",
    )
    target: str = PydanticField(
        ...,
        description="Target vertex type name (e.g. post, company).",
    )
    relation: str | None = PydanticField(
        default=None,
        description="Relation/edge type name (e.g. Neo4j relationship type). For ArangoDB used as weight.",
    )
    description: str | None = PydanticField(
        default=None,
        description="Optional semantic description of edge intent, direction semantics, and business meaning.",
    )

    identities: list[list[str]] = PydanticField(
        default_factory=list,
        description=(
            "Logical uniqueness keys for this edge: each key names fields that, "
            "together with the resolved source and target vertex ids, must be unique "
            "(``source`` / ``target`` tokens stand for endpoints; other tokens are edge "
            "attributes). Multiple keys define multiple uniqueness constraints. "
            "Non-endpoint tokens are merged into ``properties`` during "
            ":meth:`finish_init` if not already declared (same idea as vertex identity)."
        ),
    )
    properties: list[Field] = PydanticField(
        default_factory=list,
        description=(
            "Edge property names/types (relationship properties). "
            "Vertex-derived bindings belong in ingestion (:class:`~graflo.architecture.contract."
            "declarations.edge_derivation_registry.EdgeDerivationRegistry`)."
        ),
    )

    type: EdgeType = PydanticField(
        default=EdgeType.DIRECT,
        description="Edge type: DIRECT (created during ingestion) or INDIRECT (pre-existing collection).",
    )

    by: str | None = PydanticField(
        default=None,
        description="For INDIRECT edges: vertex type name used to define the edge.",
    )

    @field_validator("properties", mode="before")
    @classmethod
    def normalize_properties(cls, v: Any) -> Any:
        if not isinstance(v, list):
            return v
        return [_normalize_direct_item(item) for item in v]

    @field_validator("identities", mode="before")
    @classmethod
    def normalize_identities(cls, v: Any) -> Any:
        if v is None:
            return []
        if isinstance(v, list):
            # identities can be provided as [["source", "target"], ["source", "target", "pub_id"]]
            if all(isinstance(item, str) for item in v):
                return [list(v)]
            normalized: list[list[str]] = []
            for item in v:
                if isinstance(item, tuple):
                    item = list(item)
                if not isinstance(item, list) or not all(
                    isinstance(token, str) for token in item
                ):
                    raise ValueError("edge identities must be list[list[str]]")
                normalized.append(cast(list[str], item))
            return normalized
        raise ValueError("edge identities must be list[list[str]]")

    @model_validator(mode="after")
    def normalize_identity_keys(self) -> "Edge":
        deduped_keys: list[list[str]] = []
        seen_keys: set[tuple[str, ...]] = set()
        for key in self.identities:
            deduped_tokens: list[str] = []
            for token in key:
                if token not in deduped_tokens:
                    deduped_tokens.append(token)
            key_tuple = tuple(deduped_tokens)
            if key_tuple and key_tuple not in seen_keys:
                seen_keys.add(key_tuple)
                deduped_keys.append(deduped_tokens)
        object.__setattr__(self, "identities", deduped_keys)
        return self

    def finish_init(self, vertex_config: VertexConfig):
        """Complete logical edge initialization with vertex configuration."""
        _ = vertex_config
        self._merge_identity_fields_into_properties()
        self._validate_identity_tokens()

    def _merge_identity_fields_into_properties(self) -> None:
        """Append :class:`Field` entries for identity tokens not already declared.

        Endpoint tokens ``source`` and ``target`` are not edge properties; every
        other token (including ``relation``) is materialized like vertex identity.
        """
        endpoint_tokens = frozenset({"source", "target"})
        seen_names = {f.name for f in self.properties}
        augmented = list(self.properties)
        for key in self.identities:
            for token in key:
                if token in endpoint_tokens:
                    continue
                if token not in seen_names:
                    augmented.append(Field(name=token, type=None))
                    seen_names.add(token)
        object.__setattr__(self, "properties", augmented)

    def _validate_identity_tokens(self) -> None:
        """Validate edge identity keys against reserved tokens and declared edge fields."""
        reserved = {"source", "target", "relation"}
        direct_weight_fields = set(self.property_names)
        # Identity token "relation" maps to the default TigerGraph attribute name
        # when physical fields are declared (see EdgeConfigDBAware.effective_weights).
        logical_relation_attr = {DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME}
        allowed_fields = reserved | direct_weight_fields | logical_relation_attr
        unknown_by_key = [
            [token for token in key if token not in allowed_fields]
            for key in self.identities
        ]
        unknown_by_key = [u for u in unknown_by_key if u]
        if unknown_by_key:
            raise ValueError(
                "Edge identity key fields must use reserved tokens "
                "('source', 'target', 'relation') or declared edge property / relation fields. "
                f"Edge ({self.source}, {self.target}, {self.relation}) has unknown identity fields: {unknown_by_key}"
            )

    @property
    def edge_name_dyad(self):
        """Get the edge name as a dyad (source, target).

        Returns:
            tuple[str, str]: Source and target vertex names
        """
        return self.source, self.target

    @property
    def edge_id(self) -> EdgeId:
        """Alias for edge_id."""
        return self.source, self.target, self.relation

    @property
    def property_names(self) -> list[str]:
        """Declared materialized edge property names."""
        return [f.name for f in self.properties]


class EdgeConfig(ConfigBaseModel):
    """Configuration for managing collections of edges.

    This class manages a collection of edges, providing methods for accessing
    and manipulating edge configurations.

    Attributes:
        edges: List of edge configurations
    """

    edges: list[Edge] = PydanticField(
        default_factory=list,
        description="List of edge definitions (source, target, identities, properties, relation, etc.).",
    )
    _edges_map: dict[EdgeId, Edge] = PrivateAttr()

    @model_validator(mode="after")
    def _build_edges_map(self) -> EdgeConfig:
        """Build internal mapping of edge IDs to edge configurations."""
        object.__setattr__(self, "_edges_map", {e.edge_id: e for e in self.edges})
        return self

    @staticmethod
    def _map_key(edge: Edge) -> EdgeId:
        return edge.edge_id

    def finish_init(self, vc: VertexConfig):
        """Complete initialization of all logical edges."""
        for e in self.edges:
            e.finish_init(vertex_config=vc)

    def values(self) -> Iterator[Edge]:
        """Iterate over edge configurations."""
        return iter(self._edges_map.values())

    def items(self) -> Iterator[tuple[EdgeId, Edge]]:
        """Iterate over ``(edge_id, edge)`` pairs."""
        return iter(self._edges_map.items())

    def __contains__(self, item: EdgeId | EdgeId | Edge):
        """Check if edge exists in configuration.

        Args:
            item: Edge ID or Edge instance to check

        Returns:
            bool: True if edge exists, False otherwise
        """
        if isinstance(item, Edge):
            return self._map_key(item) in self._edges_map
        if isinstance(item, tuple) and len(item) == 3:
            return item in self._edges_map
        return False

    def update_edges(
        self,
        edge: Edge,
        vertex_config: VertexConfig,
    ):
        """Update edge configuration.

        Args:
            edge: Edge configuration to update
            vertex_config: Vertex configuration
        """
        edge_key = self._map_key(edge)
        if edge_key in self._edges_map:
            self._edges_map[edge_key].update(edge)
        else:
            self._edges_map[edge_key] = edge
            self.edges.append(edge)

        self._edges_map[edge_key].finish_init(
            vertex_config=vertex_config,
        )

    def edge_for(self, edge_id: EdgeId) -> Edge:
        """Return the config-owned :class:`Edge` instance for ``edge_id`` after merges.

        Pipeline actors may construct a partial :class:`Edge` that is merged into the
        schema edge via :meth:`update_edges`. Callers that need properties, identities,
        etc. must use this object (same reference as in :meth:`items`), not the
        pre-merge actor copy.
        """
        return self._edges_map[edge_id]

    @property
    def vertices(self):
        """Get set of vertex names involved in edges.

        Returns:
            set[str]: Set of vertex names
        """
        return {e.source for e in self.edges} | {e.target for e in self.edges}
