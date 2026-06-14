"""Graph runtime types and data structures (extraction, assembly, containers).

This package defines graph-processing data structures used across the ingestion
pipeline and database adapters. It provides:

- Core data types for vertices and edges
- Database index configurations
- Graph container implementations
- Edge mapping and casting utilities
- Action context for graph transformations

The package is designed to be database-agnostic, supporting both ArangoDB and Neo4j
through the DBType enum. It provides a unified interface for working with graph data
structures while allowing for database-specific optimizations and features.

Submodules (for lighter imports):

- ``identifiers`` — vertex/edge id aliases and edge-key serialization
- ``enums`` — EdgeMapping, EncodingType, IndexType, EdgeType, EdgeCastingType
- ``index_config`` — ABCFields, Weight, Index
- ``container`` — GraphContainer
- ``location`` — LocationIndex, ProvenancePath
- ``transform`` — TransformPayload, VertexRep, merge helpers
- ``context`` — ExtractionContext, AssemblyContext, observations, ActionContext
"""

from __future__ import annotations

from graflo.architecture.graph_types.container import GraphContainer, ItemsView
from graflo.architecture.graph_types.context import (
    ActionContext,
    AssemblyContext,
    EdgeIntent,
    ExtractionContext,
    GraphAssemblyResult,
    ResourceCastResult,
    TransformCastFailure,
    TransformObservation,
    VertexObservation,
)
from graflo.architecture.graph_types.enums import (
    EdgeCastingType,
    EdgeMapping,
    EdgeType,
    EncodingType,
    IndexType,
)
from graflo.architecture.graph_types.identifiers import (
    EdgeId,
    EdgePhysicalKey,
    GraphEntity,
    VertexName,
    deserialize_edge_key,
    serialize_edge_key,
)
from graflo.architecture.graph_types.index_config import ABCFields, Index, Weight
from graflo.architecture.graph_types.location import LocationIndex, ProvenancePath
from graflo.architecture.graph_types.transform import (
    TransformPayload,
    VertexRep,
    context_dict_from_transform_buffer_item,
    merge_observation_with_transform_buffer,
    merge_row_doc_with_transform_buffer,
)

__all__ = [
    "ABCFields",
    "ActionContext",
    "AssemblyContext",
    "EdgeCastingType",
    "EdgeId",
    "EdgeIntent",
    "EdgeMapping",
    "EdgePhysicalKey",
    "EdgeType",
    "EncodingType",
    "ExtractionContext",
    "GraphAssemblyResult",
    "GraphContainer",
    "GraphEntity",
    "Index",
    "IndexType",
    "ItemsView",
    "LocationIndex",
    "ProvenancePath",
    "ResourceCastResult",
    "TransformCastFailure",
    "TransformObservation",
    "TransformPayload",
    "VertexName",
    "VertexObservation",
    "VertexRep",
    "Weight",
    "context_dict_from_transform_buffer_item",
    "deserialize_edge_key",
    "merge_observation_with_transform_buffer",
    "merge_row_doc_with_transform_buffer",
    "serialize_edge_key",
]
