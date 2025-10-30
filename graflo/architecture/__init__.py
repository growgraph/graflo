"""Graph database architecture components.

This package defines the core architecture components for graph database operations,
including schema management, resource handling, and data transformations.

Key Components:
    - Schema: Graph database schema definition and management
    - Resource: Data resource management and processing
    - Transform: Data transformation and standardization
    - Vertex: Vertex collection configuration
    - Edge: Edge collection configuration

Example:
    >>> from graflo.architecture import Schema, Resource
    >>> schema = Schema(
    ...     general={"name": "my_graph", "version": "1.0"},
    ...     vertex_config=vertex_config,
    ...     edge_config=edge_config
    ... )
    >>> resource = Resource(name="users", data=user_data)
"""

from .edge import Edge, EdgeConfig
from .onto import Index
from .resource import Resource
from .schema import Schema
from .vertex import FieldType, Vertex, VertexConfig

__all__ = [
    "Edge",
    "EdgeConfig",
    "FieldType",
    "Index",
    "Resource",
    "Schema",
    "Vertex",
    "VertexConfig",
]
