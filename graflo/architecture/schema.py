"""Graph database schema management and configuration.

This module provides the core schema management functionality for graph databases.
It defines the structure and configuration of vertices, edges, and resources
that make up the graph database schema.

Key Components:
    - Schema: Main schema container with metadata and configurations
    - SchemaMetadata: Schema versioning and naming information
    - Resource: Resource definitions for data processing
    - VertexConfig: Vertex collection configurations
    - EdgeConfig: Edge collection configurations

The schema system provides:
    - Schema versioning and metadata
    - Resource management and validation
    - Vertex and edge configuration
    - Transform registration and management

Example:
    >>> schema = Schema(
    ...     general=SchemaMetadata(name="social_network", version="1.0"),
    ...     vertex_config=VertexConfig(...),
    ...     edge_config=EdgeConfig(...),
    ...     resources=[Resource(...)]
    ... )
    >>> resource = schema.fetch_resource("users")
"""

import dataclasses
import logging
from collections import Counter
from typing import Optional

from graflo.architecture.edge import EdgeConfig
from graflo.architecture.resource import Resource
from graflo.architecture.transform import ProtoTransform
from graflo.architecture.vertex import VertexConfig
from graflo.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SchemaMetadata(BaseDataclass):
    """Schema metadata and versioning information.

    This class holds metadata about the schema, including its name and version.
    It's used for schema identification and versioning.

    Attributes:
        name: Name of the schema
        version: Optional version string of the schema
    """

    name: str
    version: Optional[str] = None


@dataclasses.dataclass
class Schema(BaseDataclass):
    """Graph database schema configuration.

    This class represents the complete schema configuration for a graph database.
    It manages resources, vertex configurations, edge configurations, and transforms.

    Attributes:
        general: Schema metadata and versioning information
        vertex_config: Configuration for vertex collections
        edge_config: Configuration for edge collections
        resources: List of resource definitions
        transforms: Dictionary of available transforms
        _resources: Internal mapping of resource names to resources
    """

    general: SchemaMetadata
    vertex_config: VertexConfig
    edge_config: EdgeConfig
    resources: list[Resource]
    transforms: dict[str, ProtoTransform] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        """Initialize the schema after dataclass initialization.

        Sets up transforms, initializes edge configuration, and validates
        resource names for uniqueness.

        Raises:
            ValueError: If duplicate resource names are found
        """
        for name, t in self.transforms.items():
            t.name = name

        self.edge_config.finish_init(self.vertex_config)

        for r in self.resources:
            r.finish_init(
                vertex_config=self.vertex_config,
                edge_config=self.edge_config,
                transforms=self.transforms,
            )

        names = [r.name for r in self.resources]
        c = Counter(names)
        for k, v in c.items():
            if v > 1:
                raise ValueError(f"resource name {k} used {v} times")
        self._resources: dict[str, Resource] = {}
        for r in self.resources:
            self._resources[r.name] = r

    def fetch_resource(self, name: str | None = None) -> Resource:
        """Fetch a resource by name or get the first available resource.

        Args:
            name: Optional name of the resource to fetch

        Returns:
            Resource: The requested resource

        Raises:
            ValueError: If the requested resource is not found or if no resources exist
        """
        _current_resource = None

        if name is not None:
            if name in self._resources:
                _current_resource = self._resources[name]
            else:
                raise ValueError(f"Resource {name} not found")
        else:
            if self._resources:
                _current_resource = self.resources[0]
            else:
                raise ValueError("Empty resource container 😕")
        return _current_resource
