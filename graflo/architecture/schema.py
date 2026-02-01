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

from graflo.architecture.actor import EdgeActor, TransformActor, VertexActor
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
    version: str | None = None


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

    def remove_disconnected_vertices(self) -> None:
        """Remove vertices that do not take part in any relation (disconnected).

        Builds the set of vertex names that appear as source or target of any
        edge, then removes from VertexConfig all other vertices. For each
        resource, finds actors that reference disconnected vertices (via
        find_descendants) and removes them from the actor tree. Resources
        whose root actor references only disconnected vertices are removed.

        Mutates this schema in place.
        """
        connected = self.edge_config.vertices
        disconnected = self.vertex_config.vertex_set - connected
        if not disconnected:
            return

        self.vertex_config.remove_vertices(disconnected)

        def mentions_disconnected(wrapper):
            actor = wrapper.actor
            if isinstance(actor, VertexActor):
                return actor.name in disconnected
            if isinstance(actor, TransformActor):
                return actor.vertex is not None and actor.vertex in disconnected
            if isinstance(actor, EdgeActor):
                return (
                    actor.edge.source in disconnected
                    or actor.edge.target in disconnected
                )
            return False

        to_drop: list[Resource] = []
        for resource in self.resources:
            root = resource.root
            to_remove = set(
                root.find_descendants(actor_type=VertexActor, name=disconnected)
                + root.find_descendants(actor_type=TransformActor, vertex=disconnected)
                + root.find_descendants(
                    predicate=lambda w: isinstance(w.actor, EdgeActor)
                    and (
                        w.actor.edge.source in disconnected
                        or w.actor.edge.target in disconnected
                    ),
                )
            )
            if mentions_disconnected(root):
                to_drop.append(resource)
                continue
            root.remove_descendants_if(lambda w: w in to_remove)

        for r in to_drop:
            self.resources.remove(r)
            self._resources.pop(r.name, None)
