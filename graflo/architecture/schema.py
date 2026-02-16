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

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

from pydantic import (
    Field as PydanticField,
    PrivateAttr,
    field_validator,
    model_validator,
)

from graflo.architecture.actor import EdgeActor, TransformActor, VertexActor
from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.edge import EdgeConfig
from graflo.architecture.resource import Resource
from graflo.architecture.transform import ProtoTransform
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


class SchemaMetadata(ConfigBaseModel):
    """Schema metadata and versioning information.

    Holds metadata about the schema, including its name and version.
    Used for schema identification and versioning. Suitable for LLM-generated
    schema constituents.
    """

    name: str = PydanticField(
        ...,
        description="Name of the schema (e.g. graph or database identifier).",
    )
    version: str | None = PydanticField(
        default=None,
        description="Optional version string of the schema (e.g. semantic version).",
    )


class Schema(ConfigBaseModel):
    """Graph database schema configuration.

    Represents the complete schema configuration for a graph database.
    Manages resources, vertex configurations, edge configurations, and transforms.
    Suitable for LLM-generated schema constituents.
    """

    general: SchemaMetadata = PydanticField(
        ...,
        description="Schema metadata and versioning (name, version).",
    )
    vertex_config: VertexConfig = PydanticField(
        ...,
        description="Configuration for vertex collections (vertices, fields, indexes).",
    )
    edge_config: EdgeConfig = PydanticField(
        ...,
        description="Configuration for edge collections (edges, weights).",
    )
    resources: list[Resource] = PydanticField(
        default_factory=list,
        description="List of resource definitions (data pipelines mapping to vertices/edges).",
    )
    transforms: dict[str, ProtoTransform] = PydanticField(
        default_factory=dict,
        description="Dictionary of named transforms available to resources (name -> ProtoTransform).",
    )

    _resources: dict[str, Resource] = PrivateAttr()

    @field_validator("resources", mode="before")
    @classmethod
    def _coerce_resources_list(cls, v: Any) -> Any:
        """Accept empty dict as empty list for backward compatibility."""
        if isinstance(v, dict) and len(v) == 0:
            return []
        return v

    @model_validator(mode="after")
    def _init_schema(self) -> Schema:
        """Set transform names, finish edge/resource init, and build resource name map."""
        self.finish_init()
        return self

    def finish_init(self) -> None:
        """Complete schema initialization after construction or resource updates.

        Sets transform names, initializes edge configuration with vertex config,
        calls finish_init on each resource, validates unique resource names,
        and builds the internal _resources name-to-Resource mapping.

        Call this after assigning to resources (e.g. when inferring resources
        from a database) so that _resources and resource pipelines are correct.

        Raises:
            ValueError: If duplicate resource names are found.
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
        object.__setattr__(self, "_resources", {r.name: r for r in self.resources})

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
        edge, then removes from VertexConfig all other vertices.  For each
        resource, removes actors that reference disconnected vertices from the
        actor tree.  If a resource's root directly references a disconnected
        vertex (single-step pipeline) or becomes empty after pruning, the
        entire resource is removed.

        Mutates this schema in place.
        """
        connected = self.edge_config.vertices
        disconnected = self.vertex_config.vertex_set - connected
        if not disconnected:
            return

        self.vertex_config.remove_vertices(disconnected)

        def _mentions_disconnected(wrapper) -> bool:
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
            if _mentions_disconnected(root):
                to_drop.append(resource)
                continue
            root.remove_descendants_if(_mentions_disconnected)
            if not any(isinstance(a, VertexActor) for a in root.collect_actors()):
                to_drop.append(resource)

        for r in to_drop:
            self.resources.remove(r)
            self._resources.pop(r.name, None)
