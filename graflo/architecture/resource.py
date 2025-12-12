"""Resource management and processing for graph databases.

This module provides the core resource handling functionality for graph databases.
It defines how data resources are processed, transformed, and mapped to graph
structures through a system of actors and transformations.

Key Components:
    - Resource: Main class for resource processing and transformation
    - ActorWrapper: Wrapper for processing actors
    - ActionContext: Context for processing actions

The resource system allows for:
    - Data encoding and transformation
    - Vertex and edge creation
    - Weight management
    - Collection merging
    - Type casting and validation

Example:
    >>> resource = Resource(
    ...     resource_name="users",
    ...     apply=[VertexActor("user"), EdgeActor("follows")],
    ...     encoding=EncodingType.UTF_8
    ... )
    >>> result = resource(doc)
"""

import dataclasses
import logging
from collections import defaultdict
from typing import Callable

from dataclass_wizard import JSONWizard

from graflo.architecture.actor import (
    ActorWrapper,
)
from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.onto import (
    ActionContext,
    EncodingType,
    GraphEntity,
)
from graflo.architecture.transform import ProtoTransform
from graflo.architecture.vertex import (
    VertexConfig,
)
from graflo.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass(kw_only=True)
class Resource(BaseDataclass, JSONWizard):
    """Resource configuration and processing.

    This class represents a data resource that can be processed and transformed
    into graph structures. It manages the processing pipeline through actors
    and handles data encoding, transformation, and mapping.

    Attributes:
        resource_name: Name of the resource
        apply: List of actors to apply in sequence
        encoding: Data encoding type (default: UTF_8)
        merge_collections: List of collections to merge
        extra_weights: List of additional edge weights
        types: Dictionary of field type mappings
        root: Root actor wrapper for processing
        vertex_config: Configuration for vertices
        edge_config: Configuration for edges
    """

    resource_name: str
    apply: list
    encoding: EncodingType = EncodingType.UTF_8
    merge_collections: list[str] = dataclasses.field(default_factory=list)
    extra_weights: list[Edge] = dataclasses.field(default_factory=list)
    types: dict[str, str] = dataclasses.field(default_factory=dict)
    edge_greedy: bool = True

    def __post_init__(self):
        """Initialize the resource after dataclass initialization.

        Sets up the actor wrapper and type mappings. Evaluates type expressions
        for field type casting.

        Raises:
            Exception: If type evaluation fails for any field
        """
        self.root = ActorWrapper(*self.apply)
        self._types: dict[str, Callable] = dict()
        self.vertex_config: VertexConfig
        self.edge_config: EdgeConfig
        for k, v in self.types.items():
            try:
                self._types[k] = eval(v)
            except Exception as ex:
                logger.error(
                    f"For resource {self.name} for field {k} failed to cast type {v} : {ex}"
                )

    @property
    def name(self):
        """Get the resource name.

        Returns:
            str: Name of the resource
        """
        return self.resource_name

    def finish_init(
        self,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, ProtoTransform],
    ):
        """Complete resource initialization.

        Initializes the resource with vertex and edge configurations,
        and sets up the processing pipeline.

        Args:
            vertex_config: Configuration for vertices
            edge_config: Configuration for edges
            transforms: Dictionary of available transforms
        """
        self.vertex_config = vertex_config
        self.edge_config = edge_config

        logger.debug(f"total resource actor count : {self.root.count()}")
        self.root.finish_init(
            vertex_config=vertex_config,
            transforms=transforms,
            edge_config=edge_config,
            edge_greedy=self.edge_greedy,
        )

        logger.debug(f"total resource actor count (after 2 finit): {self.root.count()}")

        for e in self.extra_weights:
            e.finish_init(vertex_config)

    def __call__(self, doc: dict) -> defaultdict[GraphEntity, list]:
        """Process a document through the resource pipeline.

        Args:
            doc: Document to process

        Returns:
            defaultdict[GraphEntity, list]: Processed graph entities
        """
        ctx = ActionContext()
        ctx = self.root(ctx, doc=doc)
        acc = self.root.normalize_ctx(ctx)
        return acc

    def count(self):
        """Get the total number of actors in the resource.

        Returns:
            int: Number of actors
        """
        return self.root.count()
