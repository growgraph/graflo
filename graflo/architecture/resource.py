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
    - Dynamic vertex-type routing via VertexRouterActor in the pipeline

Example:
    >>> resource = Resource(
    ...     resource_name="users",
    ...     pipeline=[{"vertex": "user"}, {"edge": {"from": "user", "to": "user"}}],
    ...     encoding=EncodingType.UTF_8
    ... )
    >>> result = resource(doc)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Callable

from pydantic import AliasChoices, Field as PydanticField, PrivateAttr, model_validator

from graflo.architecture.actor import ActorWrapper
from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.onto import (
    ActionContext,
    EncodingType,
    GraphEntity,
)
from graflo.architecture.transform import ProtoTransform
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


class Resource(ConfigBaseModel):
    """Resource configuration and processing.

    Represents a data resource that can be processed and transformed into graph
    structures. Manages the processing pipeline through actors and handles data
    encoding, transformation, and mapping. Suitable for LLM-generated schema
    constituents.

    Dynamic vertex-type routing is handled by ``vertex_router`` steps in the
    pipeline (see :class:`~graflo.architecture.actor.VertexRouterActor`).
    """

    model_config = {"extra": "forbid"}

    resource_name: str = PydanticField(
        ...,
        description="Name of the resource (e.g. table or file identifier).",
    )
    pipeline: list[dict[str, Any]] = PydanticField(
        ...,
        description="Pipeline of actor steps to apply in sequence (vertex, edge, transform, descend). "
        'Each step is a dict, e.g. {"vertex": "user"} or {"edge": {"from": "a", "to": "b"}}.',
        validation_alias=AliasChoices("pipeline", "apply"),
    )
    encoding: EncodingType = PydanticField(
        default=EncodingType.UTF_8,
        description="Character encoding for input/output (e.g. utf-8, ISO-8859-1).",
    )
    merge_collections: list[str] = PydanticField(
        default_factory=list,
        description="List of collection names to merge when writing to the graph.",
    )
    extra_weights: list[Edge] = PydanticField(
        default_factory=list,
        description="Additional edge weight configurations for this resource.",
    )
    types: dict[str, str] = PydanticField(
        default_factory=dict,
        description='Field name to Python type expression for casting (e.g. {"amount": "float"}).',
    )
    edge_greedy: bool = PydanticField(
        default=True,
        description="If True, emit edges as soon as source/target vertices exist; if False, wait for explicit targets.",
    )

    _root: ActorWrapper = PrivateAttr()
    _types: dict[str, Callable[..., Any]] = PrivateAttr(default_factory=dict)
    _vertex_config: VertexConfig = PrivateAttr()
    _edge_config: EdgeConfig = PrivateAttr()

    @model_validator(mode="after")
    def _build_root_and_types(self) -> Resource:
        """Build root ActorWrapper from pipeline and evaluate type expressions."""
        object.__setattr__(self, "_root", ActorWrapper(*self.pipeline))
        object.__setattr__(self, "_types", {})
        for k, v in self.types.items():
            try:
                self._types[k] = eval(v)
            except Exception as ex:
                logger.error(
                    "For resource %s for field %s failed to cast type %s : %s",
                    self.name,
                    k,
                    v,
                    ex,
                )
        # Placeholders until finish_init is called by Schema
        object.__setattr__(
            self,
            "_vertex_config",
            VertexConfig(vertices=[]),
        )
        object.__setattr__(self, "_edge_config", EdgeConfig())
        return self

    @property
    def vertex_config(self) -> VertexConfig:
        """Vertex configuration (set by Schema.finish_init)."""
        return self._vertex_config

    @property
    def edge_config(self) -> EdgeConfig:
        """Edge configuration (set by Schema.finish_init)."""
        return self._edge_config

    @property
    def root(self) -> ActorWrapper:
        """Root actor wrapper for the processing pipeline."""
        return self._root

    @property
    def name(self) -> str:
        """Resource name (alias for resource_name)."""
        return self.resource_name

    def finish_init(
        self,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, ProtoTransform],
    ) -> None:
        """Complete resource initialization.

        Initializes the resource with vertex and edge configurations,
        and sets up the processing pipeline. Called by Schema after load.

        Args:
            vertex_config: Configuration for vertices
            edge_config: Configuration for edges
            transforms: Dictionary of available transforms
        """
        object.__setattr__(self, "_vertex_config", vertex_config)
        object.__setattr__(self, "_edge_config", edge_config)

        logger.debug("total resource actor count : %s", self.root.count())
        self.root.finish_init(
            vertex_config=vertex_config,
            transforms=transforms,
            edge_config=edge_config,
            edge_greedy=self.edge_greedy,
        )

        logger.debug("total resource actor count (after finit): %s", self.root.count())

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

    def count(self) -> int:
        """Total number of actors in the resource pipeline."""
        return self.root.count()
