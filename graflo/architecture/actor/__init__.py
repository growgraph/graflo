"""Actor-based system for graph data transformation and processing.

This submodule provides:
- Actor: Abstract base class for all actors
- VertexActor, EdgeActor, TransformActor, DescendActor
- VertexRouterActor, EdgeRouterActor
- ActorWrapper: Wrapper for managing actor instances
"""

from graflo.architecture.actor.base import (
    Actor,
    ActorConstants,
    ActorInitContext,
)
from graflo.architecture.actor.descend import DescendActor
from graflo.architecture.actor.edge import EdgeActor
from graflo.architecture.actor.edge_router import EdgeRouterActor
from graflo.architecture.actor.transform import TransformActor
from graflo.architecture.actor.vertex import VertexActor
from graflo.architecture.actor.vertex_router import VertexRouterActor
from graflo.architecture.actor.wrapper import ActorWrapper

__all__ = [
    "Actor",
    "ActorConstants",
    "ActorInitContext",
    "ActorWrapper",
    "DescendActor",
    "EdgeActor",
    "EdgeRouterActor",
    "TransformActor",
    "VertexActor",
    "VertexRouterActor",
]
