"""Actor-based system for graph data transformation and processing.

This submodule provides:
- Actor: Abstract base class for all actors
- VertexActor, EdgeActor, TransformActor, DescendActor
- VertexRouterActor, EdgeRouterActor
- ActorWrapper: Wrapper for managing actor instances
"""

from .base import (
    Actor,
    ActorConstants,
    ActorInitContext,
)
from .descend import DescendActor
from .edge import EdgeActor
from .edge_router import EdgeRouterActor
from .transform import TransformActor
from .vertex import VertexActor
from .vertex_router import VertexRouterActor
from .wrapper import ActorWrapper

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
