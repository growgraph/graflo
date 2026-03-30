"""Edge actor for processing edge data."""

from __future__ import annotations

from typing import Any

from .base import Actor, ActorInitContext
from .config import EdgeActorConfig
from graflo.architecture.schema.edge import Edge
from graflo.architecture.graph_types import ExtractionContext, LocationIndex


class EdgeActor(Actor):
    """Actor for processing edge data."""

    def __init__(self, config: EdgeActorConfig):
        kwargs = config.model_dump(by_alias=False, exclude_none=True)
        kwargs.pop("type", None)
        self.edge = Edge.from_dict(kwargs)
        self.vertex_config: Any = None
        self.allowed_vertex_names: set[str] | None = None

    @classmethod
    def from_config(cls, config: EdgeActorConfig) -> EdgeActor:
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        return {
            k: self.edge.__dict__[k]
            for k in ["source", "target", "match_source", "match_target"]
            if k in self.edge.__dict__
        }

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.vertex_config = init_ctx.vertex_config
        self.allowed_vertex_names = init_ctx.allowed_vertex_names
        if self.vertex_config is not None:
            init_ctx.edge_config.update_edges(
                self.edge, vertex_config=self.vertex_config
            )

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        # Early-exit for disallowed vertex endpoints.
        if self.allowed_vertex_names is not None and (
            self.edge.source not in self.allowed_vertex_names
            or self.edge.target not in self.allowed_vertex_names
        ):
            return ctx
        if (
            self.allowed_vertex_names is None
            and self.vertex_config is not None
            and (
                self.edge.source not in self.vertex_config.vertex_set
                or self.edge.target not in self.vertex_config.vertex_set
            )
        ):
            return ctx

        ctx.edge_requests.append((self.edge, lindex))
        ctx.record_edge_intent(edge=self.edge, location=lindex)
        return ctx

    def references_vertices(self) -> set[str]:
        return {self.edge.source, self.edge.target}
