"""Edge actor for processing edge data."""

from __future__ import annotations

from typing import Any

from .base import Actor, ActorInitContext
from .config import EdgeActorConfig
from graflo.architecture.edge_derivation import EdgeDerivation
from graflo.architecture.schema.edge import Edge
from graflo.architecture.graph_types import ExtractionContext, LocationIndex, Weight


class EdgeActor(Actor):
    """Actor for processing edge data."""

    def __init__(self, config: EdgeActorConfig):
        self.derivation: EdgeDerivation = config.derivation
        self._pending_vertex_weights: list[Weight] = []
        payload: dict[str, Any] = {
            "source": config.source,
            "target": config.target,
        }
        if config.relation is not None:
            payload["relation"] = config.relation
        if config.description is not None:
            payload["description"] = config.description
        if config.properties:
            payload["properties"] = config.properties
        for item in config.vertex_weights:
            self._pending_vertex_weights.append(Weight.model_validate(item))
        self.edge = Edge.from_dict(payload)
        self.vertex_config: Any = None
        self.allowed_vertex_names: set[str] | None = None

    @property
    def relation_field(self) -> str | None:
        """Alias for tooling (e.g. plot labels)."""
        return self.derivation.relation_field

    @classmethod
    def from_config(cls, config: EdgeActorConfig) -> EdgeActor:
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        return {
            k: v
            for k, v in {
                "source": self.edge.source,
                "target": self.edge.target,
                "match_source": self.derivation.match_source,
                "match_target": self.derivation.match_target,
            }.items()
            if v is not None
        }

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.vertex_config = init_ctx.vertex_config
        self.allowed_vertex_names = init_ctx.allowed_vertex_names
        if self.vertex_config is not None:
            edge_id = self.edge.edge_id
            init_ctx.edge_config.update_edges(
                self.edge, vertex_config=self.vertex_config
            )
            if self.derivation.relation_from_key:
                init_ctx.edge_derivation.mark_relation_from_key(edge_id)
            if self._pending_vertex_weights:
                init_ctx.edge_derivation.merge_vertex_weights(
                    edge_id, self._pending_vertex_weights
                )
            self.edge = init_ctx.edge_config.edge_for(edge_id)

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
        der = None if self.derivation.is_empty() else self.derivation
        ctx.record_edge_intent(
            edge=self.edge,
            location=lindex,
            derivation=der,
        )
        return ctx

    def references_vertices(self) -> set[str]:
        return {self.edge.source, self.edge.target}
