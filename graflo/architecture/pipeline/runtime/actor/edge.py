"""Edge actor for processing edge data."""

from __future__ import annotations

import logging
from typing import Any

from .base import Actor, ActorInitContext
from .config import EdgeActorConfig, EdgeLinkConfig
from graflo.architecture.edge_derivation import EdgeDerivation
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    Weight,
    merge_observation_with_transform_buffer,
)
from graflo.architecture.schema.vertex import VertexConfig

logger = logging.getLogger(__name__)


def _link_to_edge_actor_config(link: EdgeLinkConfig) -> EdgeActorConfig:
    """Convert an EdgeLinkConfig item into a standalone EdgeActorConfig for delegation."""
    data: dict[str, Any] = {"type": "edge"}
    # source/target role keys are canonicalized in EdgeLinkConfig.resolve_and_validate
    if link.source is not None:
        data["from"] = link.source
    if link.target is not None:
        data["to"] = link.target
    if link.source_role is not None:
        data["source_role"] = link.source_role
    if link.target_role is not None:
        data["target_role"] = link.target_role
    if link.relation is not None:
        data["relation"] = link.relation
    if link.relation_field is not None:
        data["relation_field"] = link.relation_field
    if link.match_source is not None:
        data["match_source"] = link.match_source
    if link.match_target is not None:
        data["match_target"] = link.match_target
    return EdgeActorConfig.model_validate(data)


class EdgeActor(Actor):
    """Actor for processing edge data.

    Operates in three modes determined by configuration:

    **Static mode** (``from``/``to`` set): both vertex types are declared at config
    time.  The schema ``Edge`` is created during ``finish_init`` and the
    ``__call__`` path is unchanged from the original implementation.

    **Dynamic mode** (at least one of ``source_role``/``target_role`` set, with
    ``source_type_field``/``target_type_field`` accepted as legacy aliases):
    vertex types for the dynamic side(s) are
    resolved at extraction time by looking up accumulator slots populated by an
    upstream ``VertexRouterActor`` (slot segment = ``role`` or ``type_field``) or a
    ``VertexActor`` with a matching ``role``.
    The schema ``Edge`` is created—or retrieved from cache—per unique
    ``(source_type, target_type, relation)`` triple encountered.

    **Multi-link mode** (``links`` list set): each item in ``links`` becomes a
    dedicated sub-``EdgeActor`` that runs in sequence per row, emitting one edge
    intent each.  Use when one flat row encodes multiple distinct relationships.
    """

    def __init__(self, config: EdgeActorConfig):
        # Multi-link mode: delegate each link to its own EdgeActor.
        if config.links:
            self._link_actors: list[EdgeActor] = [
                EdgeActor(_link_to_edge_actor_config(lk)) for lk in config.links
            ]
            # Null-out all single-intent state so the dispatch is unambiguous.
            self._source_slot_key = None
            self._target_slot_key = None
            self._static_source = None
            self._static_target = None
            self._relation_map: dict[str, str] = {}
            self._strict_edge_types = False
            self._edge_cache: dict[tuple[str, str, str | None], Edge] = {}
            self._init_ctx: ActorInitContext | None = None
            self.derivation: EdgeDerivation = EdgeDerivation()
            self._pending_vertex_weights: list[Weight] = []
            self._static_relation = None
            self.edge: Edge | None = None
            self.vertex_config: VertexConfig | None = None
            self.edge_config: EdgeConfig | None = None
            self.allowed_vertex_names: set[str] | None = None
            return

        self._link_actors = []

        self._source_slot_key = config.source_role
        self._target_slot_key = config.target_role
        # Static fallback for whichever side is not dynamic.
        self._static_source = config.source
        self._static_target = config.target
        self._relation_map = config.relation_map or {}
        self._strict_edge_types = config.strict_edge_types
        self._edge_cache = {}
        self._init_ctx = None

        self.derivation = config.derivation
        self._pending_vertex_weights = []

        # In dynamic/mixed mode the static relation (if set) is used as a fallback
        # when relation_field yields nothing.
        self._static_relation = None

        # Dynamic mode: at least one side is resolved at extraction time.
        # Static mode: both sides are fixed at config time.
        is_dynamic = (
            self._source_slot_key is not None or self._target_slot_key is not None
        )
        if not is_dynamic:
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
            self.edge: Edge | None = Edge.from_dict(payload)
        else:
            self.edge = None
            self._static_relation = config.relation

        self.vertex_config: VertexConfig | None = None
        self.edge_config: EdgeConfig | None = None
        self.allowed_vertex_names: set[str] | None = None

    @property
    def relation_field(self) -> str | None:
        """Alias for tooling (e.g. plot labels)."""
        return self.derivation.relation_field

    @classmethod
    def from_config(cls, config: EdgeActorConfig) -> "EdgeActor":
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        if self._link_actors:
            return {"links": str(len(self._link_actors))}
        items: dict[str, Any] = {}
        if self.edge is not None:
            items["source"] = self.edge.source
            items["target"] = self.edge.target
        else:
            if self._source_slot_key is not None:
                items["source_role"] = self._source_slot_key
            elif self._static_source is not None:
                items["source"] = self._static_source
            if self._target_slot_key is not None:
                items["target_role"] = self._target_slot_key
            elif self._static_target is not None:
                items["target"] = self._static_target
        for k in ("match_source", "match_target"):
            v = getattr(self.derivation, k)
            if v is not None:
                items[k] = v
        return items

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self._init_ctx = init_ctx
        self.vertex_config = init_ctx.vertex_config
        self.edge_config = init_ctx.edge_config
        self.allowed_vertex_names = init_ctx.allowed_vertex_names

        if self._link_actors:
            # Multi-link mode: delegate finish_init to each sub-actor.
            for la in self._link_actors:
                la.finish_init(init_ctx)
            return

        if self.edge is not None:
            # Static mode: register schema Edge now.
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
        else:
            # Dynamic mode: cache will be populated per-row.
            self._edge_cache.clear()

    # ------------------------------------------------------------------
    # Dynamic-mode helpers
    # ------------------------------------------------------------------

    def _get_or_create_edge(
        self, source: str, target: str, relation: str | None
    ) -> Edge | None:
        key = (source, target, relation)
        if key in self._edge_cache:
            return self._edge_cache[key]
        if self._strict_edge_types:
            # Skip if this (source, target, relation) was not pre-declared.
            if self.edge_config is not None and key not in self.edge_config:
                logger.debug(
                    "EdgeActor: strict_edge_types=True, skipping undeclared (%s, %s, %s)",
                    source,
                    target,
                    relation,
                )
                return None
        edge = Edge(source=source, target=target, relation=relation)
        if self.vertex_config is not None:
            edge.finish_init(vertex_config=self.vertex_config)
        if self.edge_config is not None and self.vertex_config is not None:
            self.edge_config.update_edges(edge, vertex_config=self.vertex_config)
        self._edge_cache[key] = edge
        logger.debug(
            "EdgeActor: registered dynamic edge (%s, %s, %s)", source, target, relation
        )
        return edge

    def _find_type_at_slot(
        self, ctx: ExtractionContext, slot_lindex: LocationIndex
    ) -> str | None:
        """Scan acc_vertex to find which vertex type has data at *slot_lindex*."""
        for vtype, by_loc in ctx.acc_vertex.items():
            if slot_lindex in by_loc and by_loc[slot_lindex]:
                return vtype
        return None

    # ------------------------------------------------------------------
    # Main dispatch
    # ------------------------------------------------------------------

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        if self._link_actors:
            # Multi-link mode: run each sub-actor in sequence.
            for la in self._link_actors:
                ctx = la(ctx, lindex, *nargs, **kwargs)
            return ctx
        if self._source_slot_key is not None or self._target_slot_key is not None:
            return self._call_dynamic(ctx, lindex, **kwargs)
        return self._call_static(ctx, lindex, **kwargs)

    def _call_static(
        self, ctx: ExtractionContext, lindex: LocationIndex, **kwargs: Any
    ) -> ExtractionContext:
        """Static mode: unchanged behavior from original EdgeActor."""
        assert self.edge is not None
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

        der = None if self.derivation.is_empty() else self.derivation
        ctx.record_edge_intent(
            edge=self.edge,
            location=lindex,
            derivation=der,
        )
        return ctx

    def _call_dynamic(
        self, ctx: ExtractionContext, lindex: LocationIndex, **kwargs: Any
    ) -> ExtractionContext:
        """Dynamic / mixed mode: resolve dynamic side(s) from VRA accumulator slots.

        Source or target (but not both) may be statically declared; that side's
        type is taken directly from config rather than looked up in the accumulator.
        """
        raw_observation = kwargs.get("doc", {})
        if not isinstance(raw_observation, dict):
            logger.debug(
                "EdgeActor: expected dict observation, got %s, skipping",
                type(raw_observation).__name__,
            )
            return ctx

        buffer_items: list[Any] = list(ctx.transform_buffer.get(lindex, []))
        doc = merge_observation_with_transform_buffer(raw_observation, buffer_items)
        ctx.obs_buffer[lindex] = dict(doc)

        # --- source type ---
        if self._source_slot_key is not None:
            source_slot_lindex = lindex.extend((self._source_slot_key, 0))
            source_type = self._find_type_at_slot(ctx, source_slot_lindex)
            if source_type is None:
                logger.debug(
                    "EdgeActor: no vertex data at source slot '%s', skipping",
                    self._source_slot_key,
                )
                return ctx
        else:
            # Mixed mode: static source
            assert self._static_source is not None
            source_type = self._static_source

        if (
            self.vertex_config is not None
            and source_type not in self.vertex_config.vertex_set
        ):
            logger.debug(
                "EdgeActor: source type '%s' not in vertex_set, skipping", source_type
            )
            return ctx

        # --- target type ---
        if self._target_slot_key is not None:
            target_slot_lindex = lindex.extend((self._target_slot_key, 0))
            target_type = self._find_type_at_slot(ctx, target_slot_lindex)
            if target_type is None:
                logger.debug(
                    "EdgeActor: no vertex data at target slot '%s', skipping",
                    self._target_slot_key,
                )
                return ctx
        else:
            # Mixed mode: static target
            assert self._static_target is not None
            target_type = self._static_target

        if (
            self.vertex_config is not None
            and target_type not in self.vertex_config.vertex_set
        ):
            logger.debug(
                "EdgeActor: target type '%s' not in vertex_set, skipping", target_type
            )
            return ctx

        # allowed_vertex_names early-exit
        if self.allowed_vertex_names is not None and (
            source_type not in self.allowed_vertex_names
            or target_type not in self.allowed_vertex_names
        ):
            return ctx

        # --- relation ---
        raw_relation: str | None
        if self.derivation.relation_field:
            raw_relation = doc.get(self.derivation.relation_field)
        else:
            raw_relation = None

        if raw_relation is not None:
            relation: str | None = self._relation_map.get(raw_relation, raw_relation)
        else:
            relation = self._static_relation

        # Create / retrieve cached schema Edge.
        edge = self._get_or_create_edge(source_type, target_type, relation)
        if edge is None:
            return ctx

        # Build derivation: slot names for dynamic sides so render_edge can filter.
        derivation = EdgeDerivation(
            match_source=self._source_slot_key,
            match_target=self._target_slot_key,
        )
        ctx.record_edge_intent(edge=edge, location=lindex, derivation=derivation)
        return ctx

    def references_vertices(self) -> set[str]:
        if self._link_actors:
            result: set[str] = set()
            for la in self._link_actors:
                result |= la.references_vertices()
            return result
        if self.edge is not None:
            return {self.edge.source, self.edge.target}
        static: set[str] = set()
        if self._static_source:
            static.add(self._static_source)
        if self._static_target:
            static.add(self._static_target)
        return (
            static
            | {s for s, _, _ in self._edge_cache}
            | {t for _, t, _ in self._edge_cache}
        )
