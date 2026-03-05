"""Edge router actor for routing documents to dynamically created edges."""

from __future__ import annotations

import logging
from typing import Any

from graflo.architecture.actor.base import Actor, ActorInitContext
from graflo.architecture.actor.config import EdgeRouterActorConfig
from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.onto import ExtractionContext, LocationIndex, VertexRep
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


class EdgeRouterActor(Actor):
    """Routes documents to dynamically created edges based on type fields."""

    def __init__(self, config: EdgeRouterActorConfig):
        self.source_type_field = config.source_type_field
        self.target_type_field = config.target_type_field
        self.source_fields = config.source_fields
        self.target_fields = config.target_fields
        self.relation_field = config.relation_field
        self.relation = config.relation
        self._source_type_map: dict[str, str] = {
            **(config.type_map or {}),
            **(config.source_type_map or {}),
        }
        self._target_type_map: dict[str, str] = {
            **(config.type_map or {}),
            **(config.target_type_map or {}),
        }
        self._relation_map: dict[str, str] = config.relation_map or {}
        self._edge_cache: dict[tuple[str, str, str | None], Edge] = {}
        self._init_ctx: ActorInitContext | None = None
        self.vertex_config: VertexConfig = VertexConfig(vertices=[])
        self.edge_config: EdgeConfig = EdgeConfig()

    @classmethod
    def from_config(cls, config: EdgeRouterActorConfig) -> EdgeRouterActor:
        return cls(config)

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self._init_ctx = init_ctx
        self.vertex_config = init_ctx.vertex_config
        self.edge_config = init_ctx.edge_config
        self._edge_cache.clear()

    def _resolve_type(self, raw: str, type_map: dict[str, str]) -> str | None:
        resolved = type_map.get(raw, raw)
        if resolved not in self.vertex_config.vertex_set:
            logger.debug(
                "EdgeRouterActor: resolved type '%s' not in vertex_set, skipping",
                resolved,
            )
            return None
        return resolved

    def _resolve_relation(self, raw: str | None) -> str | None:
        if raw is None:
            return None
        return self._relation_map.get(raw, raw)

    def _get_or_create_edge(
        self,
        source_name: str,
        target_name: str,
        relation: str | None,
    ) -> Edge:
        key = (source_name, target_name, relation)
        if key in self._edge_cache:
            return self._edge_cache[key]
        edge = Edge(source=source_name, target=target_name, relation=relation)
        edge.finish_init(vertex_config=self.vertex_config)
        self.edge_config.update_edges(edge, vertex_config=self.vertex_config)
        self._edge_cache[key] = edge
        logger.debug(
            "EdgeRouterActor: registered dynamic edge (%s, %s, %s)",
            source_name,
            target_name,
            relation,
        )
        return edge

    def _project_vertex_doc(
        self,
        doc: dict[str, Any],
        fields: dict[str, str] | None,
        vertex_name: str,
    ) -> dict[str, Any]:
        if fields is not None:
            return {vf: doc[df] for vf, df in fields.items() if df in doc}
        identity = self.vertex_config.identity_fields(vertex_name)
        return {f: doc[f] for f in identity if f in doc}

    def __call__(
        self,
        ctx: ExtractionContext,
        lindex: LocationIndex,
        *nargs: Any,
        **kwargs: Any,
    ) -> ExtractionContext:
        doc: dict[str, Any] = kwargs.get("doc", {})

        raw_source = doc.get(self.source_type_field)
        raw_target = doc.get(self.target_type_field)
        if raw_source is None or raw_target is None:
            logger.debug("EdgeRouterActor: missing type field(s) in doc, skipping")
            return ctx

        source_name = self._resolve_type(raw_source, self._source_type_map)
        target_name = self._resolve_type(raw_target, self._target_type_map)
        if source_name is None or target_name is None:
            return ctx

        raw_relation = (
            doc.get(self.relation_field) if self.relation_field else self.relation
        )
        relation = self._resolve_relation(raw_relation)

        source_doc = self._project_vertex_doc(doc, self.source_fields, source_name)
        target_doc = self._project_vertex_doc(doc, self.target_fields, target_name)

        if not source_doc or not target_doc:
            logger.debug(
                "EdgeRouterActor: could not project identity docs for "
                "(%s, %s), skipping",
                source_name,
                target_name,
            )
            return ctx

        source_lindex = lindex.extend(("src", 0))
        target_lindex = lindex.extend(("tgt", 0))
        ctx.acc_vertex[source_name][source_lindex].append(
            VertexRep(vertex=source_doc, ctx={})
        )
        ctx.acc_vertex[target_name][target_lindex].append(
            VertexRep(vertex=target_doc, ctx={})
        )

        edge = self._get_or_create_edge(source_name, target_name, relation)
        ctx.edge_requests.append((edge, lindex))
        ctx.record_edge_intent(edge=edge, location=lindex)
        return ctx

    def references_vertices(self) -> set[str]:
        return {s for s, _, _ in self._edge_cache} | {t for _, t, _ in self._edge_cache}

    def fetch_important_items(self) -> dict[str, Any]:
        items: dict[str, Any] = {
            "source_type_field": self.source_type_field,
            "target_type_field": self.target_type_field,
            "relation_field": self.relation_field or "",
            "cached_edges": sorted(str(k) for k in self._edge_cache),
        }
        if self._relation_map:
            items["relation_map"] = self._relation_map
        return items
