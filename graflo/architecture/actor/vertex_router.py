"""Vertex router actor for routing documents to vertex actors by type field."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from graflo.architecture.actor.base import Actor, ActorInitContext
from graflo.architecture.actor.config import (
    VertexActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.onto import ExtractionContext, LocationIndex
from graflo.architecture.vertex import VertexConfig

if TYPE_CHECKING:
    from graflo.architecture.actor.wrapper import ActorWrapper

logger = logging.getLogger(__name__)


class VertexRouterActor(Actor):
    """Routes documents to the correct VertexActor based on a type field."""

    def __init__(self, config: VertexRouterActorConfig):
        self.type_field = config.type_field
        self.prefix = config.prefix
        self.field_map = config.field_map
        self.type_map: dict[str, str] = config.type_map or {}
        self.vertex_from_map: dict[str, dict[str, str]] = config.vertex_from_map or {}
        self._vertex_actors: dict[str, ActorWrapper] = {}
        self._init_ctx: ActorInitContext | None = None
        self.vertex_config: VertexConfig = VertexConfig(vertices=[])

    @classmethod
    def from_config(cls, config: VertexRouterActorConfig) -> VertexRouterActor:
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        items: dict[str, Any] = {"type_field": self.type_field}
        if self.prefix:
            items["prefix"] = self.prefix
        if self.field_map:
            items["field_map"] = self.field_map
        if self.type_map:
            items["type_map"] = self.type_map
        if self.vertex_from_map:
            items["vertex_from_map"] = self.vertex_from_map
        items["vertex_types"] = sorted(self._vertex_actors.keys())
        return items

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.vertex_config = init_ctx.vertex_config
        self._init_ctx = init_ctx
        self._vertex_actors.clear()

    def _get_or_create_wrapper(self, vertex_type: str) -> "ActorWrapper | None":
        from graflo.architecture.actor.wrapper import ActorWrapper

        if vertex_type not in self.vertex_config.vertex_set:
            return None
        wrapper = self._vertex_actors.get(vertex_type)
        if wrapper is not None:
            return wrapper
        if self._init_ctx is None:
            raise RuntimeError(
                "VertexRouterActor._get_or_create_wrapper called before finish_init"
            )

        from_doc = self.vertex_from_map.get(vertex_type)
        config = VertexActorConfig(vertex=vertex_type, from_doc=from_doc)
        wrapper = ActorWrapper.from_config(config)
        wrapper.finish_init(self._init_ctx)
        self._vertex_actors[vertex_type] = wrapper
        logger.debug(
            "VertexRouterActor: lazily registered VertexActor(%s) for type_field=%s",
            vertex_type,
            self.type_field,
        )
        return wrapper

    def count(self) -> int:
        return 1 + sum(w.count() for w in self._vertex_actors.values())

    def _extract_sub_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        if self.prefix:
            return {
                k[len(self.prefix) :]: v
                for k, v in doc.items()
                if k.startswith(self.prefix)
            }
        if self.field_map:
            return {
                new_key: doc[old_key]
                for old_key, new_key in self.field_map.items()
                if old_key in doc
            }
        return doc

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        doc: dict[str, Any] = kwargs.get("doc", {})
        raw_vtype = doc.get(self.type_field)
        if raw_vtype is None:
            logger.debug(
                "VertexRouterActor: type_field '%s' not in doc, skipping",
                self.type_field,
            )
            return ctx
        vtype = self.type_map.get(raw_vtype, raw_vtype)

        wrapper = self._get_or_create_wrapper(vtype)
        if wrapper is None:
            logger.debug(
                "VertexRouterActor: vertex type '%s' (from field '%s') "
                "not in VertexConfig, skipping",
                vtype,
                self.type_field,
            )
            return ctx

        sub_doc = self._extract_sub_doc(doc)
        if not sub_doc:
            return ctx

        return wrapper(ctx, lindex, doc=sub_doc)
