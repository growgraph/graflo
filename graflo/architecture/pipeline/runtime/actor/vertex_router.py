"""Vertex router actor for routing nested JSON observations to vertex actors."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .base import Actor, ActorInitContext
from .config import (
    VertexActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    merge_observation_with_transform_buffer,
)
from graflo.architecture.schema.vertex import VertexConfig

if TYPE_CHECKING:
    from .wrapper import ActorWrapper

logger = logging.getLogger(__name__)


class VertexRouterActor(Actor):
    """Routes documents to the correct VertexActor based on a type field.

    The merged observation (document + same-location transform buffer) is passed
    through to the selected :class:`VertexActor` unchanged. Projection uses the same
    ``from`` / ``vertex_from_map`` contract as a standalone vertex step.

    Vertices are accumulated at ``lindex.extend((slot, 0))`` where ``slot`` is
    :attr:`role` when set, otherwise :attr:`type_field`. A downstream dynamic
    ``EdgeActor`` references this slot via ``source_type_field`` /
    ``target_type_field`` (or ``source_role`` / ``target_role``) using the same
    segment name.
    """

    def __init__(self, config: VertexRouterActorConfig):
        self.type_field = config.type_field
        self.role = config.role
        self.from_doc: dict[str, str] | None = config.from_doc
        self.keep_fields: tuple[str, ...] | None = (
            tuple(config.keep_fields) if config.keep_fields else None
        )
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
        if self.role is not None:
            items["role"] = self.role
        if self.from_doc:
            items["from_doc"] = self.from_doc
        if self.keep_fields:
            items["keep_fields"] = list(self.keep_fields)
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
        from .wrapper import ActorWrapper

        if vertex_type not in self.vertex_config.vertex_set:
            return None
        wrapper = self._vertex_actors.get(vertex_type)
        if wrapper is not None:
            return wrapper
        if self._init_ctx is None:
            raise RuntimeError(
                "VertexRouterActor._get_or_create_wrapper called before finish_init"
            )

        if vertex_type in self.vertex_from_map:
            per_type_from = self.vertex_from_map[vertex_type]
        else:
            per_type_from = self.from_doc
        config = VertexActorConfig(
            vertex=vertex_type,
            from_doc=per_type_from,
            keep_fields=list(self.keep_fields) if self.keep_fields else None,
        )
        wrapper = ActorWrapper.from_config(config)
        wrapper.finish_init(self._init_ctx)
        self._vertex_actors[vertex_type] = wrapper
        slot = self.role if self.role is not None else self.type_field
        logger.debug(
            "VertexRouterActor: lazily registered VertexActor(%s) for type_field=%s slot=%s",
            vertex_type,
            self.type_field,
            slot,
        )
        return wrapper

    def count(self) -> int:
        return 1 + sum(w.count() for w in self._vertex_actors.values())

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        raw_observation = kwargs.get("doc", {})
        if not isinstance(raw_observation, dict):
            logger.debug(
                "VertexRouterActor: expected dict observation slice, got %s, skipping",
                type(raw_observation).__name__,
            )
            return ctx
        buffer_items: list[Any] = list(ctx.buffer_transforms.get(lindex, []))
        doc = merge_observation_with_transform_buffer(raw_observation, buffer_items)
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

        slot = self.role if self.role is not None else self.type_field
        effective_lindex = lindex.extend((slot, 0))
        return wrapper(ctx, effective_lindex, doc=doc)
