"""Vertex router actor for routing nested JSON observations to vertex actors."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

from .base import ActorInitContext, VertexProducingActor
from .config import (
    VertexActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    merge_observation_with_transform_buffer,
)
from graflo.architecture.schema.vertex import VertexConfig, VertexName

if TYPE_CHECKING:
    from .wrapper import ActorWrapper

logger = logging.getLogger(__name__)


class VertexRouterActor(VertexProducingActor):
    """Routes documents to the correct VertexActor based on a type field.

    The merged observation (document + same-location transform buffer) is passed
    through to the selected :class:`VertexActor` unchanged. Projection uses the same
    ``from`` / ``vertex_from_map`` contract as a standalone vertex step.

    Vertices are accumulated at ``lindex.extend((role, 0))``. ``role`` is normalized
    by config validation (defaults to :attr:`type_field` when omitted), so runtime slot
    addressing uses a single internal key. A downstream dynamic ``EdgeActor`` references
    this slot via ``source_role`` / ``target_role`` (or ``source_type_field`` /
    ``target_type_field``) using the same segment name.
    """

    def __init__(self, config: VertexRouterActorConfig):
        self.type_field = config.type_field
        # Config normalization guarantees role is always present.
        self.role: str = config.role or config.type_field
        self.from_doc: dict[str, str] | None = config.from_doc
        self.keep_fields: tuple[str, ...] | None = (
            tuple(config.keep_fields) if config.keep_fields else None
        )
        self.extraction_scope: Literal["full", "mapped_only"] = config.extraction_scope
        self.type_map: dict[str, str] = config.type_map or {}
        self.vertex_from_map: dict[str, dict[str, str]] = config.vertex_from_map or {}
        self._vertex_actors: dict[str, ActorWrapper] = {}
        self._init_ctx: ActorInitContext | None = None
        self.vertex_config: VertexConfig = VertexConfig(vertices=[])

    @classmethod
    def from_config(cls, config: VertexRouterActorConfig) -> VertexRouterActor:
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        items: dict[str, Any] = {"type_field": self.type_field, "role": self.role}
        if self.from_doc:
            items["from_doc"] = self.from_doc
        if self.keep_fields:
            items["keep_fields"] = list(self.keep_fields)
        items["extraction_scope"] = self.extraction_scope
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
            extraction_scope=self.extraction_scope,
        )
        wrapper = ActorWrapper.from_config(config)
        wrapper.finish_init(self._init_ctx)
        self._vertex_actors[vertex_type] = wrapper
        logger.debug(
            "VertexRouterActor: lazily registered VertexActor(%s) for type_field=%s role=%s",
            vertex_type,
            self.type_field,
            self.role,
        )
        return wrapper

    def count(self) -> int:
        return 1 + sum(w.count() for w in self._vertex_actors.values())

    def references_vertices(self) -> set[VertexName]:
        return set(self._vertex_actors.keys())

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
        buffer_items: list[Any] = list(ctx.transform_buffer.get(lindex, []))
        doc = merge_observation_with_transform_buffer(raw_observation, buffer_items)
        ctx.obs_buffer[lindex] = dict(doc)
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

        effective_lindex = lindex.extend((self.role, 0))
        return wrapper(ctx, effective_lindex, doc=doc)
