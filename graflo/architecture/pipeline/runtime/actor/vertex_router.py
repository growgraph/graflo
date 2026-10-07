"""Vertex router actor for routing nested JSON observations to vertex actors."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any, Literal

from graflo.architecture.contract.ingestion.steps import (
    VertexActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    merge_observation_with_transform_buffer,
)
from graflo.architecture.schema.vertex import VertexConfig, VertexName

from .base import ActorInitContext, VertexProducingActor
from .vertex import (
    find_fields,
    refuse_undeclared_mapping,
    undeclared_mapping_targets,
)

if TYPE_CHECKING:
    from .wrapper import ActorWrapper

logger = logging.getLogger(__name__)

# Casting may run the same actor tree from several worker threads (sibling
# batches in flight); lazy child construction must not race. Module-level
# rather than per-actor: actors are deep-copied (manifest projection,
# evolution) and a held lock cannot cross a copy or pickle.
_VERTEX_ACTOR_CREATE_LOCK = threading.Lock()


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

    ``lookup_only`` (every routed class, or the listed ones) is handed to the
    vertex actor of each class it covers, so those rows locate edge endpoints
    and are never written. ``find`` names, per class, the secondary identity its
    rows find their existing vertex by. ``vertex_types`` bounds the classes it
    produces: a value resolving to any other class is skipped.
    """

    def __init__(self, config: VertexRouterActorConfig):
        self.config = config
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
        if self.config.type_map_only:
            items["type_map_only"] = True
        if self.config.vertex_types is not None:
            items["vertex_types"] = self.config.vertex_types
        if self.config.lookup_only:
            items["lookup_only"] = self.config.lookup_only
        if self.config.find:
            items["find"] = self.config.find
        items["routed_types"] = sorted(self._vertex_actors.keys())
        return items

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.vertex_config = init_ctx.vertex_config
        self._init_ctx = init_ctx
        self._vertex_actors.clear()
        for field, named in (
            ("lookup_only", self.config.lookup_only),
            ("vertex_types", self.config.vertex_types),
            ("find", list(self.config.find or {})),
        ):
            if isinstance(named, list):
                unknown = sorted(set(named) - self.vertex_config.vertex_set)
                if unknown:
                    raise ValueError(
                        f"vertex_router on {self.type_field!r}: {field} names "
                        f"{unknown}, which the schema does not declare"
                    )
        for vertex_type, selector in sorted((self.config.find or {}).items()):
            find_fields(
                self.vertex_config,
                vertex_type,
                selector,
                step=f"vertex_router on {self.type_field!r}",
            )
        if init_ctx.strict_references:
            # Checked for the classes the router names; one an unbounded router
            # routes to by pass-through is known only per record, and gets the
            # declared part of the map (see `_get_or_create_wrapper`).
            for vertex_type, from_doc in self.vertex_from_map.items():
                refuse_undeclared_mapping(self.vertex_config, vertex_type, from_doc)
            if self.from_doc:
                for vertex_type in sorted(self._named_classes()):
                    if vertex_type not in self.vertex_from_map:
                        refuse_undeclared_mapping(
                            self.vertex_config, vertex_type, self.from_doc
                        )

    def _named_classes(self) -> set[str]:
        """The classes the router can produce that its config names.

        The table's classes, bounded by ``vertex_types``; an open bounded router
        reaches every listed class by pass-through as well.
        """
        table = set(self.type_map.values())
        bound = self.config.vertex_types
        if bound is None:
            return table
        if self.config.type_map_only:
            return table & set(bound)
        return set(bound)

    def _get_or_create_wrapper(self, vertex_type: str) -> ActorWrapper | None:
        from .wrapper import ActorWrapper

        if vertex_type not in self.vertex_config.vertex_set:
            return None
        # Fast path without the lock: dict reads are atomic, and a wrapper is only
        # published after finish_init completes, so it is never seen half-built.
        wrapper = self._vertex_actors.get(vertex_type)
        if wrapper is not None:
            return wrapper
        if self._init_ctx is None:
            raise RuntimeError(
                "VertexRouterActor._get_or_create_wrapper called before finish_init"
            )

        with _VERTEX_ACTOR_CREATE_LOCK:
            wrapper = self._vertex_actors.get(vertex_type)
            if wrapper is not None:
                return wrapper
            if vertex_type in self.vertex_from_map:
                per_type_from = self.vertex_from_map[vertex_type]
            else:
                per_type_from = self.from_doc
            if self._init_ctx.strict_references and per_type_from:
                per_type_from = self._declared_part(vertex_type, per_type_from)
            config = VertexActorConfig.model_validate(
                {
                    "type": "vertex",
                    "vertex": vertex_type,
                    "from": per_type_from,
                    "keep_fields": list(self.keep_fields) if self.keep_fields else None,
                    "extraction_scope": self.extraction_scope,
                    "lookup_only": self.config.looks_up(vertex_type),
                    "find": self.config.find_for(vertex_type),
                }
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

    def _declared_part(
        self, vertex_type: str, from_doc: dict[str, str]
    ) -> dict[str, str]:
        """*from_doc* without the targets *vertex_type* does not declare, said once."""
        unknown = undeclared_mapping_targets(self.vertex_config, vertex_type, from_doc)
        if not unknown:
            return from_doc
        logger.warning(
            "vertex_router on %r: %s routed by pass-through does not declare %s; "
            "those `from` targets are not mapped",
            self.type_field,
            vertex_type,
            unknown,
        )
        return {k: v for k, v in from_doc.items() if k not in unknown}

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
        if self.config.type_map_only and raw_vtype not in self.type_map:
            logger.debug(
                "VertexRouterActor: value %r of '%s' is not in the closed "
                "type_map, skipping",
                raw_vtype,
                self.type_field,
            )
            return ctx
        vtype = self.type_map.get(raw_vtype, raw_vtype)
        if not self.config.admits(vtype):
            logger.debug(
                "VertexRouterActor: vertex type %r (from field '%s') is outside "
                "vertex_types, skipping",
                vtype,
                self.type_field,
            )
            return ctx

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
