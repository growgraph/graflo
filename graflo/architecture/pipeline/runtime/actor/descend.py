"""Descend actor for processing hierarchical data structures."""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Type

from .base import Actor, ActorInitContext
from .config import DescendActorConfig, VertexActorConfig
from .edge import EdgeActor
from .edge_router import EdgeRouterActor
from .transform import TransformActor
from .vertex import VertexActor
from .vertex_router import VertexRouterActor

if TYPE_CHECKING:
    from .wrapper import ActorWrapper

logger = logging.getLogger(__name__)


class DescendActor(Actor):
    """Actor for processing hierarchical data structures."""

    def __init__(
        self,
        key: str | None,
        any_key: bool = False,
        *,
        _descendants: list[ActorWrapper] | None = None,
    ):
        self.key = key
        self.any_key = any_key
        self._descendants: list[ActorWrapper] = (
            list(_descendants) if _descendants else []
        )
        self._descendants_sorted = True
        self._descendants.sort(key=lambda x: _NodeTypePriority[type(x.actor)])

    def fetch_important_items(self) -> dict[str, Any]:
        items = self._fetch_items_from_dict(("key",))
        if self.any_key:
            items["any_key"] = True
        return items

    def add_descendant(self, d: ActorWrapper) -> None:
        self._descendants.append(d)
        self._descendants_sorted = False

    def count(self) -> int:
        return sum(d.count() for d in self.descendants)

    @property
    def descendants(self) -> list[ActorWrapper]:
        if not self._descendants_sorted:
            self._descendants.sort(key=lambda x: _NodeTypePriority[type(x.actor)])
            self._descendants_sorted = True
        return self._descendants

    @classmethod
    def from_config(cls, config: DescendActorConfig) -> DescendActor:
        from .wrapper import ActorWrapper

        wrappers = [ActorWrapper.from_config(c) for c in config.pipeline]
        return cls(key=config.key, any_key=config.any_key, _descendants=wrappers)

    def _infer_vertex_descendants_from_transforms(
        self, init_ctx: ActorInitContext
    ) -> None:
        from .transform import TransformActor
        from .vertex import VertexActor

        if any(isinstance(an.actor, VertexActor) for an in self.descendants):
            return

        transform_output_fields: set[str] = set()
        for an in self.descendants:
            if isinstance(an.actor, TransformActor):
                transform_output_fields.update(str(k) for k in an.actor.t.map.keys())

        if not transform_output_fields:
            return

        inferred_vertices: list[str] = []
        for vertex_name in sorted(init_ctx.vertex_config.vertex_set):
            identity_fields = {
                f for f in init_ctx.vertex_config.identity_fields(vertex_name)
            }
            if identity_fields and identity_fields.issubset(transform_output_fields):
                inferred_vertices.append(vertex_name)

        if not inferred_vertices:
            return

        existing_targets: set[str] = set()
        for an in self.descendants:
            existing_targets.update(
                str(v) for v in an.actor.references_vertices() if v is not None
            )
        for vertex_name in inferred_vertices:
            if vertex_name in existing_targets:
                continue
            from .wrapper import ActorWrapper

            self.add_descendant(
                ActorWrapper.from_config(VertexActorConfig(vertex=vertex_name))
            )
            logger.debug(
                "DescendActor: inferred implicit VertexActor(%s) from untargeted transform fields %s",
                vertex_name,
                sorted(transform_output_fields),
            )

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        for an in self.descendants:
            an.init_transforms(init_ctx)

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.vertex_config = init_ctx.vertex_config
        self._infer_vertex_descendants_from_transforms(init_ctx)
        for an in self.descendants:
            an.finish_init(init_ctx)

    def _expand_document(self, doc: dict | list) -> list[tuple[str | None, Any]]:
        if self.key is not None:
            if isinstance(doc, dict) and self.key in doc:
                items = doc[self.key]
                aux = items if isinstance(items, list) else [items]
                return [(self.key, item) for item in aux]
            return []
        elif self.any_key:
            if isinstance(doc, dict):
                result = []
                for key, items in doc.items():
                    aux = items if isinstance(items, list) else [items]
                    result.extend([(key, item) for item in aux])
                return result
            return []
        else:
            if isinstance(doc, list):
                return [(None, item) for item in doc]
            return [(None, doc)]

    def __call__(self, ctx: Any, lindex: Any, *nargs: Any, **kwargs: Any) -> Any:
        doc: Any = kwargs.pop("doc")
        if doc is None:
            raise ValueError(f"{type(self).__name__}: doc should be provided")
        if not doc:
            return ctx

        doc_expanded = self._expand_document(doc)
        if not doc_expanded:
            return ctx

        logger.debug("Expanding %s items", len(doc_expanded))

        for idoc, (key, sub_doc) in enumerate(doc_expanded):
            logger.debug("Processing item %s/%s", idoc + 1, len(doc_expanded))
            if isinstance(sub_doc, dict):
                nargs_tuple: tuple[Any, ...] = ()
                child_kwargs = {**kwargs, "doc": sub_doc}
            else:
                nargs_tuple = (sub_doc,)
                child_kwargs = kwargs

            extra_step = (idoc,) if key is None else (key, idoc)
            for j, anw in enumerate(self.descendants):
                logger.debug(
                    "%s: %s/%s",
                    type(anw.actor).__name__,
                    j + 1,
                    len(self.descendants),
                )
                ctx = anw(ctx, lindex.extend(extra_step), *nargs_tuple, **child_kwargs)
        return ctx

    def fetch_actors(self, level: int, edges: list) -> tuple[int, type, str, list]:
        label_current = str(self)
        cname_current = type(self)
        hash_current = hash((level, cname_current, label_current))
        logger.info("%s, %s", hash_current, (level, cname_current, label_current))
        props_current = {"label": label_current, "class": cname_current, "level": level}
        for d in self.descendants:
            level_a, cname, label_a, edges_a = d.fetch_actors(level + 1, edges)
            hash_a = hash((level_a, cname, label_a))
            props_a = {"label": label_a, "class": cname, "level": level_a}
            edges = [(hash_current, hash_a, props_current, props_a)] + edges_a
        return level, type(self), str(self), edges


_NodeTypePriority: MappingProxyType[Type[Actor], int] = MappingProxyType(
    {
        DescendActor: 10,
        TransformActor: 20,
        VertexRouterActor: 30,
        EdgeRouterActor: 35,
        VertexActor: 50,
        EdgeActor: 90,
    }
)
