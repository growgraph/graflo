"""Vertex actor for processing vertex data."""

from __future__ import annotations

from typing import Any

from .base import Actor, ActorConstants, ActorInitContext
from .config import VertexActorConfig
from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    TransformPayload,
    VertexRep,
)
from graflo.architecture.schema.vertex import VertexConfig
from graflo.onto import ExpressionFlavor
from graflo.util.merge import merge_doc_basis


class VertexActor(Actor):
    """Actor for processing vertex data."""

    def __init__(self, config: VertexActorConfig):
        self.name = config.vertex
        self.from_doc: dict[str, str] | None = config.from_doc
        self.keep_fields: tuple[str, ...] | None = (
            tuple(config.keep_fields) if config.keep_fields else None
        )
        self.vertex_config: VertexConfig

    @classmethod
    def from_config(cls, config: VertexActorConfig) -> VertexActor:
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        return self._fetch_items_from_dict(("name", "from_doc", "keep_fields"))

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.vertex_config = init_ctx.vertex_config

    def _filter_and_aggregate_vertex_docs(
        self, docs: list[dict[str, Any]], doc: dict[str, Any]
    ) -> list[dict[str, Any]]:
        filters = self.vertex_config.filters(self.name)
        return [
            _doc
            for _doc in docs
            if all(cfilter(kind=ExpressionFlavor.PYTHON, **_doc) for cfilter in filters)
        ]

    def _extract_vertex_doc_from_transformed_item(
        self,
        item: Any,
        vertex_keys: tuple[str, ...],
        index_keys: tuple[str, ...],
    ) -> dict[str, Any]:
        if isinstance(item, TransformPayload):
            doc: dict[str, Any] = {}
            consumed_named: set[str] = set()
            for k, v in item.named.items():
                if k in vertex_keys and v is not None:
                    doc[k] = v
                    consumed_named.add(k)
            for j, value in enumerate(item.positional):
                if j >= len(index_keys):
                    break
                doc[index_keys[j]] = value
            for key in consumed_named:
                item.named.pop(key, None)
            if item.positional:
                item.positional = ()
            return doc

        if isinstance(item, dict):
            doc = {}
            value_keys = sorted(
                (
                    k
                    for k in item
                    if k.startswith(ActorConstants.DRESSING_TRANSFORMED_VALUE_KEY)
                ),
                key=lambda x: int(x.rsplit("#", 1)[-1]),
            )
            for j, vkey in enumerate(value_keys):
                if j >= len(index_keys):
                    break
                doc[index_keys[j]] = item.pop(vkey)
            for vkey in vertex_keys:
                if vkey not in doc and vkey in item and item[vkey] is not None:
                    doc[vkey] = item.pop(vkey)
            return doc

        return {}

    def _process_transformed_items(
        self,
        ctx: ExtractionContext,
        lindex: LocationIndex,
        doc: dict[str, Any],
        vertex_keys: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        index_keys = tuple(self.vertex_config.identity_fields(self.name))
        payloads = ctx.buffer_transforms[lindex]
        extracted_docs = [
            self._extract_vertex_doc_from_transformed_item(
                item, vertex_keys, index_keys
            )
            for item in payloads
        ]
        ctx.buffer_transforms[lindex] = [
            item
            for item in payloads
            if not (
                isinstance(item, TransformPayload)
                and not item.named
                and not item.positional
            )
            and not (isinstance(item, dict) and not item)
        ]
        return self._filter_and_aggregate_vertex_docs(extracted_docs, doc)

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        doc: dict[str, Any] = kwargs.get("doc", {})
        vertex_keys_list = self.vertex_config.fields_names(self.name)
        vertex_keys: tuple[str, ...] = tuple(vertex_keys_list)

        agg = []
        if self.from_doc:
            projected = {v_f: doc.get(d_f) for v_f, d_f in self.from_doc.items()}
            if any(v is not None for v in projected.values()):
                agg.append(projected)

        agg.extend(self._process_transformed_items(ctx, lindex, doc, vertex_keys))

        remaining_keys = set(vertex_keys) - set().union(*[d.keys() for d in agg])
        passthrough_doc = {k: doc.pop(k) for k in remaining_keys if k in doc}
        if passthrough_doc:
            agg.append(passthrough_doc)

        merged = merge_doc_basis(
            agg, index_keys=tuple(self.vertex_config.identity_fields(self.name))
        )

        obs_ctx = {q: w for q, w in doc.items() if not isinstance(w, (dict, list))}
        for m in merged:
            vertex_rep = VertexRep(vertex=m, ctx=obs_ctx)
            ctx.acc_vertex[self.name][lindex].append(vertex_rep)
            ctx.record_vertex_observation(
                vertex_name=self.name,
                location=lindex,
                vertex=vertex_rep.vertex,
                ctx=vertex_rep.ctx,
            )
        return ctx

    def references_vertices(self) -> set[str]:
        return {self.name}
