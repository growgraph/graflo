"""Transform actor for applying transformations to data."""

from __future__ import annotations

import logging
from typing import Any

from graflo.architecture.actor.base import Actor, ActorInitContext
from graflo.architecture.actor.config import TransformActorConfig
from graflo.architecture.onto import (
    ExtractionContext,
    LocationIndex,
    TransformPayload,
)
from graflo.architecture.transform import ProtoTransform, Transform

logger = logging.getLogger(__name__)


class TransformActor(Actor):
    """Actor for applying transformations to data."""

    def __init__(self, config: TransformActorConfig):
        self._kwargs = config.model_dump(by_alias=True)
        self.transforms: dict[str, ProtoTransform] = {}
        self.name = config.name
        self.params = config.params
        self.t: Transform = Transform(
            map=config.map or {},
            name=config.name,
            params=config.params,
            module=config.module,
            foo=config.foo,
            input=tuple(config.input) if config.input else (),
            output=tuple(config.output) if config.output else (),
            dress=config.dress,
        )

    def fetch_important_items(self) -> dict[str, Any]:
        items = self._fetch_items_from_dict(("name",))
        items.update({"t.input": self.t.input, "t.output": self.t.output})
        return items

    @classmethod
    def from_config(cls, config: TransformActorConfig) -> TransformActor:
        return cls(config)

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        self.transforms = init_ctx.transforms
        try:
            pt = ProtoTransform(
                **{
                    k: self._kwargs[k]
                    for k in ProtoTransform.get_fields_members()
                    if k in self._kwargs
                }
            )
            if pt.name is not None and pt._foo is not None:
                if pt.name not in self.transforms:
                    self.transforms[pt.name] = pt
                elif pt.params:
                    self.transforms[pt.name] = pt
        except (TypeError, ValueError, AttributeError) as e:
            logger.debug("Failed to initialize ProtoTransform: %s", e)

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.transforms = init_ctx.transforms
        if self.name is not None:
            pt = self.transforms.get(self.name, None)
            if pt is not None:
                next_params = self.t.params
                next_input = self.t.input
                next_output = self.t.output
                if pt.params and not self.t.params:
                    next_params = pt.params
                    if (
                        pt.input
                        and not self.t.input
                        and pt.output
                        and not self.t.output
                    ):
                        next_input = pt.input
                        next_output = pt.output
                self.t = Transform(
                    fields=self.t.fields,
                    map=self.t.map,
                    dress=self.t.dress,
                    name=self.t.name,
                    module=pt.module,
                    foo=pt.foo,
                    params=next_params,
                    input=next_input,
                    output=next_output,
                )

    def _extract_doc(self, nargs: tuple[Any, ...], **kwargs: Any) -> dict[str, Any]:
        if kwargs:
            doc: dict[str, Any] | None = kwargs.get("doc")
        elif nargs:
            doc = nargs[0]
        else:
            raise ValueError(f"{type(self).__name__}: doc should be provided")
        if doc is None:
            raise ValueError(f"{type(self).__name__}: doc should be provided")
        return doc

    def _format_transform_result(self, result: Any) -> TransformPayload:
        return TransformPayload.from_result(result)

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        logger.debug("transforms : %s %s", id(self.transforms), len(self.transforms))
        doc = self._extract_doc(nargs, **kwargs)
        transform_result = self.t(doc)
        _update_doc = self._format_transform_result(transform_result)
        ctx.buffer_transforms[lindex].append(_update_doc)
        ctx.record_transform_observation(location=lindex, payload=_update_doc)
        return ctx

    def references_vertices(self) -> set[str]:
        return set()
