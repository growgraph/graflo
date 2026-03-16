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
        self.transforms: dict[str, ProtoTransform] = {}
        self.call_use: str | None = None

        if config.rename is not None:
            self.t = Transform(map=config.rename)
            return

        if config.call is None:
            raise ValueError(
                "TransformActorConfig requires call when rename is absent."
            )

        call = config.call
        self.call_use = call.use
        self.t = Transform(
            name=call.use,
            params=call.params,
            module=call.module,
            foo=call.foo,
            input=tuple(call.input) if call.input else (),
            output=tuple(call.output) if call.output else (),
            dress=call.dress,
            strategy=call.strategy or "single",
        )

    def fetch_important_items(self) -> dict[str, Any]:
        items = self._fetch_items_from_dict(("transform",))
        items.update({"t.input": self.t.input, "t.output": self.t.output})
        return items

    @classmethod
    def from_config(cls, config: TransformActorConfig) -> TransformActor:
        return cls(config)

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        self.transforms = init_ctx.transforms

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.transforms = init_ctx.transforms
        if self.call_use is None or self.t._foo is not None:
            return
        pt = self.transforms.get(self.call_use, None)
        if pt is None:
            return
        next_params = self.t.params if self.t.params else pt.params
        next_input = self.t.input if self.t.input else pt.input
        next_output = self.t.output if self.t.output else pt.output
        transform_kwargs: dict[str, Any] = {
            "dress": self.t.dress,
            "name": self.t.name,
            "module": pt.module,
            "foo": pt.foo,
            "params": next_params,
            "input": next_input,
            "output": next_output,
            "strategy": self.t.strategy,
        }
        self.t = Transform(**transform_kwargs)

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
