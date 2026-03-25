"""Transform actor for applying transformations to data."""

from __future__ import annotations

import logging
from typing import Any

from .base import Actor, ActorInitContext
from .config import TransformActorConfig
from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    TransformPayload,
)
from graflo.architecture.contract.declarations.transform import (
    KeySelectionConfig,
    ProtoTransform,
    Transform,
)

logger = logging.getLogger(__name__)


class TransformActor(Actor):
    """Actor for applying transformations to data."""

    def __init__(self, config: TransformActorConfig):
        self.transforms: dict[str, ProtoTransform] = {}
        self.call_use: str | None = None
        self._call_config = None

        if config.rename is not None:
            self.t = Transform(rename=config.rename)
            return

        if config.call is None:
            raise ValueError(
                "TransformActorConfig requires call when rename is absent."
            )

        call = config.call
        self._call_config = call
        self.call_use = call.use
        inline_target = (
            call.target
            if call.target is not None
            else "values"
            if call.use is None
            else None
        )
        transform_kwargs: dict[str, Any] = {
            "name": call.use,
            "params": call.params,
            "module": call.module,
            "foo": call.foo,
            "input": tuple(call.input) if call.input else (),
            "output": tuple(call.output) if call.output else (),
            "input_groups": (
                tuple(tuple(group) for group in call.input_groups)
                if call.input_groups
                else ()
            ),
            "output_groups": (
                tuple(tuple(group) for group in call.output_groups)
                if call.output_groups
                else ()
            ),
            "dress": call.dress,
            "strategy": call.strategy or "single",
        }
        if inline_target is not None:
            transform_kwargs["target"] = inline_target
        if call.use is None:
            if call.keys is not None:
                transform_kwargs["keys"] = KeySelectionConfig.model_validate(
                    call.keys.model_dump()
                )
        # When call.use references ingestion_model.transforms, defer strict
        # transform validation until finish_init can hydrate module/foo.
        if call.use is not None and call.module is None and call.foo is None:
            self.t = Transform(name=call.use)
            return
        self.t = Transform(**transform_kwargs)

    def fetch_important_items(self) -> dict[str, Any]:
        items = self._fetch_items_from_dict(("transform",))
        items.update({"t.input": self.t.input, "t.output": self.t.output})
        return items

    @classmethod
    def from_config(cls, config: TransformActorConfig) -> TransformActor:
        return cls(config)

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        self.transforms = init_ctx.transforms

    def _merge_call_with_proto(self, call: Any, pt: ProtoTransform) -> dict[str, Any]:
        next_params = call.params if call.params else pt.params
        next_dress = call.dress if call.dress is not None else pt.dress
        next_target = call.target if call.target is not None else pt.target

        if next_target == "keys":
            if call.input or call.output or call.input_groups or call.output_groups:
                raise ValueError(
                    "call.input, call.output, call.input_groups, and call.output_groups "
                    "cannot be used when the effective transform target is keys "
                    "(from call.target or the named ingestion_model.transforms entry)."
                )
            if call.dress is not None:
                raise ValueError("call.dress is not supported when target='keys'.")
            if call.strategy is not None and call.strategy != "single":
                raise ValueError(
                    "call.strategy is not allowed when target='keys'; "
                    "key mode uses implicit per-key execution."
                )
            next_input: tuple[str, ...] = ()
            next_output: tuple[str, ...] = ()
            next_input_groups: tuple[tuple[str, ...], ...] = ()
            next_output_groups: tuple[tuple[str, ...], ...] = ()
        else:
            next_input_groups = (
                tuple(tuple(group) for group in call.input_groups)
                if call.input_groups
                else pt.input_groups
            )
            next_output_groups = (
                tuple(tuple(group) for group in call.output_groups)
                if call.output_groups
                else pt.output_groups
            )
            if next_input_groups:
                next_input = ()
                # Explicit grouped override should not inherit potentially
                # conflicting proto output/output_groups for a different shape.
                if call.input_groups:
                    next_output_groups = (
                        tuple(tuple(group) for group in call.output_groups)
                        if call.output_groups
                        else ()
                    )
                    next_output = tuple(call.output) if call.output else ()
                elif next_dress is not None:
                    next_output = (next_dress.key, next_dress.value)
                else:
                    next_output = tuple(call.output) if call.output else pt.output
            else:
                next_input = tuple(call.input) if call.input else pt.input
                if next_dress is not None:
                    next_output = (next_dress.key, next_dress.value)
                else:
                    next_output = tuple(call.output) if call.output else pt.output

        transform_kwargs: dict[str, Any] = {
            "dress": next_dress,
            "name": call.use,
            "module": pt.module,
            "foo": pt.foo,
            "params": next_params,
            "input": next_input,
            "output": next_output,
            "input_groups": next_input_groups,
            "output_groups": next_output_groups,
            "strategy": call.strategy or "single",
            "target": next_target,
        }
        if call.keys is not None:
            transform_kwargs["keys"] = KeySelectionConfig.model_validate(
                call.keys.model_dump()
            )
        else:
            transform_kwargs["keys"] = pt.keys.model_copy(deep=True)
        return transform_kwargs

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.transforms = init_ctx.transforms
        if self.call_use is None or self.t._foo is not None:
            return
        if self._call_config is None:
            return
        pt = self.transforms.get(self.call_use, None)
        if pt is None:
            if init_ctx.strict_references:
                raise ValueError(
                    f"Transform '{self.call_use}' referenced by transform.call.use "
                    "was not found in ingestion_model.transforms."
                )
            return
        call = self._call_config
        transform_kwargs = self._merge_call_with_proto(call, pt)
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
