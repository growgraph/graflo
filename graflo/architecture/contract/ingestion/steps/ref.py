"""Addressing edge steps inside a pipeline.

A pipeline is a list of actor configs, nested through ``descend`` steps, and
an edge step may carry several intents in ``links``. Anything that reasons
about *which step writes which edge* -- an audit, an evolution op that flips a
step option -- needs one way to walk those and one way to name a position.
This module is both, and nothing else.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.steps.models import (
    ActorConfig,
    DescendActorConfig,
    EdgeActorConfig,
    EdgeLinkConfig,
)


class EdgeStepRef(ConfigBaseModel):
    """The position of one edge intent in a pipeline.

    ``at`` descends through nested pipelines by step index, ``step`` is the
    index within the pipeline reached, and ``link`` selects one entry of that
    step's ``links``. A ref is positional: it names a place, so it stays valid
    only while the steps before it are not reordered.
    """

    at: list[int] = PydanticField(
        default_factory=list,
        description="Step indexes of the enclosing descend steps, outermost first.",
    )
    step: int = PydanticField(..., ge=0, description="Index within its pipeline.")
    link: int | None = PydanticField(
        default=None, ge=0, description="Index into the step's `links`, if it has them."
    )

    def __str__(self) -> str:
        path = "/".join(str(index) for index in [*self.at, self.step])
        return path if self.link is None else f"{path}#link{self.link}"

    @property
    def sort_key(self) -> tuple[tuple[int, ...], int]:
        return (*self.at, self.step), -1 if self.link is None else self.link


@dataclass(frozen=True)
class EdgeStepView:
    """One edge intent, with the fields that decide what it writes."""

    ref: EdgeStepRef
    payload: EdgeActorConfig | EdgeLinkConfig
    """The step, or the link entry for a multi-link step."""

    @property
    def source(self) -> str | None:
        return self.payload.source

    @property
    def target(self) -> str | None:
        return self.payload.target

    @property
    def relation(self) -> str | None:
        return self.payload.relation

    @property
    def relation_is_data_driven(self) -> bool:
        """Whether the relation is resolved per document rather than fixed here."""
        return bool(self.payload.relation_field) or (
            isinstance(self.payload, EdgeActorConfig) and self.payload.relation_from_key
        )

    @property
    def emit_inverse(self) -> bool:
        return self.payload.emit_inverse

    @property
    def static_edge_id(self) -> tuple[str, str, str] | None:
        """``(source, target, relation)`` when all three are fixed by the step."""
        if self.relation_is_data_driven:
            return None
        source, target, relation = self.source, self.target, self.relation
        if source is None or target is None or relation is None:
            return None
        return source, target, relation

    def relations_written(self) -> set[str] | None:
        """Relation names the step can write; see :func:`relations_written`."""
        return relations_written(self.payload)


def relations_written(step: EdgeActorConfig | EdgeLinkConfig) -> set[str] | None:
    """Relation names an edge intent can write, or None when that is open-ended.

    A fixed relation is one name. A mapped ``relation_field`` that writes only
    what it maps (``relation_map_only``) is the map's values, plus the fixed
    fallback. Anything else -- an unmapped field, a relation taken from a
    document key -- can name any relation the data carries. A link has no map
    and no key-derived relation of its own.
    """
    relation = step.relation
    from_key = isinstance(step, EdgeActorConfig) and step.relation_from_key
    if not step.relation_field and not from_key:
        return {relation} if relation is not None else set()
    if (
        isinstance(step, EdgeActorConfig)
        and step.relation_map_only
        and step.relation_map is not None
        and not from_key
    ):
        fallback = {relation} if relation is not None else set()
        return set(step.relation_map.values()) | fallback
    return None


def iter_edge_steps(
    pipeline: Sequence[ActorConfig], *, at: tuple[int, ...] = ()
) -> Iterator[EdgeStepView]:
    """Every edge intent of *pipeline*, nested pipelines and links included, in order."""
    for index, step in enumerate(pipeline):
        if isinstance(step, DescendActorConfig):
            yield from iter_edge_steps(step.pipeline, at=(*at, index))
        elif isinstance(step, EdgeActorConfig):
            if step.links is not None:
                for position, link in enumerate(step.links):
                    yield EdgeStepView(
                        EdgeStepRef(at=list(at), step=index, link=position), link
                    )
            else:
                yield EdgeStepView(EdgeStepRef(at=list(at), step=index), step)


def find_edge_step(
    pipeline: Sequence[ActorConfig], ref: EdgeStepRef
) -> EdgeStepView | None:
    """The edge intent at *ref*, or None when *ref* names no edge intent."""
    return next(
        (view for view in iter_edge_steps(pipeline) if view.ref == ref),
        None,
    )


def with_emit_inverse(
    pipeline: Sequence[ActorConfig], ref: EdgeStepRef, enabled: bool
) -> list[ActorConfig]:
    """*pipeline* with ``emit_inverse`` set or cleared on the intent at *ref*.

    Only that flag changes; every other step is the same object.

    Raises:
        ValueError: when *ref* names no edge intent.
    """
    if find_edge_step(pipeline, ref) is None:
        raise ValueError(f"no edge step at {ref}")

    def rewrite(steps: Sequence[ActorConfig], path: list[int]) -> list[ActorConfig]:
        out = list(steps)
        if path:
            head, *rest = path
            descend = out[head]
            assert isinstance(descend, DescendActorConfig)
            out[head] = descend.model_copy(
                update={"pipeline": rewrite(descend.pipeline, rest)}
            )
            return out
        step = out[ref.step]
        assert isinstance(step, EdgeActorConfig)
        if ref.link is not None and step.links is not None:
            links = list(step.links)
            links[ref.link] = links[ref.link].model_copy(
                update={"emit_inverse": enabled}
            )
            out[ref.step] = step.model_copy(update={"links": links})
        else:
            out[ref.step] = step.model_copy(update={"emit_inverse": enabled})
        return out

    return rewrite(pipeline, list(ref.at))


__all__ = [
    "EdgeStepRef",
    "EdgeStepView",
    "find_edge_step",
    "iter_edge_steps",
    "relations_written",
    "with_emit_inverse",
]
