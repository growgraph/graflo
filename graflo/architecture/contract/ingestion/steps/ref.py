"""Addressing edge steps inside a pipeline.

A pipeline is a list of authored step mappings, nested through ``descend``
steps, and an edge step may carry several intents in ``links``. Anything that
reasons about *which step writes which edge* -- an audit, an evolution op that
flips a step option -- needs one way to walk those and one way to name a
position. This module is both, and nothing else: it reads the authored shape
and never validates a step beyond normalizing its spelling.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)

#: Keys under which a ``descend`` step holds its nested pipeline; ``apply`` is
#: the legacy spelling.
_NESTED_KEYS = ("pipeline", "apply")


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
    """One edge intent as authored, with the fields that decide what it writes."""

    ref: EdgeStepRef
    payload: dict[str, Any]
    """The normalized step, or the link entry for a multi-link step."""

    @property
    def source(self) -> str | None:
        value = self.payload.get("from", self.payload.get("source"))
        return value if isinstance(value, str) else None

    @property
    def target(self) -> str | None:
        value = self.payload.get("to", self.payload.get("target"))
        return value if isinstance(value, str) else None

    @property
    def relation(self) -> str | None:
        value = self.payload.get("relation")
        return value if isinstance(value, str) else None

    @property
    def relation_is_data_driven(self) -> bool:
        """Whether the relation is resolved per document rather than fixed here."""
        return bool(
            self.payload.get("relation_field") or self.payload.get("relation_from_key")
        )

    @property
    def emit_inverse(self) -> bool:
        return bool(self.payload.get("emit_inverse"))

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
        """Relation names the step can write, or None when that is open-ended.

        A fixed relation is one name. A mapped ``relation_field`` that writes
        only what it maps (``relation_map_only``) is the map's values, plus the
        fixed fallback. Anything else -- an unmapped field, a relation taken
        from a document key -- can name any relation the data carries.
        """
        if not self.relation_is_data_driven:
            return {self.relation} if self.relation is not None else set()
        relation_map = self.payload.get("relation_map")
        if (
            self.payload.get("relation_map_only")
            and isinstance(relation_map, dict)
            and not self.payload.get("relation_from_key")
        ):
            fallback = {self.relation} if self.relation is not None else set()
            return {str(name) for name in relation_map.values()} | fallback
        return None


def _nested(step: dict[str, Any]) -> tuple[str, list[Any]] | None:
    for key in _NESTED_KEYS:
        nested = step.get(key)
        if isinstance(nested, list):
            return key, nested
    return None


def _edge_body(step: dict[str, Any]) -> dict[str, Any]:
    """The mapping that holds an edge step's fields: the ``edge:`` wrapper, or the step."""
    wrapped = step.get("edge")
    return wrapped if isinstance(wrapped, dict) else step


def iter_edge_steps(
    pipeline: list[Any], *, at: tuple[int, ...] = ()
) -> Iterator[EdgeStepView]:
    """Every edge intent of *pipeline*, nested pipelines and links included, in order."""
    for index, raw in enumerate(pipeline):
        if not isinstance(raw, dict):
            continue
        step = normalize_actor_step(dict(raw))
        kind = step.get("type")
        if kind == "descend":
            nested = _nested(step)
            if nested is not None:
                yield from iter_edge_steps(nested[1], at=(*at, index))
        elif kind == "edge":
            links = step.get("links")
            if isinstance(links, list):
                for position, link in enumerate(links):
                    if isinstance(link, dict):
                        yield EdgeStepView(
                            EdgeStepRef(at=list(at), step=index, link=position),
                            dict(link),
                        )
            else:
                yield EdgeStepView(EdgeStepRef(at=list(at), step=index), step)


def find_edge_step(pipeline: list[Any], ref: EdgeStepRef) -> EdgeStepView | None:
    """The edge intent at *ref*, or None when *ref* names no edge intent."""
    return next(
        (view for view in iter_edge_steps(pipeline) if view.ref == ref),
        None,
    )


def with_emit_inverse(
    pipeline: list[Any], ref: EdgeStepRef, enabled: bool
) -> list[Any]:
    """*pipeline* with ``emit_inverse`` set or cleared on the intent at *ref*.

    The step keeps the spelling it was authored in (``edge:`` wrapper or flat
    fields, ``pipeline`` or ``apply``); only the one key changes, and clearing
    removes it rather than writing ``false``, which is the default.

    Raises:
        ValueError: when *ref* names no edge intent.
    """
    if find_edge_step(pipeline, ref) is None:
        raise ValueError(f"no edge step at {ref}")

    def rewrite(steps: list[Any], path: list[int]) -> list[Any]:
        out = list(steps)
        if path:
            head, *rest = path
            step = dict(out[head])
            # A descend step is authored flat or under a ``descend:`` wrapper.
            wrapped = isinstance(step.get("descend"), dict)
            holder = dict(step["descend"]) if wrapped else step
            nested = _nested(holder)
            assert nested is not None
            key, body = nested
            holder[key] = rewrite(body, rest)
            if wrapped:
                step["descend"] = holder
            out[head] = step
            return out
        step = dict(out[ref.step])
        holder = dict(_edge_body(step))
        if ref.link is not None:
            links = list(holder["links"])
            links[ref.link] = _flagged(dict(links[ref.link]), enabled)
            holder["links"] = links
        else:
            holder = _flagged(holder, enabled)
        if isinstance(step.get("edge"), dict):
            step["edge"] = holder
        else:
            step = holder
        out[ref.step] = step
        return out

    return rewrite(pipeline, list(ref.at))


def _flagged(body: dict[str, Any], enabled: bool) -> dict[str, Any]:
    if enabled:
        body["emit_inverse"] = True
    else:
        body.pop("emit_inverse", None)
    return body


__all__ = [
    "EdgeStepRef",
    "EdgeStepView",
    "find_edge_step",
    "iter_edge_steps",
    "with_emit_inverse",
]
