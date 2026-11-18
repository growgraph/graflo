"""Which records a resource's routers keep, read off its typed pipeline.

A resource whose top level only transforms, routes and links router roles
produces something from a record only when a ``vertex_router`` routes the
record's ``type_field`` value. Those values are a condition a source can apply
before it reads (a table connector pushes them into its query), and connector
pruning uses the same rule to decide which requests still reach a class.

The condition is stated on the source's own field: a rename or a key-renaming
call of the level only relabels the value a router reads, so the field is
traced back through them (:func:`routed_field_source`).
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict

from graflo.architecture.contract.ingestion.steps.models import (
    ActorConfig,
    EdgeActorConfig,
    TransformActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.contract.ingestion.transform import ProtoTransform


class RoutedRecordFilter(BaseModel):
    """The records a pipeline produces anything from, or why that cannot be said.

    ``fields`` maps each source field a router's ``type_field`` reads to the
    values the routers send somewhere; a record counts when any field holds one
    of its values. An empty mapping means no record produces anything.
    ``fields`` is ``None`` when a record may produce something without a
    router, or a routed value is not a source field's; ``blocked_by`` then
    names the step that causes it.
    """

    model_config = ConfigDict(frozen=True)

    fields: dict[str, frozenset[Any]] | None
    blocked_by: str | None = None

    @property
    def applies(self) -> bool:
        """Whether the condition holds for every record the pipeline uses."""
        return self.fields is not None


def describe_step(step: ActorConfig, index: int) -> str:
    """A short label for step *index* of a level, for log lines."""
    if isinstance(step, VertexRouterActorConfig):
        return f"step {index} (vertex_router on {step.type_field!r})"
    if isinstance(step, TransformActorConfig):
        if step.rename is not None:
            return f"step {index} (transform rename)"
        call = step.call
        name = (
            call.use
            if call is not None and call.use is not None
            else f"{call.module}.{call.foo}"
            if call is not None and call.foo is not None
            else "call"
        )
        return f"step {index} (transform {name})"
    return f"step {index} ({step.type})"


class RoutedFieldSource(BaseModel):
    """The source field a router reads its ``type_field`` from, or why there is none."""

    model_config = ConfigDict(frozen=True)

    name: str | None
    blocked_by: str | None = None


def routed_field_source(
    level: Sequence[ActorConfig],
    field: str,
    transforms: Mapping[str, ProtoTransform],
) -> RoutedFieldSource:
    """The source field whose value a router of *level* reads as *field*.

    Every transform of a level runs before its routers, in the order listed,
    and a record keeps its own keys beside what the transforms write. Read
    backwards from the router: a ``rename`` onto the field leads to the field
    it renames, a key-renaming call leaves the values under the record's own
    keys alone, and any other step that does not write the field is passed.

    ``name`` is ``None`` when the value is not a source field's: a call writes
    it, a call's writes cannot be known, a ``rename`` takes the field away or
    several fields onto it, or a guarded ``rename`` touches it (the source
    would depend on the record). ``blocked_by`` names that step.

    Args:
        level: The steps of one pipeline level.
        field: A router's ``type_field``.
        transforms: ``ingestion_model.transforms`` by name, for ``call.use``.
    """
    current = field

    def blocked(index: int, step: ActorConfig, reason: str) -> RoutedFieldSource:
        return RoutedFieldSource(
            name=None, blocked_by=f"{describe_step(step, index)}: {reason}"
        )

    for index in range(len(level) - 1, -1, -1):
        step = level[index]
        if not isinstance(step, TransformActorConfig):
            continue
        if step.rename is not None:
            onto = [old for old, new in step.rename.items() if new == current]
            if not onto and current not in step.rename:
                continue
            if step.when is not None:
                return blocked(index, step, f"it renames {current!r} under a guard")
            if len(onto) > 1:
                return blocked(index, step, f"it renames {sorted(onto)} to {current!r}")
            if not onto:
                return blocked(index, step, f"it renames {current!r} away")
            current = onto[0]
            continue
        if step.call is not None and step.renames_keys(transforms):
            continue
        written = step.fields_written(transforms)
        if written is None:
            return blocked(index, step, "the fields it writes cannot be known")
        if current in written:
            return blocked(index, step, f"it writes {[current]}")
    return RoutedFieldSource(name=current)


def routers_keep_subset(pipeline: Sequence[ActorConfig]) -> bool:
    """Whether a top-level router of *pipeline* is closed or bounded by ``vertex_types``."""
    return any(
        isinstance(step, VertexRouterActorConfig) and step.keeps_subset
        for step in pipeline
    )


def routed_record_filter(
    pipeline: Sequence[ActorConfig],
    declared: Collection[str],
    transforms: Mapping[str, ProtoTransform],
) -> RoutedRecordFilter:
    """The records *pipeline* produces anything from, as routed values per field.

    Holds when every top-level step is a transform, a ``vertex_router``, or an
    ``edge`` between router roles, and each router's ``type_field`` reads a
    source field (:func:`routed_field_source`); the values are keyed by that
    field. A ``when`` guard on the ``type_field`` only reads it.

    Args:
        pipeline: The resource's top-level steps.
        declared: The classes the schema declares.
        transforms: ``ingestion_model.transforms`` by name, for ``call.use``.
    """
    routers = [s for s in pipeline if isinstance(s, VertexRouterActorConfig)]
    if not routers:
        return RoutedRecordFilter(fields=None, blocked_by="no top-level vertex_router")
    roles = {router.slot for router in routers}
    for index, step in enumerate(pipeline):
        if isinstance(step, (VertexRouterActorConfig, TransformActorConfig)):
            continue
        if isinstance(step, EdgeActorConfig) and _ends_in_roles(step, roles):
            continue
        reason = (
            "an edge end is not a router role"
            if isinstance(step, EdgeActorConfig)
            else "it produces records without a router"
        )
        return RoutedRecordFilter(
            fields=None, blocked_by=f"{describe_step(step, index)}: {reason}"
        )
    values: dict[str, set[Any]] = {}
    for router in routers:
        source = routed_field_source(pipeline, router.type_field, transforms)
        if source.name is None:
            return RoutedRecordFilter(fields=None, blocked_by=source.blocked_by)
        values.setdefault(source.name, set()).update(router.routed_values(declared))
    return RoutedRecordFilter(
        fields={field: frozenset(vals) for field, vals in values.items() if vals}
    )


def _ends_in_roles(step: EdgeActorConfig, roles: Collection[str]) -> bool:
    """Whether every end of every intent of *step* is one of *roles*."""
    return all(
        intent.source is None
        and intent.target is None
        and intent.source_role in roles
        and intent.target_role in roles
        for intent in step.intents()
    )
