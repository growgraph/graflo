"""Which records a resource's routers keep, read off its typed pipeline.

A resource whose top level only transforms, routes and links router roles
produces something from a record only when a ``vertex_router`` routes the
record's ``type_field`` value. Those values are a condition a source can apply
before it reads (a table connector pushes them into its query), and connector
pruning uses the same rule to decide which requests still reach a class.
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

    ``fields`` maps each routed ``type_field`` to the values its routers send
    somewhere; a record counts when any field holds one of its values. An empty
    mapping means no record produces anything. ``fields`` is ``None`` when a
    record may produce something without a router; ``blocked_by`` then names
    the step that allows it.
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


def fields_written_before_routing(
    level: Sequence[ActorConfig], transforms: Mapping[str, ProtoTransform]
) -> frozenset[str] | None:
    """Fields the transforms of one pipeline level may write; ``None`` if one cannot say.

    Every transform of a level runs before the level's routers, whatever its
    place in the list, so all of them count.
    """
    written: set[str] = set()
    for step in level:
        if not isinstance(step, TransformActorConfig):
            continue
        fields = step.fields_written(transforms)
        if fields is None:
            return None
        written |= fields
    return frozenset(written)


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
    ``edge`` between router roles, and no transform of the level writes a
    router's ``type_field``. A transform's writes are resolved the way the
    runtime resolves them (:meth:`TransformActorConfig.fields_written`): a
    ``when`` guard on the ``type_field`` only reads it.

    Args:
        pipeline: The resource's top-level steps.
        declared: The classes the schema declares.
        transforms: ``ingestion_model.transforms`` by name, for ``call.use``.
    """
    routers = [s for s in pipeline if isinstance(s, VertexRouterActorConfig)]
    if not routers:
        return RoutedRecordFilter(fields=None, blocked_by="no top-level vertex_router")
    roles = {router.slot for router in routers}
    routed_fields = {router.type_field for router in routers}
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
    for index, step in enumerate(pipeline):
        if not isinstance(step, TransformActorConfig):
            continue
        written = step.fields_written(transforms)
        if written is None:
            return RoutedRecordFilter(
                fields=None,
                blocked_by=(
                    f"{describe_step(step, index)}: the fields it writes cannot "
                    "be known"
                ),
            )
        rewritten = sorted(written & routed_fields)
        if rewritten:
            return RoutedRecordFilter(
                fields=None,
                blocked_by=f"{describe_step(step, index)}: it writes {rewritten}",
            )
    values: dict[str, set[Any]] = {}
    for router in routers:
        values.setdefault(router.type_field, set()).update(
            router.routed_values(declared)
        )
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
