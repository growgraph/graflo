"""What a merge did to the sources, and the resource order it leaves them in."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.resource import (
    _steps_producing,
    step_finds,
    step_looks_up,
)
from graflo.architecture.contract.manifest import GraphManifest

from .equivalence import Side


@dataclass(frozen=True)
class KeyOwner:
    """A resource a derived identity names that produces the merged class.

    It computes the class's key, so it creates and fuses its nodes.
    """

    resource: str
    vertex: str
    side: Side
    members: tuple[str, ...]


@dataclass(frozen=True)
class AttachedProducer:
    """A resource merge turned from creating a merged class into attaching to it.

    Its steps producing the class now ``find`` it by *key*, write their
    properties onto the node found, and create none.
    """

    resource: str
    vertex: str
    side: Side
    members: tuple[str, ...]
    key: str
    """The secondary identity its records find the class by."""


@dataclass(frozen=True)
class PinnedReference:
    """A resource that only references a re-keyed member, pointed at its key.

    Its edge endpoints for the class now select *key*.
    """

    resource: str
    vertex: str
    side: Side
    members: tuple[str, ...]
    key: str


@dataclass(frozen=True)
class SharedKeySpace:
    """Members of one side whose demoted keys are one secondary identity.

    Their key values are taken to be one space: an id of one member finds a
    node of another.
    """

    vertex: str
    side: Side
    members: tuple[str, ...]
    key: str


@dataclass(frozen=True)
class OrderCycle:
    """A resource order constraint the union's resource order leaves unmet.

    *resource* depends on *before* for *vertex* -- it attaches to or
    references what *before* writes -- but runs first, because the
    constraints formed a cycle and the declared order was kept.
    """

    resource: str
    before: str
    vertex: str


@dataclass(frozen=True)
class DemotedKey:
    """A member's pre-merge key the merge demoted to a lookup-only secondary."""

    vertex: str
    side: Side
    member: str
    fields: tuple[str, ...]
    secondary: str
    branches: tuple[str, ...] = ()
    """Funnel branches the member's records can complete, in order, up to the
    one over its own key. Two or more mean the key no longer deduplicates."""


@dataclass(frozen=True)
class MergeReport:
    """What a merge did to the sources, beyond the manifest it returns."""

    demoted: list[DemotedKey] = field(default_factory=list)
    owners: list[KeyOwner] = field(default_factory=list)
    attached: list[AttachedProducer] = field(default_factory=list)
    references: list[PinnedReference] = field(default_factory=list)
    shared_key_spaces: list[SharedKeySpace] = field(default_factory=list)
    order_cycles: list[OrderCycle] = field(default_factory=list)


def _names_class(step: dict[str, Any], vertex: str) -> bool:
    """Whether *step*, producing *vertex*, names it rather than passing it through.

    A ``vertex_router`` names a class in its ``type_map`` targets, its
    ``vertex_types``, its ``find`` map or a ``lookup_only`` list. An open
    router reaches every declared class by pass-through, so counting that
    reach would make it a writer of every class. Any other step names what it
    produces.
    """
    if step.get("type") != "vertex_router":
        return True
    type_map = step.get("type_map")
    if isinstance(type_map, dict) and vertex in type_map.values():
        return True
    return any(
        isinstance(names, dict | list) and vertex in names
        for names in (
            step.get("find"),
            step.get("vertex_types"),
            step.get("lookup_only"),
        )
    )


def _order_after_merge(
    manifest: GraphManifest,
    owners: Sequence[KeyOwner],
    attached: Sequence[AttachedProducer],
) -> list[OrderCycle]:
    """Run each resource after the resources whose nodes it needs.

    A resource whose every step producing a class only looks it up or finds
    it runs after every resource writing that class -- otherwise, in one
    ingest, it finds nothing. A router step counts toward a class only when
    it names it (:func:`_names_class`). A resource attached to a merged class
    runs after the class's key owners on its own side, and is not constrained
    by the other side's: those never write the key it finds by. The declared
    order is moved only as far as that requires
    (:func:`~graflo.architecture.evolution.ordering.order_resources`). When
    the constraints form a cycle the resources in it keep their declared
    order among themselves, and each constraint left unmet is returned.
    """
    from .ordering import order_resources

    ingestion = manifest.ingestion_model
    schema = manifest.graph_schema
    if ingestion is None or schema is None:
        return []
    known = schema.core_schema.vertex_config.vertex_set
    because: dict[tuple[str, str], set[str]] = {}
    other_side: set[tuple[str, str, str]] = set()
    for producer in attached:
        for owner in owners:
            if owner.vertex != producer.vertex:
                continue
            if owner.side != producer.side:
                other_side.add((owner.resource, producer.resource, owner.vertex))
                continue
            because.setdefault((owner.resource, producer.resource), set()).add(
                producer.vertex
            )
    for vertex in sorted(known):
        writers: list[str] = []
        referrers: list[str] = []
        for resource in ingestion.resources:
            steps = _steps_producing(resource.pipeline, vertex, known_vertices=known)
            refers = [
                step_looks_up(step, vertex)
                or step_finds(step, vertex, known_vertices=known) is not None
                for step in steps
                if _names_class(step, vertex)
            ]
            if refers and all(refers):
                referrers.append(resource.name)
            elif refers:
                writers.append(resource.name)
        for writer in writers:
            for referrer in referrers:
                if (writer, referrer, vertex) not in other_side:
                    because.setdefault((writer, referrer), set()).add(vertex)

    declared = [resource.name for resource in ingestion.resources]
    order, unmet = order_resources(declared, because)
    if order != declared:
        by_name = {resource.name: resource for resource in ingestion.resources}
        ingestion.resources = [by_name[name] for name in order]
        manifest.ingestion_model = IngestionModel.model_validate(
            ingestion.to_dict(skip_defaults=False)
        )
    return [
        OrderCycle(resource=after, before=before, vertex=vertex)
        for before, after in unmet
        for vertex in sorted(because[(before, after)])
    ]
