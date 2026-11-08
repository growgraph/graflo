"""Record sync ownership: which resource's records replace which elements.

A resource that declares ``sync`` **owns** one vertex type and a set of edges.
In sync mode, re-ingesting one of its records replaces that record's vertex
properties and deletes and recreates the edges it owns. Each owned edge belongs
to one owner record, its **owner key**: the source endpoint when the resource
owns the source type, else the target endpoint when it owns the target type,
else the value of the edge's ``owner_field``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, NamedTuple

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeId

if TYPE_CHECKING:
    from graflo.architecture.contract.ingestion.resource import ResourceConfig
    from graflo.architecture.schema import CoreSchema

#: Where an owned edge's owner key is read: an endpoint, or ``owner_field``.
OwnerSide = Literal["source", "target", "field"]


class OwnedEdge(NamedTuple):
    """How one declared edge is owned: by which vertex type, read from where."""

    owner: str
    side: OwnerSide
    owner_field: str | None


class SyncEdge(ConfigBaseModel):
    """An edge a resource's records own."""

    source: str = PydanticField(..., description="Edge source vertex type.")
    target: str = PydanticField(..., description="Edge target vertex type.")
    relation: str | None = PydanticField(default=None, description="Edge relation.")
    owner_field: str | None = PydanticField(
        default=None,
        description=(
            "Edge identity field holding the owner key, for an edge whose "
            "endpoints are neither of the owned type."
        ),
    )

    @property
    def edge_id(self) -> EdgeId:
        return self.source, self.target, self.relation


class ResourceSync(ConfigBaseModel):
    """What a resource's records replace when re-ingested in sync mode."""

    owns: str = PydanticField(
        ..., description="Vertex type whose properties the resource's records replace."
    )
    edges: list[SyncEdge] = PydanticField(
        default_factory=list,
        description="Edges the resource's records replace, each owned by one record.",
    )

    def owner_side(self, edge: SyncEdge) -> OwnerSide:
        """Where *edge*'s owner key is read; the source wins when both endpoints are owned."""
        if edge.source == self.owns:
            return "source"
        if edge.target == self.owns:
            return "target"
        return "field"


def owned_edges(resources: Iterable[ResourceConfig]) -> dict[EdgeId, OwnedEdge]:
    """Every edge the *resources* own, by its declared id."""
    return {
        entry.edge_id: OwnedEdge(
            resource.sync.owns, resource.sync.owner_side(entry), entry.owner_field
        )
        for resource in resources
        if resource.sync is not None
        for entry in resource.sync.edges
    }


def owned_types(resources: Iterable[ResourceConfig]) -> set[str]:
    """Every vertex type the *resources* own."""
    return {r.sync.owns for r in resources if r.sync is not None}


def sync_errors(
    resources: Sequence[ResourceConfig],
    produced: Mapping[str, set[str]],
    core_schema: CoreSchema,
) -> list[str]:
    """Why the resources' ``sync`` blocks cannot be honoured; empty when they can.

    Args:
        resources: Every resource of the ingestion model.
        produced: The vertex types each resource can produce, by resource name.
        core_schema: The schema the blocks refer to.
    """
    from graflo.architecture.contract.ingestion.emits import may_emit, step_may_write
    from graflo.architecture.contract.ingestion.steps.ref import iter_edge_steps

    errors: list[str] = []
    owners: dict[str, str] = {}
    edge_owners: dict[EdgeId, str] = {}
    vertices = core_schema.vertex_config.vertex_set
    declared = dict(core_schema.edge_config.items())
    for resource in resources:
        sync = resource.sync
        if sync is None:
            continue
        name = resource.name
        if sync.owns not in vertices:
            errors.append(
                f"{name}: owns {sync.owns!r}, which the schema does not declare"
            )
        elif sync.owns not in produced[name]:
            errors.append(f"{name}: owns {sync.owns!r}, which it does not produce")
        if sync.owns in owners:
            errors.append(
                f"{name}: owns {sync.owns!r}, already owned by {owners[sync.owns]!r}"
            )
        owners.setdefault(sync.owns, name)
        for entry in sync.edges:
            edge_id = entry.edge_id
            if edge_id in edge_owners:
                errors.append(
                    f"{name}: owns edge {edge_id}, already owned by "
                    f"{edge_owners[edge_id]!r}"
                )
                continue
            edge_owners[edge_id] = name
            edge = declared.get(edge_id)
            if edge is None:
                errors.append(
                    f"{name}: owns edge {edge_id}, which the schema does not declare"
                )
                continue
            if not may_emit(resource, edge_id, produced[name]):
                errors.append(f"{name}: owns edge {edge_id}, which it does not write")
            if any(
                view.relation_is_data_driven and step_may_write(view, edge_id)
                for view in iter_edge_steps(resource.pipeline)
            ):
                # Its owned edges are deleted by type, which must be known here.
                errors.append(
                    f"{name}: owns edge {edge_id}, whose relation a step reads "
                    "from the data"
                )
            side = sync.owner_side(entry)
            identity = edge.identities[0] if edge.identities else []
            if side != "field":
                if entry.owner_field is not None:
                    errors.append(
                        f"{name}: edge {edge_id} is owned by its {side}; "
                        "it takes no owner_field"
                    )
            elif entry.owner_field is None:
                errors.append(
                    f"{name}: edge {edge_id} touches no {sync.owns!r}; name the "
                    "identity field that holds its owner as owner_field"
                )
            elif entry.owner_field in ("source", "target", "relation") or (
                entry.owner_field not in identity
            ):
                errors.append(
                    f"{name}: owner_field {entry.owner_field!r} of edge {edge_id} "
                    f"is not an identity field of the edge ({identity})"
                )
            elif (
                sync.owns in vertices
                and len(core_schema.vertex_config.identity_fields(sync.owns)) != 1
            ):
                # The field holds one value, so it names an owner by one field.
                errors.append(
                    f"{name}: edge {edge_id} names its owner by owner_field, which "
                    f"needs {sync.owns!r} to have a single identity field"
                )
    for edge_id, owner in edge_owners.items():
        others = [
            resource.name
            for resource in resources
            if resource.name != owner
            and may_emit(resource, edge_id, produced[resource.name])
        ]
        if others:
            errors.append(
                f"edge {edge_id} is owned by {owner!r} but also written by {others}"
            )
    return errors
