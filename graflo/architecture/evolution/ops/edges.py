"""Ops that add, remove, merge, rename, retarget, or redirect edges."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any, Literal

from pydantic import AliasChoices, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import (
    Field,
)

from .validation import validate_merge_sources, validate_rename_map_is_injective


class RenameRelationsOp(ConfigBaseModel):
    """Rename logical edge relation names across schema and ingestion."""

    op: Literal["rename_relations"] = "rename_relations"
    renames: dict[str, str] = PydanticField(
        ...,
        validation_alias=AliasChoices("renames", "relations"),
        description=(
            "Relation rename map: ``{old_relation: new_relation}``. Must be "
            "injective. ``relations`` is accepted as a legacy alias."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameRelationsOp:
        validate_rename_map_is_injective(
            self.renames,
            kind="rename_relations",
            merge_hint="MergeEdgesOp(sources=[...], into=...)",
        )
        return self


class EdgeSelector(ConfigBaseModel):
    """Schema edge triple selector matching :data:`~graflo.architecture.graph_types.EdgeId`."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches edges with no relation set.",
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation


def _validate_unique_edge_selectors(
    selectors: Sequence[EdgeSelector], *, kind: str
) -> None:
    edge_ids = [selector.edge_id() for selector in selectors]
    if len(edge_ids) != len(set(edge_ids)):
        raise ValueError(
            f"{kind} edges entries must be unique by (source, target, relation)"
        )


class RemoveEdgesOp(ConfigBaseModel):
    """Remove logical edges from schema, profile, and ingestion selectors.

    Two addressing forms, combinable in one op. ``relations`` removes a relation
    on every endpoint pair it occurs on. ``edges`` removes exactly the named
    triples, which is the only way to remove one pair of several sharing a
    relation, or an edge with no relation set at all.
    """

    op: Literal["remove_edges"] = "remove_edges"
    relations: list[str] = PydanticField(
        default_factory=list,
        description="Relation names to remove from edge definitions and references.",
    )
    edges: list[EdgeSelector] = PydanticField(
        default_factory=list,
        description="Edge triples ``(source, target, relation)`` to remove.",
    )

    @model_validator(mode="after")
    def _validate_targets(self) -> RemoveEdgesOp:
        if not self.relations and not self.edges:
            raise ValueError("remove_edges requires at least one of relations or edges")
        _validate_unique_edge_selectors(self.edges, kind="remove_edges")
        return self


class MergeEdgesOp(ConfigBaseModel):
    """Merge source relation names into a canonical relation name."""

    op: Literal["merge_edges"] = "merge_edges"
    sources: list[str] = PydanticField(
        ...,
        description="Relation names to merge away. Must not include ``into``.",
        min_length=1,
    )
    into: str = PydanticField(
        ...,
        description="Canonical relation name that receives all source relations.",
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> MergeEdgesOp:
        validate_merge_sources(self.sources, self.into, kind="merge_edges")
        return self


def validate_per_edge(
    by_relation: Mapping[str, Collection[str]],
    per_edge: Sequence[EdgeSelector],
    names: Callable[[Any], Collection[str]],
    *,
    kind: str,
) -> None:
    """Refuse an empty op, a triple named twice, and a property named both ways.

    A property changed relation-wide and on one edge of that relation at once
    has two readings, so neither is picked.
    """
    if not by_relation and not per_edge:
        raise ValueError(f"{kind} requires at least one relation or per_edge entry")
    _validate_unique_edge_selectors(per_edge, kind=kind)
    for entry in per_edge:
        if entry.relation is None:
            continue
        both = sorted(set(names(entry)) & set(by_relation.get(entry.relation, ())))
        if both:
            raise ValueError(
                f"{kind}: {both} on {entry.edge_id()} are named both for relation "
                f"{entry.relation!r} and for the edge"
            )


class EdgePropertyRenames(EdgeSelector):
    """Property renames on one edge triple."""

    renames: dict[str, str] = PydanticField(
        ..., description="``{old_field: new_field}``.", min_length=1
    )


class EdgePropertyRemovals(EdgeSelector):
    """Property removals on one edge triple."""

    fields: list[str] = PydanticField(
        ..., description="Names of the properties to remove.", min_length=1
    )


class EdgePropertyAdditions(EdgeSelector):
    """Property additions on one edge triple."""

    properties: list[str | Field] = PydanticField(
        ...,
        description="Properties to add: a bare name or a full field.",
        min_length=1,
    )

    def field_names(self) -> list[str]:
        return [
            entry if isinstance(entry, str) else entry.name for entry in self.properties
        ]


class RenameEdgePropertiesOp(ConfigBaseModel):
    """Rename edge properties across schema/profile/ingestion.

    ``renames`` addresses a relation and applies to every edge on it;
    ``per_edge`` addresses one triple, including an edge with no relation.
    """

    op: Literal["rename_edge_properties"] = "rename_edge_properties"
    renames: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-relation edge field rename map: "
            "``{relation_name: {old_field: new_field}}``."
        ),
    )
    per_edge: list[EdgePropertyRenames] = PydanticField(
        default_factory=list, description="Renames on single edge triples."
    )

    @model_validator(mode="after")
    def _validate_targets(self) -> RenameEdgePropertiesOp:
        validate_per_edge(
            self.renames,
            self.per_edge,
            lambda entry: entry.renames,
            kind="rename_edge_properties",
        )
        return self

    def renames_for(self, edge_id: tuple[str, str, str | None]) -> dict[str, str]:
        """The renames that reach the edge *edge_id*, from either form."""
        out = dict(self.renames.get(edge_id[2], {})) if edge_id[2] is not None else {}
        for entry in self.per_edge:
            if entry.edge_id() == edge_id:
                out |= entry.renames
        return out


class RemoveEdgePropertiesOp(ConfigBaseModel):
    """Remove edge properties across schema/profile/ingestion.

    ``removals`` addresses a relation and applies to every edge on it;
    ``per_edge`` addresses one triple, including an edge with no relation.
    """

    op: Literal["remove_edge_properties"] = "remove_edge_properties"
    removals: dict[str, list[str]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-relation edge field removals: ``{relation_name: [field_name, ...]}``."
        ),
    )
    per_edge: list[EdgePropertyRemovals] = PydanticField(
        default_factory=list, description="Removals on single edge triples."
    )

    @model_validator(mode="after")
    def _validate_targets(self) -> RemoveEdgePropertiesOp:
        validate_per_edge(
            self.removals,
            self.per_edge,
            lambda entry: entry.fields,
            kind="remove_edge_properties",
        )
        return self

    def removals_for(self, edge_id: tuple[str, str, str | None]) -> set[str]:
        """The names removed from the edge *edge_id*, from either form."""
        out = (
            set(self.removals.get(edge_id[2], [])) if edge_id[2] is not None else set()
        )
        for entry in self.per_edge:
            if entry.edge_id() == edge_id:
                out |= set(entry.fields)
        return out


class AddEdgePropertiesOp(ConfigBaseModel):
    """Add edge properties for each relation in schema/profile defaults.

    An entry may be a bare name (an untyped property) or a full
    :class:`~graflo.architecture.schema.vertex.Field`, as on
    :class:`AddVertexPropertiesOp`, so a typed or grounded edge property is
    one replayable step rather than an add followed by a type change.

    ``additions`` addresses a relation and applies to every edge on it;
    ``per_edge`` addresses one triple, including an edge with no relation.
    """

    op: Literal["add_edge_properties"] = "add_edge_properties"
    additions: dict[str, list[str | Field]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-relation edge property additions: "
            "``{relation_name: [field_name | Field, ...]}``."
        ),
    )
    per_edge: list[EdgePropertyAdditions] = PydanticField(
        default_factory=list, description="Additions on single edge triples."
    )

    @model_validator(mode="after")
    def _validate_targets(self) -> AddEdgePropertiesOp:
        validate_per_edge(
            {relation: self.field_names(relation) for relation in self.additions},
            self.per_edge,
            lambda entry: entry.field_names(),
            kind="add_edge_properties",
        )
        return self

    def field_names(self, relation: str) -> list[str]:
        """The names added to *relation*, whichever shape they were written in."""
        return [
            entry if isinstance(entry, str) else entry.name
            for entry in self.additions.get(relation, [])
        ]

    def additions_for(self, edge_id: tuple[str, str, str | None]) -> list[str | Field]:
        """The entries that reach the edge *edge_id*, from either form."""
        out = list(self.additions.get(edge_id[2], [])) if edge_id[2] is not None else []
        for entry in self.per_edge:
            if entry.edge_id() == edge_id:
                out += entry.properties
        return out


class AddEdgesOp(ConfigBaseModel):
    """Introduce new logical edge relations between existing vertex types."""

    op: Literal["add_edges"] = "add_edges"
    edges: list[Edge] = PydanticField(
        ...,
        description="Full edge definitions, in the shape the schema block accepts.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_ids(self) -> AddEdgesOp:
        edge_ids = [(edge.source, edge.target, edge.relation) for edge in self.edges]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError(
                "add_edges entries must be unique by (source, target, relation)"
            )
        return self


class EdgeRetargetEntry(ConfigBaseModel):
    """Repoint one edge triple at a different source and/or target vertex type."""

    source: str = PydanticField(..., description="Current source vertex type name.")
    target: str = PydanticField(..., description="Current target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    new_source: str | None = PydanticField(
        default=None,
        description="Replacement source vertex type; omit to keep the current one.",
    )
    new_target: str | None = PydanticField(
        default=None,
        description="Replacement target vertex type; omit to keep the current one.",
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation

    def retargeted_edge_id(self) -> tuple[str, str, str | None]:
        return (
            self.new_source or self.source,
            self.new_target or self.target,
            self.relation,
        )

    @model_validator(mode="after")
    def _require_a_change(self) -> EdgeRetargetEntry:
        if self.new_source is None and self.new_target is None:
            raise ValueError(
                "retarget_edges requires at least one of new_source or new_target"
            )
        if self.retargeted_edge_id() == self.edge_id():
            raise ValueError(
                f"retarget_edges: edge {self.edge_id()} would not change endpoints"
            )
        return self


class RetargetEdgesOp(ConfigBaseModel):
    """Change which vertex types an edge connects, preserving everything else.

    Remove-plus-add would lose the edge's properties, uniqueness keys, ``directed``
    flag, and its ``db_profile`` physical spec. Retargeting rewrites the ``EdgeId``
    everywhere it is keyed instead: edge config, physical specs, and pipeline edge steps.
    """

    op: Literal["retarget_edges"] = "retarget_edges"
    edges: list[EdgeRetargetEntry] = PydanticField(
        ...,
        description="Per-edge endpoint retargeting.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> RetargetEdgesOp:
        edge_ids = [entry.edge_id() for entry in self.edges]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError(
                "retarget_edges entries must be unique by (source, target, relation)"
            )
        retargeted = [entry.retargeted_edge_id() for entry in self.edges]
        if len(retargeted) != len(set(retargeted)):
            raise ValueError(
                "retarget_edges entries must not collide after retargeting"
            )
        return self


class SetEdgeDirectedOp(ConfigBaseModel):
    """Set the ``directed`` flag on logical edges.

    Small, but load-bearing for replay: ``directed`` decides what
    :class:`AddInverseEdgesOp` is allowed to duplicate, so an un-authorable flag makes
    inverse-edge change sets non-replayable.
    """

    op: Literal["set_edge_directed"] = "set_edge_directed"
    edges: list[EdgeSelector] = PydanticField(
        ...,
        description="Edge triples whose ``directed`` flag changes.",
        min_length=1,
    )
    directed: bool = PydanticField(
        ...,
        description="Value applied to every selected edge.",
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> SetEdgeDirectedOp:
        _validate_unique_edge_selectors(self.edges, kind="set_edge_directed")
        return self
