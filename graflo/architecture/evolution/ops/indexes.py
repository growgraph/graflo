"""Ops that add or remove secondary indexes."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import Index


class AddVertexIndexesOp(ConfigBaseModel):
    """Author secondary indexes on vertices in the database profile."""

    op: Literal["add_vertex_indexes"] = "add_vertex_indexes"
    indexes: dict[str, Annotated[list[Index], PydanticField(min_length=1)]] = (
        PydanticField(
            ...,
            description="``{vertex_name: [Index, ...]}``.",
            min_length=1,
        )
    )


class RemoveVertexIndexesOp(ConfigBaseModel):
    """Withdraw authored vertex indexes, addressed by field list.

    Indexes derived from ``secondary_identities`` are not removable here — they would
    be re-registered by the next ``finish_init``. Use
    :class:`RemoveSecondaryIdentitiesOp` for those.
    """

    op: Literal["remove_vertex_indexes"] = "remove_vertex_indexes"
    indexes: dict[
        str,
        Annotated[
            list[Annotated[list[str], PydanticField(min_length=1)]],
            PydanticField(min_length=1),
        ],
    ] = PydanticField(
        ...,
        description="``{vertex_name: [[field, ...], ...]}``.",
        min_length=1,
    )


class EdgeIndexEntry(ConfigBaseModel):
    """Indexes for one edge physical spec."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    purpose: str | None = PydanticField(
        default=None,
        description="Physical variant purpose; ``None`` addresses the base spec.",
    )
    indexes: list[Index] = PydanticField(
        default_factory=list,
        description="Indexes to add to this spec.",
    )
    fields: list[list[str]] = PydanticField(
        default_factory=list,
        description="Field lists identifying indexes to remove from this spec.",
    )

    def physical_key(self) -> tuple[str, str, str | None, str | None]:
        return self.source, self.target, self.relation, self.purpose


def _validate_edge_index_entries(
    entries: Sequence[EdgeIndexEntry],
    *,
    kind: str,
    carries: Literal["indexes", "fields"],
) -> None:
    """Each entry addresses one spec, and carries the field the op reads.

    ``EdgeIndexEntry`` serves both ops, so the wrong field is silently ignored
    unless checked here.
    """
    keys = [entry.physical_key() for entry in entries]
    if len(keys) != len(set(keys)):
        raise ValueError(
            f"{kind} entries must be unique by (source, target, relation, purpose)"
        )
    empty = sorted(
        str(entry.physical_key()) for entry in entries if not getattr(entry, carries)
    )
    if empty:
        raise ValueError(f"{kind}: entries list no `{carries}`: {empty}")


class AddEdgeIndexesOp(ConfigBaseModel):
    """Author secondary indexes on edge physical specs."""

    op: Literal["add_edge_indexes"] = "add_edge_indexes"
    edges: list[EdgeIndexEntry] = PydanticField(
        ...,
        description="Per-spec indexes to add (``indexes`` on each entry).",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_entries(self) -> AddEdgeIndexesOp:
        _validate_edge_index_entries(
            self.edges, kind="add_edge_indexes", carries="indexes"
        )
        return self


class RemoveEdgeIndexesOp(ConfigBaseModel):
    """Withdraw authored indexes from edge physical specs, addressed by field list."""

    op: Literal["remove_edge_indexes"] = "remove_edge_indexes"
    edges: list[EdgeIndexEntry] = PydanticField(
        ...,
        description="Per-spec indexes to remove (``fields`` on each entry).",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_entries(self) -> RemoveEdgeIndexesOp:
        _validate_edge_index_entries(
            self.edges, kind="remove_edge_indexes", carries="fields"
        )
        return self
