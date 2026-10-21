"""What a producer states about entities, before anything is known about which they are.

A producer that holds only some properties of an entity states a
:class:`PartialRecord`: a type and the properties it holds. It rarely holds a
full key, so a record is not a vertex yet. A :class:`RecordGraph` holds one
source's records and the relations stated between them, keyed by local refs.

Each record, stated property and relation may carry ``evidence``: whatever the
producer keeps about where it read it. graflo stores and returns it, and never
interprets it.

Resolution (:mod:`graflo.link.resolve`) maps records to entities: it gives each
record it can identify an identity (:class:`Resolved`) and *stages* the rest
with the reason (:class:`StagedRecord`). A relation waits for its staged
endpoints (:class:`HeldRelation`). Nothing staged is written to the graph.
Candidate gathering (:mod:`graflo.link.candidates`) lists the entities a staged
record may be; something else -- a pick, a rule, a person -- decides.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.identifiers import EdgeId

_EVIDENCE = "The producer's own record of where this was stated; never interpreted."


class StatedProp(ConfigBaseModel):
    """A property value a record states."""

    value: Any
    evidence: dict[str, Any] | None = Field(default=None, description=_EVIDENCE)


class PartialRecord(ConfigBaseModel):
    """An entity a source states: its type and the properties it holds."""

    ref: str = Field(description="Local id, unique within its graph.")
    type: str = Field(description="Vertex type.")
    may_create: bool = Field(
        description=(
            "The record may create its entity when absent, rather than only "
            "reference one assumed to exist. Decides only whether a type that "
            "takes no key from the data is minted."
        )
    )
    props: dict[str, StatedProp] = Field(default_factory=dict)
    evidence: dict[str, Any] | None = Field(default=None, description=_EVIDENCE)

    def values(self) -> dict[str, Any]:
        """Stated values by property name."""
        return {name: prop.value for name, prop in self.props.items()}


class RecordRelation(ConfigBaseModel):
    """An edge a source states between two records."""

    edge_id: EdgeId
    source: str = Field(description="Ref of the source record.")
    target: str = Field(description="Ref of the target record.")
    props: dict[str, StatedProp] = Field(default_factory=dict)
    evidence: dict[str, Any] | None = Field(default=None, description=_EVIDENCE)

    def values(self) -> dict[str, Any]:
        """Stated values by property name."""
        return {name: prop.value for name, prop in self.props.items()}


class RecordGraph(ConfigBaseModel):
    """One source's records and the relations between them."""

    source_id: str
    schema_ref: str = Field(description="``<schema name>@<version>`` read against.")
    reference_time: datetime | None = None
    records: list[PartialRecord] = Field(default_factory=list)
    relations: list[RecordRelation] = Field(default_factory=list)

    @model_validator(mode="after")
    def _refs_resolve(self) -> RecordGraph:
        refs = [r.ref for r in self.records]
        if len(set(refs)) != len(refs):
            raise ValueError(f"{self.source_id}: record refs are not unique")
        known = set(refs)
        for relation in self.relations:
            for ref in (relation.source, relation.target):
                if ref not in known:
                    raise ValueError(
                        f"{self.source_id}: relation {relation.edge_id} names "
                        f"unknown record {ref!r}"
                    )
        return self


ResolvedBy = Literal["key", "alternate", "minted"]
StageReason = Literal[
    "partial_key",
    "no_key",
    "unmatched",
    "ambiguous",
    "owned_unmatched",
    "keyless_reference",
    "hidden",
]


class Resolved(ConfigBaseModel):
    """A record identified as one entity."""

    source_id: str
    ref: str
    type: str
    identity: dict[str, Any]
    by: ResolvedBy = Field(
        description=(
            "``key``: the record states a complete primary key. ``alternate``: a "
            "complete secondary identity found exactly one entity. ``minted``: a "
            "keyless entity the record may create."
        )
    )
    alternate: str | None = Field(
        default=None, description="Secondary identity the lookup used."
    )
    found_in: Literal["batch", "graph"] | None = Field(
        default=None,
        description="Where a lookup found the entity: another record or the graph.",
    )
    owned: bool = Field(
        default=False,
        description="The type is ingested by a connector: the record links, never writes.",
    )
    link_only: bool = Field(
        default=False,
        description=(
            "Found in the graph outside what the write may change (the "
            "resolution's ``writable`` filter): the record links, never writes."
        ),
    )


class StagedRecord(ConfigBaseModel):
    """A record not identified yet, and why."""

    source_id: str
    record: PartialRecord
    reason: StageReason = Field(
        description=(
            "``partial_key``: some key fields stated, no key complete. ``no_key``: "
            "no key field stated. ``unmatched``: a secondary identity found "
            "nothing. ``ambiguous``: it found several entities. "
            "``owned_unmatched``: a connector-owned entity the graph does not "
            "hold yet. ``keyless_reference``: a keyless type the record only "
            "references. ``hidden``: its key names an entity the resolution's "
            "guard hides, so it is neither written nor linked."
        )
    )
    matches: int = Field(default=0, description="Entities a lookup found.")


class HeldRelation(ConfigBaseModel):
    """A relation waiting for staged endpoints."""

    source_id: str
    relation: RecordRelation
    waiting_on: list[str] = Field(description="Refs of its staged endpoints.")


class Staged(ConfigBaseModel):
    """Everything resolution could not write yet."""

    records: list[StagedRecord] = Field(default_factory=list)
    relations: list[HeldRelation] = Field(default_factory=list)


class Resolution(ConfigBaseModel):
    """The outcome of resolving record graphs."""

    graphs: list[RecordGraph] = Field(default_factory=list)
    resolved: list[Resolved] = Field(default_factory=list)
    staged: Staged = Field(default_factory=Staged)

    def identity_of(self, source_id: str, ref: str) -> dict[str, Any] | None:
        """Identity of record *ref* of *source_id*, when resolved."""
        for item in self.resolved:
            if item.source_id == source_id and item.ref == ref:
                return item.identity
        return None


CandidateLane = Literal[
    "alternate", "exact", "key", "normalized", "alias", "relation", "metadata"
]


class EntityCandidate(ConfigBaseModel):
    """An entity a staged record may be. A candidate never closes a record."""

    vertex_type: str
    key: dict[str, Any] = Field(description="Its identity fields and values.")
    label: str | None = Field(default=None, description="Its label, if it has one.")
    lane: CandidateLane = Field(
        description=(
            "How it was found. ``alternate``: one of the entities a stated "
            "secondary identity matched. ``exact``, ``key``, ``normalized``, "
            "``alias``: a lookup over the record's stated labels and the "
            "caller's lookup text. ``relation``: linked by a stated relation to a "
            "resolved record of the same source. ``metadata``: named by the "
            "source's metadata, added by the caller."
        )
    )
    matched: str | None = Field(
        default=None,
        description="The words or values of the record it was found by.",
    )
    relation: str | None = Field(
        default=None, description="``relation`` lane: the relation followed."
    )
    anchor: str | None = Field(
        default=None,
        description="``relation`` lane: ref of the resolved record it is linked to.",
    )
    role: str | None = Field(
        default=None,
        description="``metadata`` lane: a role the caller assigns.",
    )


class StagedCandidates(ConfigBaseModel):
    """The candidates of one staged record, best first."""

    source_id: str
    record: PartialRecord
    reason: StageReason
    candidates: list[EntityCandidate] = Field(default_factory=list)
    truncated: bool = Field(
        default=False, description="More were found than the cap let through."
    )
