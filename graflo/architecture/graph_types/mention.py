"""What a text names, before anything is known about which entities it means.

An extractor reading text sees *mentions*: a type, the properties the text
states, and where it states them. A text rarely states a full key, so a mention
is not a vertex yet. A :class:`MentionGraph` holds one source's mentions and
the relations the text states between them, keyed by local refs.

Resolution (``graflo.hq.mention_resolve``) maps mentions to entities: it gives
each mention it can identify an identity (:class:`Resolved`) and *stages* the
rest with the reason (:class:`StagedMention`). A relation waits for its staged
endpoints (:class:`HeldRelation`). Nothing staged is written to the graph.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.identifiers import EdgeId


class TextSpan(ConfigBaseModel):
    """Character offsets, end exclusive, into the source text, or into the
    context item ``context`` names."""

    start: int
    end: int
    context: str | None = Field(
        default=None, description="Source id of the context item, if not the text."
    )


class StatedProp(ConfigBaseModel):
    """A property value the text states."""

    value: Any
    quote: str
    span: TextSpan


class Mention(ConfigBaseModel):
    """An entity a text names: its type and what the text states about it."""

    ref: str = Field(description="Local id, unique within its graph.")
    type: str = Field(description="Vertex type.")
    introduces: bool = Field(
        description=(
            "The text introduces the entity rather than refers to one assumed "
            "known. Decides only whether a type with no key in the data is minted."
        )
    )
    quote: str
    span: TextSpan
    props: dict[str, StatedProp] = Field(default_factory=dict)

    def values(self) -> dict[str, Any]:
        """Stated values by property name."""
        return {name: prop.value for name, prop in self.props.items()}


class Relation(ConfigBaseModel):
    """An edge the text states between two mentions."""

    edge_id: EdgeId
    source: str = Field(description="Ref of the source mention.")
    target: str = Field(description="Ref of the target mention.")
    quote: str
    span: TextSpan
    props: dict[str, StatedProp] = Field(default_factory=dict)

    def values(self) -> dict[str, Any]:
        """Stated values by property name."""
        return {name: prop.value for name, prop in self.props.items()}


class UnmappedText(ConfigBaseModel):
    """Something the text states that no type covers."""

    quote: str
    note: str
    span: TextSpan


class MentionGraph(ConfigBaseModel):
    """One source's mentions and the relations between them."""

    source_id: str
    schema_ref: str = Field(description="``<schema name>@<version>`` read against.")
    reference_time: datetime | None = None
    mentions: list[Mention] = Field(default_factory=list)
    relations: list[Relation] = Field(default_factory=list)
    unmapped: list[UnmappedText] = Field(default_factory=list)

    @model_validator(mode="after")
    def _refs_resolve(self) -> MentionGraph:
        refs = [m.ref for m in self.mentions]
        if len(set(refs)) != len(refs):
            raise ValueError(f"{self.source_id}: mention refs are not unique")
        known = set(refs)
        for relation in self.relations:
            for ref in (relation.source, relation.target):
                if ref not in known:
                    raise ValueError(
                        f"{self.source_id}: relation {relation.edge_id} names "
                        f"unknown mention {ref!r}"
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
]


class Resolved(ConfigBaseModel):
    """A mention identified as one entity."""

    source_id: str
    ref: str
    type: str
    identity: dict[str, Any]
    by: ResolvedBy = Field(
        description=(
            "``key``: the mention states a complete primary key. ``alternate``: a "
            "complete secondary identity found exactly one entity. ``minted``: a "
            "keyless entity the text introduces."
        )
    )
    alternate: str | None = Field(
        default=None, description="Secondary identity the lookup used."
    )
    found_in: Literal["batch", "graph"] | None = Field(
        default=None,
        description="Where a lookup found the entity: another mention or the graph.",
    )
    owned: bool = Field(
        default=False,
        description="The type is ingested by a connector: the mention links, never writes.",
    )


class StagedMention(ConfigBaseModel):
    """A mention not identified yet, and why."""

    source_id: str
    mention: Mention
    reason: StageReason = Field(
        description=(
            "``partial_key``: some key fields stated, no key complete. ``no_key``: "
            "no key field stated. ``unmatched``: a secondary identity found "
            "nothing. ``ambiguous``: it found several entities. "
            "``owned_unmatched``: a connector-owned entity the graph does not "
            "hold yet. ``keyless_reference``: a keyless type the text refers to "
            "without introducing it."
        )
    )
    matches: int = Field(default=0, description="Entities a lookup found.")


class HeldRelation(ConfigBaseModel):
    """A relation waiting for staged endpoints."""

    source_id: str
    relation: Relation
    waiting_on: list[str] = Field(description="Refs of its staged endpoints.")


class Staged(ConfigBaseModel):
    """Everything resolution could not write yet."""

    mentions: list[StagedMention] = Field(default_factory=list)
    relations: list[HeldRelation] = Field(default_factory=list)


class Resolution(ConfigBaseModel):
    """The outcome of resolving mention graphs."""

    graphs: list[MentionGraph] = Field(default_factory=list)
    resolved: list[Resolved] = Field(default_factory=list)
    staged: Staged = Field(default_factory=Staged)

    def identity_of(self, source_id: str, ref: str) -> dict[str, Any] | None:
        """Identity of mention *ref* of *source_id*, when resolved."""
        for item in self.resolved:
            if item.source_id == source_id and item.ref == ref:
                return item.identity
        return None
