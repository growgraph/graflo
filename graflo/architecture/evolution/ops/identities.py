"""Ops that replace or extend vertex and edge identities."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import AliasChoices, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.identity_funnel import IdentityFunnel
from graflo.architecture.schema.vertex import (
    SecondaryIdentity,
)


class NaturalIdentityTarget(ConfigBaseModel):
    """Target a natural key: the named properties identify the vertex directly."""

    mode: Literal["natural"] = "natural"
    identity: list[str] = PydanticField(
        ...,
        description="Property names forming the new primary identity.",
        min_length=1,
    )


class HashIdentityTarget(ConfigBaseModel):
    """Target a hash identity: a deterministic synthetic ``id`` digested from fields."""

    mode: Literal["hash"] = "hash"
    hash_from: list[str] = PydanticField(
        ...,
        description="Source property names whose values are digested into ``id``.",
        min_length=1,
    )


class FunnelIdentityTarget(ConfigBaseModel):
    """Target an identity funnel: fallback branches digested into ``digest_field``.

    The general form of :class:`HashIdentityTarget` — a flat hash key is a funnel
    with one branch. Both resolve to identity mode ``hash``.
    """

    mode: Literal["funnel"] = "funnel"
    funnel: IdentityFunnel = PydanticField(
        ...,
        description="Ordered fallback branches; the first complete one wins.",
    )
    digest_field: str = PydanticField(
        default="id",
        min_length=1,
        description="Property the digest is stored in; the vertex's identity field.",
    )

    @model_validator(mode="after")
    def _digest_field_is_no_branch_field(self) -> FunnelIdentityTarget:
        # The cast drops a digest vertex's identity field from each record, so
        # a branch reading the same field could never complete.
        if self.digest_field in self.funnel.field_names:
            raise ValueError(
                f"funnel identity: digest_field {self.digest_field!r} is also a "
                "branch field; the digest would replace that branch's input"
            )
        return self


class AssignedIdentityTarget(ConfigBaseModel):
    """Target an assigned identity: an intentional UUID primary key."""

    mode: Literal["assigned"] = "assigned"


class BlankIdentityTarget(ConfigBaseModel):
    """Target a blank identity: an auto-generated placeholder ID."""

    mode: Literal["blank"] = "blank"


IdentityTarget = Annotated[
    NaturalIdentityTarget
    | HashIdentityTarget
    | FunnelIdentityTarget
    | AssignedIdentityTarget
    | BlankIdentityTarget,
    PydanticField(discriminator="mode"),
]


class IdentityReplacement(ConfigBaseModel):
    """New identity policy for one vertex, plus what becomes of the old one."""

    to: IdentityTarget = PydanticField(
        ...,
        description="The identity policy this vertex should have after the op.",
    )
    retire: Literal["demote", "keep", "drop"] = PydanticField(
        default="demote",
        description=(
            "What happens to the old identity field-set. ``demote`` turns it into a "
            "secondary identity (lookup index follows automatically), ``keep`` leaves "
            "the fields as plain properties, ``drop`` removes them. Demotion is "
            "downgraded to ``keep`` when the old identity was synthetic (hash / "
            "assigned / blank) or already equals the new one."
        ),
    )
    retire_as: str | None = PydanticField(
        default=None,
        description=(
            "Name for the demoted secondary identity. Defaults to "
            "``retired_identity``. Only meaningful with ``retire: demote``."
        ),
    )
    endpoints: Literal["follow_new", "pin_to_retired"] = PydanticField(
        default="follow_new",
        description=(
            "How edge steps that match this vertex on its primary identity behave "
            "afterwards. ``follow_new`` (default) leaves them on the primary, so they "
            "match the new identity. ``pin_to_retired`` rewrites them to select the "
            "demoted secondary identity, preserving the previous matching behaviour "
            "for sources that only carry the old key. Requires ``retire: demote``."
        ),
    )

    @model_validator(mode="after")
    def _validate_endpoint_policy(self) -> IdentityReplacement:
        if self.endpoints == "pin_to_retired" and self.retire != "demote":
            raise ValueError(
                "endpoints: pin_to_retired requires retire: demote — there is no "
                "retired secondary identity to pin to otherwise"
            )
        if self.retire_as is not None and self.retire != "demote":
            raise ValueError("retire_as is only meaningful with retire: demote")
        return self


class ReplaceIdentityOp(ConfigBaseModel):
    """Replace the identity policy of one or more vertices.

    Covers both a change of identity *fields* and a change of identity *mode*
    (``natural`` / ``hash`` / ``assigned`` / ``blank``), because the cascade is the
    same in either case: the field-set that upserts changes, and everything that
    referenced the old one must be repointed or retired.

    Not covered: ``blank`` vertices cannot retire by demotion (they cannot declare
    secondary identities at all).
    """

    op: Literal["replace_identity"] = "replace_identity"
    replacements: dict[str, IdentityReplacement] = PydanticField(
        ...,
        validation_alias=AliasChoices("replacements", "vertices"),
        description=(
            "Per-vertex identity replacement: ``{vertex_name: replacement}``. "
            "``vertices`` is accepted as a legacy alias."
        ),
        min_length=1,
    )


class AddSecondaryIdentitiesOp(ConfigBaseModel):
    """Declare alternate lookup keys on existing vertices.

    Secondary identities are lookup-only: upserts keep using the primary identity.
    Each declared field-set automatically gains a non-unique index at
    :meth:`Schema.finish_init`, so this op is how an edge-only source gains a way to
    reference endpoints by a business key without touching the primary identity.
    """

    op: Literal["add_secondary_identities"] = "add_secondary_identities"
    additions: dict[str, list[SecondaryIdentity]] = PydanticField(
        ...,
        description=(
            "Per-vertex secondary identities to declare: "
            "``{vertex_name: [{name, fields}, ...]}``. A bare field list is accepted "
            "for each entry and auto-named."
        ),
        min_length=1,
    )


class RemoveSecondaryIdentitiesOp(ConfigBaseModel):
    """Withdraw alternate lookup keys, dropping their derived indexes.

    Rejected when a surviving edge step still selects the removed field-set — that
    step would have no way to resolve its endpoint.
    """

    op: Literal["remove_secondary_identities"] = "remove_secondary_identities"
    removals: dict[str, list[str | list[str]]] = PydanticField(
        ...,
        description=(
            "Per-vertex secondary identities to withdraw, addressed by name or by "
            "field list: ``{vertex_name: [name | [field, ...], ...]}``."
        ),
        min_length=1,
    )


class EdgeIdentitiesEntry(ConfigBaseModel):
    """New uniqueness keys for one edge triple."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    identities: list[list[str]] = PydanticField(
        ...,
        description=(
            "Replacement uniqueness keys. Each key lists fields that, together with "
            "the resolved endpoints, must be unique; the ``source`` / ``target`` "
            "tokens stand for the endpoints themselves. An empty list clears them."
        ),
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation


class ReplaceEdgeIdentitiesOp(ConfigBaseModel):
    """Replace the uniqueness keys of logical edges.

    The edge-side counterpart of :class:`ReplaceIdentityOp`. There is no retire policy:
    edge identities have no lookup plane to demote into. Non-endpoint tokens are merged
    into edge ``properties`` by ``Edge.finish_init``, as with authored identities.
    """

    op: Literal["replace_edge_identities"] = "replace_edge_identities"
    edges: list[EdgeIdentitiesEntry] = PydanticField(
        ...,
        description="Per-edge replacement uniqueness keys.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> ReplaceEdgeIdentitiesOp:
        edge_ids = [entry.edge_id() for entry in self.edges]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError(
                "replace_edge_identities entries must be unique by "
                "(source, target, relation)"
            )
        return self
