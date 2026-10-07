"""Ops that declare, materialize, and emit inverse edges."""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.steps.ref import EdgeStepRef
from graflo.architecture.schema.edge import normalize_inverse_table


class DeclareEdgeInversesOp(ConfigBaseModel):
    """Declare inverse pairs and symmetric relations in ``edge_config``.

    Purely logical: it records how relation names read one fact from its two
    endpoints and creates nothing. ``inverses`` pairs two distinct names; a pair
    is unordered, so ``{a: b}``, ``{b: a}`` and ``{a: b, b: a}`` declare the same
    thing. ``symmetric`` names relations that are their own inverse. Together
    they must give every relation at most one inverse (no ``a: b`` with
    ``b: c``), in the op and against what is already declared.

    Realize a pair with :class:`AddInverseEdgesOp` (explicit, portable inverse
    edges) or :class:`SetNativeInversesOp` (TigerGraph maintains the pair) --
    never both for one relation. A symmetric relation is realized by its edges
    being undirected (:class:`SetEdgeDirectedOp`), which the schema requires.
    """

    op: Literal["declare_edge_inverses"] = "declare_edge_inverses"
    inverses: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Pairs to declare: ``{relation: inverse_relation}``, either order.",
    )
    symmetric: list[str] = PydanticField(
        default_factory=list,
        description="Relations to declare as their own inverse.",
    )

    @model_validator(mode="after")
    def _validate_table(self) -> DeclareEdgeInversesOp:
        if not self.inverses and not self.symmetric:
            raise ValueError(
                "declare_edge_inverses: nothing to declare; give inverses or symmetric"
            )
        normalize_inverse_table(
            self.inverses.items(), self.symmetric, kind="declare_edge_inverses"
        )
        return self


class RetractEdgeInversesOp(ConfigBaseModel):
    """Withdraw declared inverses, addressed by relation name.

    A paired relation retracts its whole pair (either side names it); a
    symmetric relation retracts its own declaration. Refused while a native
    inverse still realizes a pair: that would leave the database maintaining a
    type the schema no longer names. Explicit inverse edges are ordinary edges
    and survive a retraction; so does ``directed: false``.
    """

    op: Literal["retract_edge_inverses"] = "retract_edge_inverses"
    relations: list[str] = PydanticField(
        ...,
        description="Relations whose declaration is withdrawn (either side of a pair).",
        min_length=1,
    )


class AddInverseEdgesOp(ConfigBaseModel):
    """Realize declared inverse pairs as explicit logical edges (portable to every backend).

    Edges are derived from the relation map: for each directed edge ``(S, T, r)``
    whose relation has a declared pair ``inv``, adds the logical edge
    ``(T, S, inv)`` unless it exists and its physical spec, and sets
    ``emit_inverse`` on the edge steps that write ``r``, so the same rows write
    both. No step is generated: the inverse is mirrored at assembly, after the
    relation is resolved, which covers every way a step can name its relation.
    A resource that already writes the inverse with a step of its own is left
    alone. The pair must be declared first (:class:`DeclareEdgeInversesOp`);
    this op never declares. A symmetric relation has no inverse edge to add --
    its edges are undirected. Refused for a relation whose inverse is native
    (:class:`SetNativeInversesOp`), since both would store the same fact.

    Withdraw with :class:`RemoveEdgesOp` on the inverse edges: removing an edge
    clears the ``emit_inverse`` flags that fed it.
    """

    op: Literal["add_inverse_edges"] = "add_inverse_edges"
    relations: list[str] | None = PydanticField(
        default=None,
        description=(
            "Paired relations whose edges get their inverse edge. Omitted: every "
            "relation in ``edge_config.inverses``, both sides."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique(self) -> AddInverseEdgesOp:
        if self.relations is not None and len(set(self.relations)) != len(
            self.relations
        ):
            raise ValueError("add_inverse_edges: relations must be unique")
        return self


class SetNativeInversesOp(ConfigBaseModel):
    """Have the database maintain declared inverses (TigerGraph ``WITH REVERSE_EDGE``).

    Physical, not logical: adds relations to, or removes them from,
    ``db_profile.native_inverses``. Keyed by relation, as TigerGraph is: the
    reverse type belongs to the edge type, which spans every ``(S, T)`` pair of
    the relation. The reverse type is named by the declared pair, so the pair
    must be declared (:class:`DeclareEdgeInversesOp`). Refused for symmetric
    relations, where explicit inverse edges exist, on undirected edges, for a
    relation stored under several physical names, and on non-TigerGraph
    profiles.
    """

    op: Literal["set_native_inverses"] = "set_native_inverses"
    relations: list[str] = PydanticField(
        ...,
        description="Paired relations whose declared inverse the database maintains.",
        min_length=1,
    )
    enabled: bool = PydanticField(
        default=True,
        description="``False`` withdraws the native inverse.",
    )

    @model_validator(mode="after")
    def _validate_unique(self) -> SetNativeInversesOp:
        if len(set(self.relations)) != len(self.relations):
            raise ValueError("set_native_inverses: relations must be unique")
        return self


class SetInverseEmissionOp(ConfigBaseModel):
    """Set or clear ``emit_inverse`` on edge steps, addressed by position.

    The ingestion half of a materialized inverse, as a primitive: which steps
    mirror the edges they write into the declared inverse. A step is addressed
    by resource and :class:`EdgeStepRef` (``at`` / ``step`` / ``link``), so the
    op says exactly which steps change and nothing is inferred at replay time.

    Enabling is refused where the flag could never write anything -- a step
    naming exactly one edge whose relation has no declared pair, is symmetric,
    or has no declared inverse edge. A step whose relation comes from the data
    is accepted; it mirrors per document what has a materialized inverse.
    """

    op: Literal["set_inverse_emission"] = "set_inverse_emission"
    steps: dict[str, list[EdgeStepRef]] = PydanticField(
        ...,
        description="Edge steps whose flag changes: ``{resource: [step ref, ...]}``.",
        min_length=1,
    )
    enabled: bool = PydanticField(
        default=True,
        description="``False`` clears the flag.",
    )

    @model_validator(mode="after")
    def _validate_steps(self) -> SetInverseEmissionOp:
        empty = sorted(name for name, refs in self.steps.items() if not refs)
        if empty:
            raise ValueError(f"set_inverse_emission: no steps given for {empty}")
        for name, refs in self.steps.items():
            keys = [ref.sort_key for ref in refs]
            if len(set(keys)) != len(keys):
                raise ValueError(
                    f"set_inverse_emission: steps of {name!r} must be unique"
                )
        return self
