"""Equivalence declarations that align left and right members in a merge."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field as PydanticField
from pydantic import field_validator, model_validator

from graflo.architecture.base import ConfigBaseModel

from .branches import (
    DerivationSpec,
    DerivedBranch,
    IdentityBranchDecl,
    LocalKeyBranch,
    branch_fields,
    check_identity_branches,
)


def _describe_branch_shapes() -> str:
    return (
        "a branch is a property name, a list of names, `{name, sources}`, or "
        "`{local_key: {resource: {field, tag}}}`"
    )


def _member_list(value: str | list[str]) -> list[str]:
    """Normalize a ``str | list[str]`` equivalence side to a list of members."""
    return [value] if isinstance(value, str) else list(value)


class PropertyEquivalence(ConfigBaseModel):
    """Align a property from the left and/or right member(s) onto a canonical name.

    At least one of ``left`` / ``right`` must be set. A bare string applies to
    every member declared on that side of the owning
    :class:`VertexEquivalence`; a ``{member: field}`` dict maps per member,
    for when members are not aligned under the same source field name.

    Exact-name matches do **not** need a :class:`PropertyEquivalence`: after
    boundary rename, ``merge_vertex_models`` unions fields by spelling, so a
    property present under the same name on every member fuses for free.
    Declare an equivalence only to rename or to pick a different ``into``; the
    merged key is declared on the :class:`VertexEquivalence`, the merged type
    in :attr:`MergeManifestsOp.field_types`.
    """

    left: str | dict[str, str] | None = PydanticField(
        default=None,
        description=(
            "Field name on the left member(s): a bare string applies to every "
            "left member of the owning equivalence, a ``{member: field}`` dict "
            "maps per member."
        ),
    )
    right: str | dict[str, str] | None = PydanticField(
        default=None,
        description="Same shape as ``left``, for the right member(s).",
    )
    into: str = PydanticField(
        ...,
        description="Canonical property name on the merged vertex.",
    )

    @model_validator(mode="after")
    def _require_side(self) -> PropertyEquivalence:
        if self.left is None and self.right is None:
            raise ValueError(
                "PropertyEquivalence requires at least one of left or right"
            )
        for side, spec in (("left", self.left), ("right", self.right)):
            if isinstance(spec, dict) and not spec:
                raise ValueError(
                    f"PropertyEquivalence: {side} is an empty per-member map"
                )
        return self


class VertexEquivalence(ConfigBaseModel):
    """Collapse one or more left classes and one or more right classes into one.

    GraFlo applies this map deterministically; it does not infer semantic
    matches. ``left`` / ``right`` accept a bare class name (a 1-1 equivalence)
    or a list (an n-ary cluster): ``{Company, Shop} ~ {Org, Branch} ->
    Company``. A member may also be spelled by the canonical name a vocabulary
    gives it, which stands for every class the vocabulary sends there.

    The group is everything the equivalence links: its members, and every
    class a vocabulary merges with one of them. Per-member maps (property
    equivalences, member-keyed identity sources) may name any class of the
    group by its own name.

    Properties with the same spelling on every member after alignment fuse by
    exact name without an entry in ``properties`` — list only renames.

    ``identity`` is the merged key, as ordered funnel branches in canonical
    names. A branch is a property the members carry (a name, or a composite
    ``[a, b]``), a :class:`DerivedBranch` each source computes from its own
    columns, or a :class:`LocalKeyBranch` (the tagged fallback, last). One
    property branch keys the class on that natural key; anything else keys it
    on a funnel, where a record keys on its first complete branch. A funnel's
    digest is stored in ``digest_field`` (``id`` by default).

    ``derive`` declares attributes each source computes, without making each
    one a branch: a name or composite branch in ``identity`` keys on them, so
    ``[host_key, group_key]`` fires only when both are derived and the digest
    joins them. A ``DerivedBranch`` ``{name, sources}`` is the same as
    ``derive: {name: sources}`` with the branch ``name``.
    """

    left: str | list[str] = PydanticField(
        ..., description="One or more left-manifest vertex type names."
    )
    right: str | list[str] = PydanticField(
        ..., description="One or more right-manifest vertex type names."
    )
    into: str | None = PydanticField(
        default=None,
        description=(
            "Merged vertex type name: any name, including one either side "
            "uses or vacates. Never translated by a vocabulary; when it differs "
            "from the vocabulary's name for the group it wins. Omitted, the "
            "name comes from the vocabulary, or from the one spelling every "
            "member shares."
        ),
    )
    properties: list[PropertyEquivalence] = PydanticField(
        default_factory=list,
        description="Property alignment map applied before the vertex merge.",
    )
    identity: list[IdentityBranchDecl] | None = PydanticField(
        default=None,
        description=(
            "The merged key, as ordered funnel branches in canonical names: a "
            "property the members carry, a composite ``[a, b]``, a derived "
            "branch ``{name, sources}``, or ``{local_key: ...}`` (last). One "
            "property branch is a natural key; anything else is a funnel. When "
            "unset, identity is carried through only if every member agrees; "
            "disagreement raises `MergeIdentityError`."
        ),
    )
    derive: dict[str, dict[str, DerivationSpec | dict[str, DerivationSpec]]] | None = (
        PydanticField(
            default=None,
            description=(
                "Attributes each source derives from its own columns, keyed by "
                "attribute name; each entry has the shape of `DerivedBranch."
                "sources`. Identity branches key on them by name: a composite "
                "``[a, b]`` of derived attributes fires only when every part is "
                "derived. Requires `identity`."
            ),
        )
    )
    digest_field: str = PydanticField(
        default="id",
        min_length=1,
        description=(
            "Property the merged funnel's digest is stored in -- the class's "
            "identity field. Only for a funnel `identity`; it must not be a "
            "branch field. Set it when a member carries a real `id`."
        ),
    )
    derive_at: dict[str, list[int]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-resource pipeline level the derived and local-key branches "
            "derive at, as ``descend`` step indices. Omitted resources resolve "
            "to the single level producing the class (for member-keyed "
            "sources: the level producing the member on its side); supply a "
            "path only when a resource produces it at more than one level."
        ),
    )
    retire: Literal["demote", "keep"] = PydanticField(
        default="demote",
        description=(
            "What becomes of each member's pre-merge key once `identity` "
            "re-keys the merged class. `demote` keeps each as a lookup-only "
            "secondary identity on `into`, and points edge steps of resources "
            "that only reference a member at it; `keep` leaves the fields as "
            "plain properties. Unused while the merged class keeps its "
            "members' shared key."
        ),
    )
    allow: list[Literal["self_relations", "observation_fusion"]] = PydanticField(
        default_factory=list,
        description=(
            "Consequences of this merge it accepts. `self_relations`: an edge "
            "between two members becomes an edge from the class to itself. "
            "`observation_fusion`: two members produced in one accumulator "
            "slot (same pipeline level and `role`, or both bare) fuse into one "
            "node."
        ),
    )

    @field_validator("identity", mode="before")
    @classmethod
    def _branch_shapes(cls, value: Any) -> Any:
        """Name the four branch shapes instead of pydantic's union error."""
        if not isinstance(value, list):
            return value
        for entry in value:
            if isinstance(entry, str | DerivedBranch | LocalKeyBranch):
                continue
            if isinstance(entry, list) and all(isinstance(f, str) for f in entry):
                continue
            if isinstance(entry, dict) and ("sources" in entry or "local_key" in entry):
                continue
            raise ValueError(
                f"VertexEquivalence: identity branch {entry!r} has no known "
                f"shape; {_describe_branch_shapes()}"
            )
        return value

    def raw_branches(self) -> list[tuple[str, ...]]:
        """The property branches of ``identity``, each as its field tuple."""
        return [
            tuple(branch_fields(branch))
            for branch in self.identity or []
            if isinstance(branch, str | list)
        ]

    def derived_branches(self) -> list[DerivedBranch]:
        """The derived branches of ``identity``, in priority order."""
        return [b for b in self.identity or [] if isinstance(b, DerivedBranch)]

    def derive_attributes(self) -> list[DerivedBranch]:
        """The ``derive`` entries, each as the derivation of one attribute."""
        return [
            DerivedBranch(name=name, sources=sources)
            for name, sources in (self.derive or {}).items()
        ]

    def derivations(self) -> list[DerivedBranch]:
        """Every derived attribute: the ``derive`` entries, then derived branches."""
        return [*self.derive_attributes(), *self.derived_branches()]

    def local_key_branch(self) -> LocalKeyBranch | None:
        """The ``local_key`` branch of ``identity``, if declared."""
        return next(
            (b for b in self.identity or [] if isinstance(b, LocalKeyBranch)), None
        )

    @property
    def has_derivation(self) -> bool:
        """Whether the key needs pipeline steps: ``derive``, a derived or local-key branch."""
        return bool(self.derive) or any(
            isinstance(b, DerivedBranch | LocalKeyBranch) for b in self.identity or []
        )

    @property
    def left_members(self) -> list[str]:
        return _member_list(self.left)

    @property
    def right_members(self) -> list[str]:
        return _member_list(self.right)

    def members(self, side: Literal["left", "right"]) -> list[str]:
        return self.left_members if side == "left" else self.right_members

    def property_maps(
        self, side: Literal["left", "right"]
    ) -> dict[str, dict[str, str]]:
        """``{member: {old_field: into_field}}`` for *side*, bare strings expanded."""
        member_names = self.members(side)
        out: dict[str, dict[str, str]] = {}
        for pe in self.properties:
            spec = pe.left if side == "left" else pe.right
            if spec is None:
                continue
            per_member = (
                dict.fromkeys(member_names, spec) if isinstance(spec, str) else spec
            )
            for member, old in per_member.items():
                if old == pe.into:
                    continue
                bucket = out.setdefault(member, {})
                existing = bucket.get(old)
                if existing is not None and existing != pe.into:
                    raise ValueError(
                        f"VertexEquivalence: {side}:{member}.{old!r} would rename "
                        f"to both {existing!r} and {pe.into!r}"
                    )
                bucket[old] = pe.into
        return out

    @model_validator(mode="after")
    def _validate_members(self) -> VertexEquivalence:
        for side, members in (
            ("left", self.left_members),
            ("right", self.right_members),
        ):
            if not members:
                raise ValueError(
                    f"VertexEquivalence: {side} must name at least one class"
                )
            if len(members) != len(set(members)):
                raise ValueError(
                    f"VertexEquivalence: {side} lists a class more than once: {members}"
                )
        if len(self.allow) != len(set(self.allow)):
            raise ValueError(
                f"VertexEquivalence: allow lists a value twice: {self.allow}"
            )
        # Per-member property maps may name any class of the group, which only
        # the manifests and the vocabulary decide: checked at resolution.
        return self

    @model_validator(mode="after")
    def _validate_identity(self) -> VertexEquivalence:
        if self.derive is not None:
            if not self.derive:
                raise ValueError(
                    "VertexEquivalence: derive lists no attribute; omit it"
                )
            if self.identity is None:
                raise ValueError(
                    f"VertexEquivalence: derive computes {sorted(self.derive)}, "
                    "but no identity keys on them; declare identity"
                )
            # Each entry is validated as a derived branch's sources are.
            self.derive_attributes()
        if self.identity is None:
            if self.derive_at:
                raise ValueError(
                    "VertexEquivalence: derive_at is set but no identity branch "
                    "derives anything"
                )
            if self.digest_field != "id":
                raise ValueError(
                    f"VertexEquivalence: digest_field {self.digest_field!r} is "
                    "set but no identity is declared; it names where a funnel "
                    "identity's digest is stored"
                )
            return self
        check_identity_branches(
            self.identity, label="VertexEquivalence", derived=list(self.derive or {})
        )
        if self.derive_at and not self.has_derivation:
            raise ValueError(
                "VertexEquivalence: derive_at is set but no identity branch "
                "derives anything"
            )
        if self.is_natural_key:
            if self.digest_field != "id":
                raise ValueError(
                    f"VertexEquivalence: digest_field {self.digest_field!r} is "
                    f"set, but identity {self.identity!r} is a natural key, "
                    "stored in its own fields; digest_field applies only to a "
                    "funnel"
                )
            return self
        branch_names = {f for b in self.identity for f in branch_fields(b)}
        if self.digest_field in branch_names:
            raise ValueError(
                f"VertexEquivalence: digest_field {self.digest_field!r} is also "
                "an identity branch field. A branch names what the digest is "
                "computed from, not where it is stored, and the digest would "
                "replace that branch's input; store the digest in another field"
            )
        return self

    @property
    def is_natural_key(self) -> bool:
        """Whether ``identity`` is one property branch, kept as a natural key."""
        return self.identity is not None and (
            len(self.identity) == 1 and not self.has_derivation
        )


class RelationEquivalence(ConfigBaseModel):
    """Collapse one or more left relations and one or more right relations onto one name.

    Shares the ``left`` / ``right`` n-ary shape and the naming rules of
    :class:`VertexEquivalence`.
    """

    left: str | list[str] = PydanticField(
        ..., description="One or more left relation names."
    )
    right: str | list[str] = PydanticField(
        ..., description="One or more right relation names."
    )
    into: str | None = PydanticField(
        default=None,
        description=(
            "Merged relation name, never translated by a vocabulary. Omitted, "
            "the name comes from the vocabulary, or from the one spelling "
            "every member shares."
        ),
    )

    @property
    def left_members(self) -> list[str]:
        return _member_list(self.left)

    @property
    def right_members(self) -> list[str]:
        return _member_list(self.right)

    def members(self, side: Literal["left", "right"]) -> list[str]:
        return self.left_members if side == "left" else self.right_members

    @model_validator(mode="after")
    def _validate_members(self) -> RelationEquivalence:
        for side, members in (
            ("left", self.left_members),
            ("right", self.right_members),
        ):
            if not members:
                raise ValueError(
                    f"RelationEquivalence: {side} must name at least one relation"
                )
            if len(members) != len(set(members)):
                raise ValueError(
                    f"RelationEquivalence: {side} lists a relation more than once: {members}"
                )
        return self
