"""Identity branch declarations for a merged vertex."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any

from pydantic import Field as PydanticField
from pydantic import field_validator, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.steps.models import TransformGuardConfig
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel


class DerivationSpec(ConfigBaseModel):
    """How one resource derives a canonical attribute from its raw doc fields."""

    input: list[str] = PydanticField(
        ...,
        min_length=1,
        description=(
            "RAW source-doc field names fed to the function, in order. "
            "Documents keep their original keys after property renames, so "
            "canonical property names are usually wrong here."
        ),
    )
    module: str = PydanticField(
        default="graflo.util.transform",
        description="Module holding the derivation function.",
    )
    foo: str = PydanticField(
        default="normalized_key",
        description=(
            "Function name; called as ``foo(*values, **params)``. The default "
            "trims and casefolds one value."
        ),
    )
    params: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Keyword parameters for the function.",
    )
    when: TransformGuardConfig | None = PydanticField(
        default=None,
        description=(
            "Run the step only for documents whose RAW field holds one of the "
            "listed values (``{field, in}``). A merge derives this guard from "
            "how the resource produces the class; one set here replaces it. "
            "Not allowed on a spec keyed by member: the member decides."
        ),
    )


class LocalKeySource(ConfigBaseModel):
    """Where one resource's side-local key comes from, and its namespace tag.

    The tag is what keeps records of different sources apart once they fail to
    fuse: ``f2`` from one source and ``f2`` from another are different
    entities, and ``a:f2`` / ``b:f2`` say so. Omitted, it is the origin of the
    resource's side (see :attr:`MergeManifestsOp.origins`). An explicit
    ``tag=None`` (stored as ``""``, the neutral element, so it survives
    serialization) keeps the raw value as the local key with no separator —
    the author's claim that the values are already unique across every source
    of the class (UUIDs, IRIs, ids the source itself prefixes).
    """

    field: str = PydanticField(
        ...,
        description="RAW doc field carrying the side-local key.",
    )
    tag: str | None = PydanticField(
        default=None,
        description=(
            "Namespace tag: tag 'a' turns 'f2' into 'a:f2'. Omitted, the "
            'origin of the resource\'s side. ``null`` or ``""`` keeps the raw '
            "value, no separator — only for values already unique across every "
            "source of the class."
        ),
    )
    when: TransformGuardConfig | None = PydanticField(
        default=None,
        description=(
            "Same as :attr:`DerivationSpec.when`: an explicit guard replacing "
            "the one the merge derives. Not allowed on a source keyed by member."
        ),
    )

    @field_validator("tag", mode="before")
    @classmethod
    def _none_is_the_empty_tag(cls, value: Any) -> Any:
        return "" if value is None else value


def _refuse_member_keyed_guards(
    kind: str, sources: Mapping[str, Any], label: str
) -> None:
    """A spec keyed by member may not carry ``when``: the member decides."""
    for resource, entry in sources.items():
        if not isinstance(entry, dict):
            continue
        if not entry:
            raise ValueError(
                f"{kind} {label!r}: resource {resource!r} keys its sources by "
                "member but names none"
            )
        guarded = sorted(m for m, spec in entry.items() if spec.when is not None)
        if guarded:
            raise ValueError(
                f"{kind} {label!r}: member-keyed sources for resource "
                f"{resource!r} set `when` on {guarded}; the member already "
                "decides, and the guard is derived from how the resource "
                "produces it"
            )


class DerivedBranch(ConfigBaseModel):
    """A funnel branch over an attribute each source derives from its own columns.

    ``name`` is the canonical attribute the branch keys on: the digest's
    input, not where the digest is stored (that is
    :attr:`VertexEquivalence.digest_field`). ``sources`` is
    keyed by resource, because derivation inputs are that resource's raw
    column names. An entry is either

    * one :class:`DerivationSpec` — the resource derives the attribute the
      same way for every record of the class it produces; or
    * a dict keyed by member class — the resource produces several members
      and which one a record *is* decides the derivation. A member is keyed by
      its own name on its side or by its canonical one.

    The merge reads how the resource produces the class on its side and guards
    each step with ``when``: a member-keyed spec on the discriminator values
    that route onto its member, a single spec behind a ``vertex_router`` on
    the values that route onto the class, and nothing for a plain ``vertex``
    step. An explicit :attr:`DerivationSpec.when` replaces the derived guard.
    """

    name: str = PydanticField(
        ...,
        description=(
            "Derived attribute this branch digests; its funnel branch id. Not "
            "where the key is stored: see `VertexEquivalence.digest_field`."
        ),
    )
    sources: dict[str, DerivationSpec | dict[str, DerivationSpec]] = PydanticField(
        ...,
        min_length=1,
        description=(
            "Per-resource derivation: ``{resource: spec}``, or ``{resource: "
            "{member_class: spec}}`` when the member a document is must decide "
            "the derivation."
        ),
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> DerivedBranch:
        _refuse_member_keyed_guards("derived branch", self.sources, self.name)
        return self

    def specs_for(self, resource: str) -> list[DerivationSpec]:
        """Derivations *resource* contributes to this branch, in order."""
        spec = self.sources.get(resource)
        if spec is None:
            return []
        return [spec] if isinstance(spec, DerivationSpec) else list(spec.values())

    def members_for(self, resource: str) -> list[str] | None:
        """Member classes keying *resource*'s specs, or ``None`` if unkeyed."""
        spec = self.sources.get(resource)
        return list(spec) if isinstance(spec, dict) else None


class LocalKeyBranch(ConfigBaseModel):
    """The last funnel branch: each source's own key behind a namespace tag.

    A record that completes no earlier branch keys on this one, so it is still
    written — as its own vertex, not joined with records of another source.
    ``local_key`` maps each resource to the column carrying its own key, or,
    like :attr:`DerivedBranch.sources`, to one such source per member class.
    """

    local_key: dict[str, LocalKeySource | dict[str, LocalKeySource]] = PydanticField(
        ...,
        min_length=1,
        description=(
            "Per-resource local-key wiring: ``{resource: source}``, or "
            "``{resource: {member_class: source}}`` when the member decides."
        ),
    )
    name: str = PydanticField(
        default="local_key",
        description="Canonical fallback property name on the class.",
    )
    sep: str = PydanticField(
        default=":",
        description="Separator between tag and key.",
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> LocalKeyBranch:
        _refuse_member_keyed_guards("local_key branch", self.local_key, self.name)
        return self

    @property
    def sources(self) -> dict[str, LocalKeySource | dict[str, LocalKeySource]]:
        """The per-resource wiring, under the name :class:`DerivedBranch` uses."""
        return self.local_key

    def sources_for(self, resource: str) -> list[LocalKeySource]:
        """Local-key sources *resource* contributes, in order."""
        source = self.local_key.get(resource)
        if source is None:
            return []
        return [source] if isinstance(source, LocalKeySource) else list(source.values())

    def members_for(self, resource: str) -> list[str] | None:
        """Member classes keying *resource*'s sources, or ``None`` if unkeyed."""
        source = self.local_key.get(resource)
        return list(source) if isinstance(source, dict) else None


#: A funnel branch over canonical properties the members already carry: one
#: name, or a composite ``[a, b]`` whose fields must all be present.
RawBranch = str | list[str]


#: One entry of :attr:`VertexEquivalence.identity`.
IdentityBranchDecl = RawBranch | DerivedBranch | LocalKeyBranch


def branch_fields(branch: IdentityBranchDecl) -> list[str]:
    """The canonical fields *branch* keys on."""
    if isinstance(branch, str):
        return [branch]
    if isinstance(branch, list):
        return list(branch)
    return [branch.name]


def branch_id(branch: IdentityBranchDecl) -> str:
    """The funnel branch id *branch* lowers to — part of the digest payload."""
    return "_".join(branch_fields(branch))


def identity_branches_funnel(branches: Sequence[IdentityBranchDecl]) -> IdentityFunnel:
    """The funnel *branches* declare, in declared order."""
    return IdentityFunnel(
        branches=[
            IdentityBranch(id=branch_id(branch), fields=branch_fields(branch))
            for branch in branches
        ]
    )


def check_identity_branches(
    branches: Sequence[IdentityBranchDecl],
    *,
    label: str,
    derived: Collection[str] = (),
) -> None:
    """Refuse a branch list no funnel can be built from, naming *label*.

    Non-empty; no empty composite; at most one ``local_key``, and it last --
    every record completes it, so a branch after it never fires; unique
    branch ids, since they take part in the digest; and no derived name that
    a property branch keys on, which the derivation would overwrite.

    *derived* names the attributes a ``derive`` block computes. A name or
    composite branch may key on them, but a composite is all derived or all
    properties, every derived attribute is keyed on, and none shares a name
    with a derived or local-key branch.
    """
    if not branches:
        raise ValueError(
            f"{label}: identity lists no branch; omit it to carry the members' "
            "shared key through"
        )
    for branch in branches:
        if isinstance(branch, list) and not branch:
            raise ValueError(f"{label}: an identity branch is empty")
    local = [i for i, b in enumerate(branches) if isinstance(b, LocalKeyBranch)]
    if len(local) > 1:
        raise ValueError(f"{label}: identity has more than one local_key")
    if local and local[0] != len(branches) - 1:
        raise ValueError(
            f"{label}: the local_key branch must be the last one; a record keys "
            "on its first complete branch, and every record completes the local key"
        )
    ids = [branch_id(branch) for branch in branches]
    repeated = sorted({i for i in ids if ids.count(i) > 1})
    if repeated:
        raise ValueError(f"{label}: identity repeats the branches {repeated}")
    raw_fields = {
        f for b in branches if isinstance(b, str | list) for f in branch_fields(b)
    }
    stepped = {
        b.name for b in branches if isinstance(b, DerivedBranch | LocalKeyBranch)
    }
    shadowing = sorted(stepped & raw_fields)
    if shadowing:
        raise ValueError(
            f"{label}: derived branches {shadowing} are named like properties "
            "another branch keys on; the derivation would overwrite them"
        )
    attributes = set(derived)
    clashing = sorted(attributes & stepped)
    if clashing:
        raise ValueError(
            f"{label}: derive and the identity branches both name {clashing}; "
            "two derivations would write one attribute"
        )
    for branch in branches:
        if not isinstance(branch, list):
            continue
        properties = [f for f in branch if f not in attributes]
        if properties and len(properties) < len(branch):
            raise ValueError(
                f"{label}: branch {branch} mixes derived attributes with the "
                f"properties {properties}; derive {properties} too, e.g. with "
                "`normalized_key`, so every part of the branch is derived"
            )
    unused = sorted(attributes - raw_fields)
    if unused:
        raise ValueError(
            f"{label}: derive computes {unused}, but no identity branch keys "
            "on them; list them in a branch or drop them"
        )
