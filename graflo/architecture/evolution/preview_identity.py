"""How a merged class is keyed, as the merge preview reports it.

One :class:`PreviewIdentity` per vertex group: the funnel branches in the
order a record tries them, every source that feeds each branch -- the raw
fields a resource reads, the transform and guard that turn them into the
branch value, the member whose own key a fallback branch is -- and the member
keys the new identity demotes to lookups. Everything is read from the group's
effective declaration (the closed group, with the fallback branches appended),
the same one merge lowers, so nothing here restates a rule.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, TypeVar

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

from .canonical import ClusterResolution
from .equivalence import Cluster, Side
from .ops import (
    CanonicalizeOp,
    DerivationSpec,
    DerivedBranch,
    VertexEquivalence,
    branch_fields,
    branch_id,
)

#: ``raw``: property fields members carry; ``derived``: an attribute a
#: transform computes per resource; ``own_key``: one member's own key, kept on
#: the merged class, which ``fallback: own_key`` appends.
BranchKind = Literal["raw", "derived", "own_key"]

#: ``natural``: one property branch keys the class; ``funnel``: several
#: branches, or a derived one, digested into ``digest_field``; ``carried``: no
#: identity declared, the members' agreeing key carried through.
IdentityMode = Literal["natural", "funnel", "carried"]

_DEFAULT_MODULE = DerivationSpec.model_fields["module"].default

_T = TypeVar("_T")


class BranchSource(ConfigBaseModel):
    """One thing that feeds a branch: a resource's derivation, or a member that carries a property branch or owns a fallback key."""

    resource: str | None = PydanticField(
        default=None, description="Union resource name; None for a property branch."
    )
    side: Side | None = PydanticField(
        default=None, description="The side the resource or member comes from."
    )
    exists: bool = PydanticField(
        default=True, description="False when no side has a resource of this name."
    )
    member: str | None = PydanticField(
        default=None,
        description="The member a member-keyed entry is for, or the member carrying a property branch.",
    )
    produces: list[str] = PydanticField(
        default_factory=list,
        description="Members of the group the resource produces on its side.",
    )
    input: list[str] = PydanticField(
        default_factory=list, description="Raw fields a derivation reads."
    )
    module: str | None = PydanticField(default=None, description="Transform module.")
    foo: str | None = PydanticField(default=None, description="Transform function.")
    params: dict[str, Any] = PydanticField(
        default_factory=dict, description="Transform keyword arguments."
    )
    when: dict[str, Any] | None = PydanticField(
        default=None, description="The declared guard, as written."
    )
    derive_at: list[int] | None = PydanticField(
        default=None, description="The declared descend step path for the resource."
    )
    label: str = PydanticField(
        default="",
        description="One-line caption: transform and guard, or the key's owner.",
    )


class PreviewBranch(ConfigBaseModel):
    """One funnel branch, or one ``derive`` attribute."""

    position: int = PydanticField(
        ..., description="1-based order a record tries it in; 0 for a `derive` entry."
    )
    id: str = PydanticField(..., description="Branch id, as the funnel names it.")
    kind: BranchKind = PydanticField(..., description="What the branch keys on.")
    fields: list[str] = PydanticField(
        default_factory=list, description="Merged attributes the branch reads."
    )
    sources: list[BranchSource] = PydanticField(default_factory=list)


class PreviewDemotion(ConfigBaseModel):
    """A member key the new identity demotes to a lookup-only secondary."""

    side: Side
    member: str
    fields: list[str] = PydanticField(
        default_factory=list,
        description="The key fields, as the member spells them after its vocabulary renames.",
    )
    merged_fields: list[str] = PydanticField(
        default_factory=list, description="Their names on the merged class."
    )
    key_space: str = PydanticField(..., description="What names the demoted key.")
    tags: list[str] = PydanticField(
        default_factory=list,
        description="Every key space the op's `key_spaces` name for it.",
    )


class PreviewIdentity(ConfigBaseModel):
    """How a merged class is keyed, and where every key value comes from."""

    mode: IdentityMode
    branches: list[PreviewBranch] = PydanticField(default_factory=list)
    derive: list[PreviewBranch] = PydanticField(
        default_factory=list,
        description="`derive` entries: attributes a branch keys on, computed per resource.",
    )
    digest_field: str | None = PydanticField(
        default=None, description="Where a funnel stores its digest."
    )
    fallback: str | None = PydanticField(
        default=None,
        description="`own_key` when each member's own key is a last branch.",
    )
    retire: str | None = PydanticField(
        default=None, description="What happens to each member's own key."
    )
    derive_at: dict[str, list[int]] = PydanticField(default_factory=dict)
    demoted: list[PreviewDemotion] = PydanticField(default_factory=list)

    def role_of(self, attribute: str) -> list[int]:
        """Positions of the branches that key on merged *attribute*."""
        return [b.position for b in self.branches if attribute in b.fields]


def identity_block(
    cluster: Cluster,
    resolution: ClusterResolution,
    sides: Mapping[str, GraphManifest],
    composite: Mapping[Side, CanonicalizeOp],
) -> PreviewIdentity:
    """The :class:`PreviewIdentity` of one vertex group.

    Args:
        cluster: The group, with its effective declaration.
        resolution: The naming resolution it came from.
        sides: Both manifests, resources renamed as the union names them.
        composite: Each side's composite relabel.
    """
    from .merge import _members_produced

    declaration = cluster.declaration
    assert isinstance(declaration, VertexEquivalence)
    identity = declaration.identity
    demoted = _demotions(cluster, resolution)
    if identity is None:
        return PreviewIdentity(mode="carried", demoted=demoted)
    funnel = not declaration.is_natural_key
    owners = {
        fields: (side, member)
        for side, member, fields in resolution.fallback_branches.get(
            cluster.into or "", ()
        )
    }

    def resource_source(resource: str) -> BranchSource:
        produced = _members_produced(resource, cluster, sides)
        if produced is None:
            return BranchSource(resource=resource, exists=False)
        side, members = produced
        return BranchSource(
            resource=resource,
            side=side,
            produces=members,
            derive_at=declaration.derive_at.get(resource),
        )

    def derived(branch: DerivedBranch, position: int) -> PreviewBranch:
        sources: list[BranchSource] = []
        for resource, entry in branch.sources.items():
            base = resource_source(resource)
            for member, spec in _entries(entry):
                sources.append(
                    base.model_copy(
                        update={
                            "member": member,
                            "input": list(spec.input),
                            "module": spec.module,
                            "foo": spec.foo,
                            "params": dict(spec.params),
                            "when": _guard(spec),
                            "label": derivation_label(spec),
                        }
                    )
                )
        return PreviewBranch(
            position=position,
            id=branch.name,
            kind="derived",
            fields=[branch.name],
            sources=sources,
        )

    branches: list[PreviewBranch] = []
    for position, branch in enumerate(identity, start=1):
        if isinstance(branch, DerivedBranch):
            branches.append(derived(branch, position))
            continue
        fields = branch_fields(branch)
        owner = owners.get(tuple(fields))
        branches.append(
            PreviewBranch(
                position=position,
                id=branch_id(branch),
                kind="raw" if owner is None else "own_key",
                fields=fields,
                sources=(
                    _carriers(cluster, fields, sides, composite)
                    if owner is None
                    else [
                        BranchSource(
                            side=owner[0],
                            member=owner[1],
                            label=f"own key of {owner[0]}:{owner[1]}",
                        )
                    ]
                ),
            )
        )
    return PreviewIdentity(
        mode="funnel" if funnel else "natural",
        branches=branches,
        derive=[derived(b, 0) for b in declaration.derive_attributes()],
        digest_field=declaration.digest_field if funnel else None,
        fallback=declaration.fallback,
        retire=declaration.retire,
        derive_at={r: list(p) for r, p in declaration.derive_at.items()},
        demoted=demoted,
    )


def declared_identity(declaration: VertexEquivalence) -> PreviewIdentity:
    """The identity *declaration* states, read without the manifests.

    What :func:`identity_block` reports minus everything only the manifests
    settle: no resource has a side, no member carries a property branch,
    and neither demoted keys nor the fallback's own-key branches are listed
    (``fallback`` says whether there are any).
    """
    identity = declaration.identity
    if identity is None:
        return PreviewIdentity(mode="carried")

    def resource(name: str) -> BranchSource:
        return BranchSource(resource=name, derive_at=declaration.derive_at.get(name))

    def derived(branch: DerivedBranch, position: int) -> PreviewBranch:
        return PreviewBranch(
            position=position,
            id=branch.name,
            kind="derived",
            fields=[branch.name],
            sources=[
                resource(name).model_copy(
                    update={
                        "member": member,
                        "input": list(spec.input),
                        "module": spec.module,
                        "foo": spec.foo,
                        "params": dict(spec.params),
                        "when": _guard(spec),
                        "label": derivation_label(spec),
                    }
                )
                for name, entry in branch.sources.items()
                for member, spec in _entries(entry)
            ],
        )

    branches: list[PreviewBranch] = []
    for position, branch in enumerate(identity, start=1):
        if isinstance(branch, DerivedBranch):
            branches.append(derived(branch, position))
        else:
            branches.append(
                PreviewBranch(
                    position=position,
                    id=branch_id(branch),
                    kind="raw",
                    fields=branch_fields(branch),
                )
            )
    funnel = not declaration.is_natural_key
    return PreviewIdentity(
        mode="funnel" if funnel else "natural",
        branches=branches,
        derive=[derived(b, 0) for b in declaration.derive_attributes()],
        digest_field=declaration.digest_field if funnel else None,
        fallback=declaration.fallback,
        retire=declaration.retire,
        derive_at={r: list(p) for r, p in declaration.derive_at.items()},
    )


def derivation_label(spec: DerivationSpec) -> str:
    """``normalized_key(serial_number)``, with params, module and guard when set."""
    name = spec.foo if spec.module == _DEFAULT_MODULE else f"{spec.module}.{spec.foo}"
    args = ", ".join(spec.input)
    if spec.params:
        args += "; " + ", ".join(f"{k}={v!r}" for k, v in spec.params.items())
    return f"{name}({args}){_guard_label(spec.when)}"


def _guard_label(when: Any) -> str:
    if when is None:
        return ""
    return f" when {when.field} in [{', '.join(when.values)}]"


def _guard(spec: DerivationSpec) -> dict[str, Any] | None:
    return spec.when.to_dict() if spec.when is not None else None


def _entries(entry: _T | dict[str, _T]) -> list[tuple[str | None, _T]]:
    """A source entry as ``(member, spec)`` pairs; ``None`` for an unkeyed one."""
    if isinstance(entry, dict):
        return list(entry.items())
    return [(None, entry)]


def _carriers(
    cluster: Cluster,
    fields: list[str],
    sides: Mapping[str, GraphManifest],
    composite: Mapping[Side, CanonicalizeOp],
) -> list[BranchSource]:
    """Members whose renamed properties include every field of a property branch."""
    carriers: list[BranchSource] = []
    for side in ("left", "right"):
        schema = sides[side].graph_schema
        if schema is None:
            continue
        vertex_config = schema.core_schema.vertex_config
        renames = composite[side].properties if side in composite else {}
        for member in cluster.members(side):
            if member not in vertex_config.vertex_set:
                continue
            rename = renames.get(member, {})
            names = {
                rename.get(p.name, p.name) for p in vertex_config[member].properties
            }
            if set(fields) <= names:
                carriers.append(BranchSource(side=side, member=member))
    return carriers


def _demotions(
    cluster: Cluster, resolution: ClusterResolution
) -> list[PreviewDemotion]:
    """Each member key the group demotes, with the names the demotion gave it.

    A key field a property branch names keeps its name, so ``merged_fields``
    lists only the fields the demotion renamed.
    """
    demoted: list[PreviewDemotion] = []
    for side in ("left", "right"):
        for member in cluster.members(side):
            key = resolution.demoted_keys.get((side, member))
            if key is None:
                continue
            space = resolution.key_spaces.get((side, member), side)
            merged = [
                writes[-1][1]
                for (s, m, _own), writes in resolution.property_layers.items()
                if s == side and m == member and writes[-1][0] == "demotion"
            ]
            demoted.append(
                PreviewDemotion(
                    side=side,
                    member=member,
                    fields=list(key),
                    merged_fields=merged,
                    key_space=space,
                    tags=list(resolution.key_tags.get((side, member), ())),
                )
            )
    return demoted
