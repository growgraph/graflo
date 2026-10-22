"""How a merged class is keyed, as the merge preview reports it.

One :class:`PreviewIdentity` per vertex group: the funnel branches in the
order a record tries them, every source that feeds each branch -- the raw
fields a resource reads, the transform and guard that turn them into the
branch value, the tag a local key carries -- and the member keys the new
identity demotes to lookups. Everything is read from the group's effective
declaration (the closed group, with the local keys merge adds itself), the
same one merge lowers, so nothing here restates a rule.
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
    LocalKeyBranch,
    LocalKeySource,
    VertexEquivalence,
    branch_fields,
    branch_id,
)

#: ``raw``: property fields every member must carry; ``derived``: an attribute
#: a transform computes per resource; ``local_key``: each resource's own key,
#: tagged.
BranchKind = Literal["raw", "derived", "local_key"]

#: ``natural``: one property branch keys the class; ``funnel``: several
#: branches, or a derived one, digested into ``digest_field``; ``carried``: no
#: identity declared, the members' agreeing key carried through.
IdentityMode = Literal["natural", "funnel", "carried"]

_DEFAULT_MODULE = DerivationSpec.model_fields["module"].default

_T = TypeVar("_T")


class BranchSource(ConfigBaseModel):
    """One thing that feeds a branch: a resource's derivation or local key, or a member that carries a property branch."""

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
    field: str | None = PydanticField(
        default=None, description="Raw field a local key reads."
    )
    tag: str | None = PydanticField(
        default=None, description="The tag a local key value carries; '' is untagged."
    )
    tag_omitted: bool = PydanticField(
        default=False, description="The tag was left out and defaults to the origin."
    )
    auto: bool = PydanticField(
        default=False, description="Merge added this local key: nothing declared one."
    )
    when: dict[str, Any] | None = PydanticField(
        default=None, description="The declared guard, as written."
    )
    derive_at: list[int] | None = PydanticField(
        default=None, description="The declared descend step path for the resource."
    )
    label: str = PydanticField(
        default="", description="One-line caption: transform and guard, or tag."
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
    sep: str | None = PydanticField(
        default=None,
        description="What joins a local key's tag and value; None for other kinds.",
    )


class PreviewDemotion(ConfigBaseModel):
    """A member key the new identity demotes to a lookup-only secondary."""

    side: Side
    member: str
    fields: list[str] = PydanticField(
        default_factory=list, description="The key fields, as the member spells them."
    )
    merged_fields: list[str] = PydanticField(
        default_factory=list, description="Their names on the merged class."
    )
    key_space: str = PydanticField(..., description="What names the demoted key.")
    tags: list[str] = PydanticField(
        default_factory=list, description="Every key space the op's tags name for it."
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
    funnel = declaration.has_derivation or len(identity) > 1
    auto = resolution.auto_key_sources.get(cluster.into or "", frozenset())
    origins = resolution.origins

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

    def local_key(branch: LocalKeyBranch, position: int) -> PreviewBranch:
        sources: list[BranchSource] = []
        for resource, entry in branch.local_key.items():
            base = resource_source(resource)
            for member, source in _entries(entry):
                is_auto = (resource, member) in auto
                origin = origins.get(base.side, base.side) if base.side else None
                sources.append(
                    base.model_copy(
                        update={
                            "member": member,
                            "field": source.field,
                            "tag": origin if source.tag_omitted else source.tag,
                            "tag_omitted": source.tag_omitted,
                            "auto": is_auto,
                            "when": _guard(source),
                            "label": local_key_label(
                                source, origin=origin, auto=is_auto
                            ),
                        }
                    )
                )
        return PreviewBranch(
            position=position,
            id=branch.name,
            kind="local_key",
            fields=[branch.name],
            sources=sources,
            sep=branch.sep,
        )

    branches: list[PreviewBranch] = []
    for position, branch in enumerate(identity, start=1):
        if isinstance(branch, DerivedBranch):
            branches.append(derived(branch, position))
        elif isinstance(branch, LocalKeyBranch):
            branches.append(local_key(branch, position))
        else:
            fields = branch_fields(branch)
            branches.append(
                PreviewBranch(
                    position=position,
                    id=branch_id(branch),
                    kind="raw",
                    fields=fields,
                    sources=_carriers(cluster, fields, sides, composite),
                )
            )
    return PreviewIdentity(
        mode="funnel" if funnel else "natural",
        branches=branches,
        derive=[derived(b, 0) for b in declaration.derive_attributes()],
        digest_field=declaration.digest_field if funnel else None,
        retire=declaration.retire,
        derive_at={r: list(p) for r, p in declaration.derive_at.items()},
        demoted=demoted,
    )


def derivation_label(spec: DerivationSpec) -> str:
    """``normalized_key(serial_number)``, with params, module and guard when set."""
    name = spec.foo if spec.module == _DEFAULT_MODULE else f"{spec.module}.{spec.foo}"
    args = ", ".join(spec.input)
    if spec.params:
        args += "; " + ", ".join(f"{k}={v!r}" for k, v in spec.params.items())
    return f"{name}({args}){_guard_label(spec.when)}"


def local_key_label(source: LocalKeySource, *, origin: str | None, auto: bool) -> str:
    """``tag maintenance``, ``tag left (default)``, ``untagged``; `` · auto`` when merge added it."""
    if source.tag_omitted:
        text = f"tag {origin} (default)" if origin else "tag (origin)"
    elif source.tag:
        text = f"tag {source.tag}"
    else:
        text = "untagged"
    if auto:
        text += " · auto"
    return text + _guard_label(source.when)


def _guard_label(when: Any) -> str:
    if when is None:
        return ""
    return f" when {when.field} in [{', '.join(when.values)}]"


def _guard(spec: DerivationSpec | LocalKeySource) -> dict[str, Any] | None:
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
