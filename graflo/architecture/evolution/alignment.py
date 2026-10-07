"""Derived identity: lower a merged class's identity branches to fundamental ops.

A :class:`~graflo.architecture.evolution.ops.VertexEquivalence` declares the
merged key as ordered funnel branches. A branch over properties the members
already carry needs nothing but the funnel. A
:class:`~graflo.architecture.evolution.ops.DerivedBranch` or
:class:`~graflo.architecture.evolution.ops.LocalKeyBranch` needs each source to
compute its attribute, and :func:`identity_to_ops` emits only fundamental ops
for that —

1. ``AddVertexPropertiesOp`` — declare the derived attributes on the class;
2. ``AddResourceTransformsOp`` — per-resource derivation steps
   (normalization, local-key namespacing) appended to the pipelines;
3. ``EnsureExtractedFieldsOp`` — when a producing router restricts what it
   extracts;
4. ``ReplaceIdentityOp`` — a priority funnel over the branches, in declared
   order. ``merge_manifests`` then demotes each member's own key to a
   secondary identity against that final funnel.

The division of labor is deliberate: **a primary identity is a property of the
class**, so the funnel references only canonical attributes; *how* a given
source populates them is resource knowledge and lives in that resource's
pipeline. Derivation inputs are RAW source-doc field names — property renames
rewrite ``vertex.from`` maps so documents keep their original keys, and
``transform.call.input`` is never rewritten.

**The member is the unit of derivation.** Every record that becomes the
canonical class was produced *as one member* of the equivalence cluster by
*one resource* — by a ``vertex: Shop`` step, or by a ``vertex_router`` key
whose value was ``Shop``. When a resource produces several members, its
derivations may be keyed by member; the lowering then reads the *side*
manifest (the merge has already rewritten router ``type_map`` values to the
canonical name, so the union no longer knows which key was which member) to
learn how the resource produces each member, and guards the step with
``when`` on the router's discriminator. A guarded step that does not fire
writes nothing, so each member's derivation is the single writer of the
attribute for its own documents.

A derivation that is *not* keyed by member is guarded the same way whenever a
router produces the class: ``when`` admits the discriminator values that route
onto it — the ``type_map`` keys mapping to it, or its own name for
pass-through — so the step runs for no other class's documents. An explicit
``when`` on the spec replaces that derived guard. Only a level where a plain
``vertex`` step also produces the class lowers unguarded; there a sibling
class declaring a derived attribute name is refused, since the router would
hand it the derived value.

A level producing the class through routers that read *different*
discriminators — the two roles of an edge resource — hosts no derivation at
all, keyed or not: the level's transform buffer is shared by every router at
it, so one derived attribute cannot hold one value per role. Such a resource
stays out of the sources and references the class instead;
:func:`hosts_member_derivation` tells the two apart.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from graflo.architecture.contract.ingestion.resource import (
    find_vertex_producing_levels,
    is_pass_through_router,
    is_unbounded_router,
    resolve_pipeline_level,
    step_finds,
    step_looks_up,
    step_produces_vertices,
)
from graflo.architecture.contract.ingestion.steps.models import TransformGuardConfig
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.identity_funnel import IdentityFunnel

from .canonical import CanonicalMap
from .equivalence import Side
from .ops import (
    AddResourceTransformsOp,
    AddVertexPropertiesOp,
    CanonicalizeOp,
    DerivationSpec,
    DerivedBranch,
    EnsureExtractedFields,
    EnsureExtractedFieldsOp,
    FunnelIdentityTarget,
    IdentityBranchDecl,
    IdentityReplacement,
    LocalKeyBranch,
    LocalKeySource,
    ManifestOp,
    NaturalIdentityTarget,
    ReplaceIdentityOp,
    branch_fields,
    check_identity_branches,
    identity_branches_funnel,
)

#: Anything whose ``properties`` say which attribute names exist only
#: post-rename: a declared vocabulary or the composite relabel merge applies.
VocabularyMap = CanonicalMap | CanonicalizeOp

__all__ = [
    "AlignmentConflictError",
    "IdentityPlan",
    "hosts_member_derivation",
    "identity_to_ops",
    "validate_identity",
]

logger = logging.getLogger(__name__)

#: Pre-merge side manifests keyed by side (``"left"`` / ``"right"``), as
#: ``merge_manifests`` holds them: after its resource rename policy, before
#: it relabels and merges the clusters.
SideManifests = Mapping[str, GraphManifest]

#: The aligned cluster's member classes per side.
ClusterMembers = Mapping[str, Collection[str]]

#: A branch that derives its attribute in the pipelines.
SteppedBranch = DerivedBranch | LocalKeyBranch


class AlignmentConflictError(ValueError):
    """A derived identity contradicts the union manifest or canonical maps."""


def _conflict(check: str, detail: str, hint: str) -> AlignmentConflictError:
    return AlignmentConflictError(
        f"derived identity conflict ({check}): {detail}. {hint}"
    )


@dataclass(frozen=True)
class IdentityPlan:
    """One class's declared identity branches, and where its sources derive them.

    ``branches`` are in funnel order, as :attr:`VertexEquivalence.identity`
    declares them; ``at`` is :attr:`VertexEquivalence.derive_at`;
    ``digest_field`` is :attr:`VertexEquivalence.digest_field`; ``derive``
    holds :attr:`VertexEquivalence.derive`, one derivation per attribute, which
    name and composite branches key on.
    """

    vertex: str
    branches: tuple[IdentityBranchDecl, ...]
    at: Mapping[str, list[int]] = dataclasses.field(default_factory=dict)
    digest_field: str = "id"
    derive: tuple[DerivedBranch, ...] = ()

    def __post_init__(self) -> None:
        try:
            check_identity_branches(
                self.branches,
                label=f"identity of {self.vertex!r}",
                derived=[d.name for d in self.derive],
            )
        except ValueError as exc:
            raise _conflict(
                "identity branches", str(exc), "Reorder or rename the branches."
            ) from exc

    @property
    def derived(self) -> list[DerivedBranch]:
        """Every derived attribute: the ``derive`` entries, then derived branches."""
        return [
            *self.derive,
            *(b for b in self.branches if isinstance(b, DerivedBranch)),
        ]

    @property
    def local_key(self) -> LocalKeyBranch | None:
        """The ``local_key`` branch, if declared."""
        return next((b for b in self.branches if isinstance(b, LocalKeyBranch)), None)

    @property
    def stepped(self) -> list[SteppedBranch]:
        """Branches whose attribute the pipelines derive, derived ones first."""
        out: list[SteppedBranch] = list(self.derived)
        if self.local_key is not None:
            out.append(self.local_key)
        return out

    @property
    def raw(self) -> list[tuple[str, ...]]:
        """Branches over properties the members carry, as field tuples.

        A name or composite branch over ``derive`` attributes is not one: its
        fields are derived, never all derived and properties at once.
        """
        attributes = {d.name for d in self.derive}
        return [
            tuple(branch_fields(b))
            for b in self.branches
            if isinstance(b, str | list) and branch_fields(b)[0] not in attributes
        ]

    def derived_names(self) -> list[str]:
        """Attributes the pipelines derive: every derived branch, then the local key."""
        return [branch.name for branch in self.stepped]

    def funnel(self) -> IdentityFunnel:
        """The funnel the branches declare."""
        return identity_branches_funnel(self.branches)


def _specs(
    branch: SteppedBranch, resource: str
) -> list[DerivationSpec | LocalKeySource]:
    if isinstance(branch, DerivedBranch):
        return list(branch.specs_for(resource))
    return list(branch.sources_for(resource))


def _canonical_rename_targets(canonical_maps: Sequence[VocabularyMap]) -> set[str]:
    """Property names that exist only post-rename — absent from raw documents."""
    targets: set[str] = set()
    for cm in canonical_maps:
        for attr_map in cm.properties.values():
            targets.update(new for old, new in attr_map.items() if old != new)
    return targets


def _resource_pipelines(manifest: GraphManifest) -> dict[str, list]:
    im = manifest.ingestion_model
    if im is None:
        return {}
    return {resource.name: list(resource.pipeline) for resource in im.resources}


def _vertex_set(manifest: GraphManifest) -> set[str]:
    """Classes *manifest* declares — what a router can route to by pass-through."""
    schema = manifest.graph_schema
    if schema is None:
        return set()
    return set(schema.core_schema.vertex_config.vertex_set)


def _guard_dict(guard: TransformGuardConfig) -> dict[str, Any]:
    return {"field": guard.field, "in": list(guard.values)}


def _level_discriminators(steps: list[dict]) -> list[str] | None:
    """The discriminators the routers among *steps* read, sorted.

    ``None`` when a plain ``vertex`` step is among them: that step reads the
    buffer for every document at its level, so no discriminator guards it.
    """
    if any(step.get("type") == "vertex" for step in steps):
        return None
    return sorted(
        {
            str(step.get("type_field"))
            for step in steps
            if step.get("type") == "vertex_router"
        }
    )


def _across_roles(
    resource: str, vertex: str, type_fields: list[str]
) -> AlignmentConflictError:
    """The refusal for a level that produces *vertex* through several roles."""
    return _conflict(
        "derivation across roles",
        f"resource {resource!r} produces {vertex!r} at one level through routers "
        f"reading {type_fields}; a level's transform buffer is shared by every "
        "router at it, so one derived attribute cannot hold a different value "
        "per role",
        "Leave the resource out of the branch's sources — merge attaches it to "
        "the class by the members' own keys — or produce each role in its "
        "own resource.",
    )


def hosts_member_derivation(
    manifest: GraphManifest,
    resource: str,
    member: str,
    *,
    at: Sequence[int] | None = None,
) -> bool:
    """Whether *resource* can derive an attribute for the records that become *member*.

    ``False`` when the level producing *member* — *at*, or the one level that
    does — produces it through routers reading more than one discriminator:
    the level's transform buffer is shared by every router at it, so one
    derived attribute cannot hold a different value per role, and a source
    naming the resource, keyed or not, is refused. Merge gives such a resource
    no automatic key; it references the class by the members' own keys.
    ``True`` wherever the other validators decide — a resource or level that
    does not produce the member, or several levels without *at*.
    """
    pipelines = _resource_pipelines(manifest)
    if resource not in pipelines:
        return True
    pipeline = pipelines[resource]
    if at is not None:
        path = list(at)
    else:
        candidates = find_vertex_producing_levels(
            pipeline, member, known_vertices=_vertex_set(manifest)
        )
        if len(candidates) != 1:
            return True
        path = candidates[0]
    try:
        steps = _producing_steps(manifest, resource, path, member)
    except ValueError:
        return True
    type_fields = _level_discriminators(steps)
    return type_fields is None or len(type_fields) <= 1


# --------------------------------------------------------------------------- #
# Members: how a resource produces one class, read off its side manifest.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _MemberProduction:
    """How one resource produces one member of the cluster.

    A plain ``vertex`` step produces the member unconditionally at its level,
    so ``type_field`` is ``None`` and no guard is needed — the level *is* the
    member. A ``vertex_router`` produces it for the discriminator values in
    ``values``: the ``type_map`` keys mapping to the member, or the member's
    own name when no key does (the router routes an unmapped value as-is).
    """

    level: list[int]
    type_field: str | None
    values: tuple[str, ...]

    def guard(self) -> dict[str, Any] | None:
        if self.type_field is None:
            return None
        return {"field": self.type_field, "in": list(self.values)}


#: ``{resource: {member: production}}`` for every member-keyed entry.
MemberProductions = dict[str, dict[str, _MemberProduction]]


def _member_keyed_resources(plan: IdentityPlan) -> dict[str, set[str]]:
    """Member classes each resource keys any of its sources by."""
    out: dict[str, set[str]] = {}
    for branch in plan.stepped:
        for resource in branch.sources:
            members = branch.members_for(resource)
            if members is not None:
                out.setdefault(resource, set()).update(members)
    return out


def _side_of(resource: str, sides: SideManifests) -> tuple[str, GraphManifest]:
    """The side whose ingestion model names *resource*.

    Unique once merge has applied its resource rename policy; a resource
    still present on both sides is one it has not disambiguated.
    """
    hits = [
        (side, manifest)
        for side, manifest in sides.items()
        if resource in _resource_pipelines(manifest)
    ]
    if not hits:
        raise _conflict(
            "resource on no side",
            f"resource {resource!r} is defined on none of the sides {sorted(sides)}",
            "Member-keyed sources resolve against the pre-merge side that owns "
            "the resource.",
        )
    if len(hits) > 1:
        raise _conflict(
            "resource on both sides",
            f"resource {resource!r} is defined on {[s for s, _ in hits]}",
            "Resolve the collision with the merge op's resource policy first.",
        )
    return hits[0]


def _produced_vertices(
    steps: list[Any], *, known_vertices: Collection[str] | None = None
) -> set[str]:
    """Every class any step of *steps* (recursively) produces."""
    out: set[str] = set()
    for step in steps:
        if not isinstance(step, dict):
            continue
        out |= step_produces_vertices(step, known_vertices=known_vertices)
        normalized = normalize_actor_step(dict(step))
        if normalized.get("type") == "descend":
            sub = normalized.get("pipeline")
            if isinstance(sub, list):
                out |= _produced_vertices(sub, known_vertices=known_vertices)
    return out


def _resolve_member_production(
    plan: IdentityPlan,
    resource: str,
    member: str,
    sides: SideManifests,
) -> _MemberProduction:
    side, manifest = _side_of(resource, sides)
    pipeline = _resource_pipelines(manifest)[resource]
    known = _vertex_set(manifest)
    candidates = find_vertex_producing_levels(pipeline, member, known_vertices=known)
    if not candidates:
        raise _conflict(
            "resource does not produce the member",
            f"resource {resource!r} ({side}) has no pipeline step producing {member!r}",
            "A member key names a class the resource produces on its side — "
            "through merge, by its own name or its canonical one: "
            f"{sorted(_produced_vertices(pipeline, known_vertices=known))}. "
            "A vertex_router routes any class the side's schema declares, or "
            "only those its vertex_types lists.",
        )
    if resource in plan.at:
        path = list(plan.at[resource])
        if path not in candidates:
            raise _conflict(
                "level produces nothing",
                f"`derive_at` sends resource {resource!r} derivations to level "
                f"{path or 'root'}, which produces no {member!r}",
                f"{member!r} is produced at {candidates}.",
            )
    elif len(candidates) > 1:
        raise _conflict(
            "ambiguous level",
            f"resource {resource!r} produces {member!r} at levels {candidates}",
            f"Pick one with derive_at={{{resource!r}: {candidates[0]}}} on the "
            "equivalence.",
        )
    else:
        path = candidates[0]

    steps = _producing_steps(manifest, resource, path, member)
    type_fields = _level_discriminators(steps)
    if type_fields is None:
        return _MemberProduction(level=path, type_field=None, values=())
    if len(type_fields) != 1:
        raise _across_roles(resource, member, type_fields)
    routers = [step for step in steps if step.get("type") == "vertex_router"]
    return _MemberProduction(
        level=path, type_field=type_fields[0], values=_routed_values(routers, member)
    )


def resolve_member_productions(
    plan: IdentityPlan, sides: SideManifests
) -> MemberProductions:
    """How each resource produces every member its sources are keyed by.

    Read off the *sides* — the pre-merge manifests — because the merge has
    already rewritten each router's ``type_map`` values to the canonical name,
    so the union cannot say which key produced which member.

    All members a resource keys must resolve to one pipeline level: the
    derivations are appended per resource at one level, and a member produced
    under a different ``descend`` would not see them.
    """
    out: MemberProductions = {}
    for resource, members in sorted(_member_keyed_resources(plan).items()):
        per_member = {
            member: _resolve_member_production(plan, resource, member, sides)
            for member in sorted(members)
        }
        levels = {tuple(p.level) for p in per_member.values()}
        if len(levels) > 1:
            raise _conflict(
                "members at different levels",
                f"resource {resource!r} produces "
                f"{ {m: p.level for m, p in per_member.items()} }",
                "Derivations are appended per resource at one level; produce "
                "the members at one level or derive them through separate "
                "resources.",
            )
        out[resource] = per_member
    return out


def rekey_members(
    plan: IdentityPlan,
    *,
    sides: SideManifests,
    resolve: Callable[[Side, str], str],
) -> IdentityPlan:
    """Name every member key as its side names the member.

    *resolve* maps ``(side, key)`` to the member it names — ``merge_manifests``
    passes the cluster's resolution, so a member may be keyed by its own name
    or its canonical one. Unresolved keys pass through for the validator to
    report. Two keys naming one member under one resource are refused.
    """

    def _names(resource: str, keys: Iterable[str]) -> dict[str, str]:
        side_name, _ = _side_of(resource, sides)
        side: Side = "left" if side_name == "left" else "right"
        out: dict[str, str] = {}
        for key in keys:
            member = resolve(side, key)
            prior = next((k for k, m in out.items() if m == member), None)
            if prior is not None:
                raise _conflict(
                    "member keyed twice",
                    f"resource {resource!r} keys {side} member {member!r} as both "
                    f"{prior!r} and {key!r}",
                    "Key each member once.",
                )
            out[key] = member
        return out

    def _rekeyed(entries: Mapping[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for resource, entry in entries.items():
            if isinstance(entry, dict):
                names = _names(resource, entry)
                out[resource] = {names[m]: v for m, v in entry.items()}
            else:
                out[resource] = entry
        return out

    branches: list[IdentityBranchDecl] = []
    for branch in plan.branches:
        if isinstance(branch, DerivedBranch):
            branch = branch.model_copy(update={"sources": _rekeyed(branch.sources)})
        elif isinstance(branch, LocalKeyBranch):
            branch = branch.model_copy(update={"local_key": _rekeyed(branch.local_key)})
        branches.append(branch)
    derive = tuple(
        d.model_copy(update={"sources": _rekeyed(d.sources)}) for d in plan.derive
    )
    return dataclasses.replace(plan, branches=tuple(branches), derive=derive)


def _require_sides(
    plan: IdentityPlan, sides: SideManifests | None
) -> SideManifests | None:
    if sides is None and _member_keyed_resources(plan):
        raise _conflict(
            "member-keyed sources without sides",
            "sources keyed by member class need the pre-merge side manifests "
            "to resolve how each resource produces the member",
            "merge_manifests supplies them; when calling directly, pass "
            "sides={'left': ..., 'right': ...}.",
        )
    return sides


# --------------------------------------------------------------------------- #
# Levels
# --------------------------------------------------------------------------- #


def resolve_derivation_levels(
    plan: IdentityPlan,
    manifest: GraphManifest,
    *,
    productions: MemberProductions | None = None,
) -> dict[str, list[int]]:
    """Pipeline level each referenced resource derives at, keyed by resource.

    A derivation must land at the level that produces the class: an actor
    reads its transform buffer at its own ``LocationIndex`` with no ancestor
    fallback, and a ``descend`` subtree runs before its own level's
    transforms. Placing it anywhere else derives nothing, silently.

    ``plan.at`` (the equivalence's ``derive_at``) overrides the lookup. A
    member-keyed resource takes the level its members are produced at (see
    :func:`resolve_member_productions`). Otherwise a resource must produce the
    class at exactly one level — zero and several are both
    :class:`AlignmentConflictError`, because either answer the resolver could
    pick would be a guess about where the source fields live.
    """
    pipelines = _resource_pipelines(manifest)
    known = _vertex_set(manifest)
    levels: dict[str, list[int]] = {}
    for resource in sorted(_referenced_resources(plan)):
        pipeline = pipelines.get(resource, [])
        if resource in plan.at:
            path = list(plan.at[resource])
            try:
                level = resolve_pipeline_level(list(pipeline), path)
            except ValueError as exc:
                raise _conflict(
                    "unresolvable level",
                    f"`derive_at` for resource {resource!r}: {exc}",
                    "Each index must address a descend step; [] is the root level.",
                ) from exc
            # An override that resolves but produces nothing is the failure this
            # resolution exists to prevent: the derivations would be appended,
            # run, find no inputs, and skip without a word.
            if not any(
                isinstance(step, dict)
                and plan.vertex in step_produces_vertices(step, known_vertices=known)
                for step in level
            ):
                candidates = find_vertex_producing_levels(
                    pipeline, plan.vertex, known_vertices=known
                )
                raise _conflict(
                    "level produces nothing",
                    f"`derive_at` sends resource {resource!r} derivations to level "
                    f"{path or 'root'}, which produces no {plan.vertex!r}",
                    (
                        f"A transform is only visible to actors at its own "
                        f"level; {plan.vertex!r} is produced at {candidates}."
                    )
                    if candidates
                    else f"This resource never produces {plan.vertex!r}.",
                )
            levels[resource] = path
            continue

        if productions and resource in productions:
            # The members resolved to one level on the side; the union's
            # pipeline for this resource is the same object graph.
            levels[resource] = list(next(iter(productions[resource].values())).level)
            continue

        candidates = find_vertex_producing_levels(
            pipeline, plan.vertex, known_vertices=known
        )
        if not candidates:
            raise _conflict(
                "resource does not produce the class",
                f"resource {resource!r} has no pipeline step producing {plan.vertex!r}",
                "A derived branch computes an attribute for the documents that "
                "become this class; a resource that never produces it has "
                "nothing to derive.",
            )
        if len(candidates) > 1:
            raise _conflict(
                "ambiguous level",
                f"resource {resource!r} produces {plan.vertex!r} at "
                f"levels {candidates}",
                f"Derivation inputs live at one level. Pick it with "
                f"derive_at={{{resource!r}: {candidates[0]}}} on the equivalence.",
            )
        levels[resource] = candidates[0]
    return levels


def _referenced_resources(plan: IdentityPlan) -> set[str]:
    names: set[str] = set(plan.at)
    for branch in plan.stepped:
        names.update(branch.sources)
    return names


def _producing_steps(
    manifest: GraphManifest, resource: str, path: list[int], vertex: str
) -> list[dict]:
    """Normalized steps at *path* in *resource* that produce *vertex*.

    The two tiers of :func:`find_vertex_producing_levels`, within one level:
    the steps naming the class explicitly when any does, else every open
    router there — each routes the raw discriminator value as the class name.
    """
    pipeline = list(_resource_pipelines(manifest).get(resource, []))
    level = resolve_pipeline_level(pipeline, list(path))
    steps = [
        normalize_actor_step(dict(step)) for step in level if isinstance(step, dict)
    ]
    explicit = [step for step in steps if vertex in step_produces_vertices(step)]
    if explicit or vertex not in _vertex_set(manifest):
        return explicit
    return [step for step in steps if is_unbounded_router(step)]


def _routed_values(routers: list[dict], vertex: str) -> tuple[str, ...]:
    """Discriminator values *routers* send onto *vertex*.

    Per router: the ``type_map`` keys mapping to it, and -- for an open router
    whose table does not key it -- the raw value that *is* the class name,
    which passes through to it. A closed one reaches it only through its
    table; one whose ``vertex_types`` leaves *vertex* out, not at all.
    """
    values: set[str] = set()
    for step in routers:
        bound = step.get("vertex_types")
        if bound is not None and vertex not in bound:
            continue
        type_map = step.get("type_map") or {}
        values.update(key for key, target in type_map.items() if target == vertex)
        if is_pass_through_router(step) and vertex not in type_map:
            values.add(vertex)
    return tuple(sorted(values))


def _class_guard(steps: list[dict], vertex: str) -> dict[str, Any] | None:
    """Guard admitting only the documents *steps* route onto *vertex*.

    ``None`` when no single guard can say so: a plain ``vertex`` step among
    the producers reads the buffer for every document at its level, and a
    guard on a discriminator those documents may not carry would suppress the
    derivation. Unguarded is what the sibling-class check then covers.
    Routers reading different discriminators are refused before this is asked.
    """
    type_fields = _level_discriminators(steps)
    if type_fields is None or len(type_fields) != 1:
        return None
    routers = [step for step in steps if step.get("type") == "vertex_router"]
    return {"field": type_fields[0], "in": list(_routed_values(routers, vertex))}


def _unguarded_names(plan: IdentityPlan, resource: str) -> list[str]:
    """Attributes *resource* derives with neither a member key nor its own ``when``."""
    return [
        branch.name
        for branch in plan.stepped
        if resource in branch.sources
        and branch.members_for(resource) is None
        and all(spec.when is None for spec in _specs(branch, resource))
    ]


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def validate_identity(
    plan: IdentityPlan,
    manifest: GraphManifest,
    *,
    canonical_maps: Sequence[VocabularyMap] = (),
    sides: SideManifests | None = None,
    cluster_members: ClusterMembers | None = None,
    member_identity: Collection[str] | None = None,
    uncovered_producers: Literal["refuse", "allow"] = "refuse",
    completes_property_branch: Callable[[str], bool] | None = None,
) -> None:
    """Fail loudly when *plan* contradicts *manifest* or the canonical maps.

    *manifest* is the merged union the ops will be applied to. Pass the maps
    used to canonicalize the sides — declared :class:`CanonicalMap`\\ s or the
    composite :class:`~graflo.architecture.evolution.ops.CanonicalizeOp`
    merge applied — to catch derivation inputs written in canonical
    vocabulary: renamed documents still carry their raw field names, so a
    rename *target* used as a derivation input reads an absent field and
    silently derives nothing.

    *sides* are the pre-merge side manifests, required by member-keyed
    sources; *cluster_members* are the cluster's members per side, which lets
    a member key be checked against the cluster it claims.

    *member_identity* are the identity fields the class's records carry — for
    a merged class, its members' own pre-merge keys under their canonical
    names. A derived attribute named like one would overwrite that key's
    value with the derived one. It defaults to the class's current identity
    in *manifest*.

    *uncovered_producers* decides a resource that upserts the class but
    derives none of its key (see :func:`uncovered_producers`): ``refuse`` it,
    or ``allow`` it for a caller that turns it into a reference -- merge does,
    once the member keys it can look the class up by exist.
    *completes_property_branch* says whether a resource's records complete
    one of the plan's property branches; see :func:`uncovered_producers`.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise AlignmentConflictError("a derived identity requires graph_schema")
    vertex_config = schema.core_schema.vertex_config
    if plan.vertex not in vertex_config.vertex_set:
        raise _conflict(
            "unknown vertex",
            f"{plan.vertex!r} is not defined in the manifest",
            f"Defined: {sorted(vertex_config.vertex_set)}.",
        )
    if manifest.ingestion_model is None:
        raise AlignmentConflictError(
            "a derived identity requires ingestion_model — derivations are "
            "resource pipeline steps"
        )
    known_resources = {r.name for r in manifest.ingestion_model.resources}

    resource_refs: set[str] = set(plan.at)
    raw_inputs: dict[str, list[str]] = {}
    for branch in plan.stepped:
        resource_refs.update(branch.sources)
        for resource in branch.sources:
            for spec in _specs(branch, resource):
                fields = raw_inputs.setdefault(resource, [])
                fields.extend(
                    spec.input if isinstance(spec, DerivationSpec) else [spec.field]
                )
                if spec.when is not None:
                    fields.append(spec.when.field)

    missing = sorted(resource_refs - known_resources)
    if missing:
        raise _conflict(
            "unknown resources",
            f"{missing} are not defined in the manifest",
            f"Defined: {sorted(known_resources)}.",
        )

    sides = _require_sides(plan, sides)
    productions = resolve_member_productions(plan, sides) if sides is not None else {}
    for resource, per_member in productions.items():
        assert sides is not None
        side, _ = _side_of(resource, sides)
        if cluster_members is not None:
            allowed = set(cluster_members.get(side, ()))
            outside = sorted(set(per_member) - allowed)
            if outside:
                raise _conflict(
                    "member outside the cluster",
                    f"resource {resource!r} ({side}) keys derivations by "
                    f"{outside}, which are not members of the "
                    f"{plan.vertex!r} cluster on that side",
                    f"Members on the {side}: {sorted(allowed)}.",
                )
        for production in per_member.values():
            if production.type_field is not None:
                raw_inputs.setdefault(resource, []).append(production.type_field)

    current_identity = set(
        member_identity
        if member_identity is not None
        else vertex_config.identity_fields(plan.vertex)
    )
    colliding = sorted(set(plan.derived_names()) & current_identity)
    if colliding:
        raise _conflict(
            "identity collision",
            f"derived attributes {colliding} are already identity fields of "
            f"{plan.vertex!r}'s records",
            "Pick attribute names distinct from the members' own keys; a "
            "derivation would overwrite the key's value. To key on a member's "
            "own key, list it as a property branch.",
        )

    declared = set(vertex_config.property_names(plan.vertex))
    undeclared = sorted({f for fields in plan.raw for f in fields} - declared)
    if undeclared:
        raise _conflict(
            "undeclared property branch",
            f"property branches name {undeclared}, which {plan.vertex!r} does "
            "not declare",
            "A property branch keys on a property the members carry, under "
            "its canonical name.",
        )

    rename_targets = _canonical_rename_targets(tuple(canonical_maps))
    if rename_targets:
        for resource, fields in raw_inputs.items():
            canonical_used = sorted(
                set(fields)
                & rename_targets - _raw_properties(resource, sides, cluster_members)
            )
            if canonical_used:
                raise _conflict(
                    "canonical name as derivation input",
                    f"resource {resource!r} derivations read {canonical_used}, "
                    "which are canonical rename targets — documents still "
                    "carry the RAW source field names",
                    "Use the raw field names the source documents actually "
                    "carry (property renames rewrite vertex.from maps, not "
                    "transform inputs).",
                )

    levels = resolve_derivation_levels(plan, manifest, productions=productions)
    if uncovered_producers == "refuse":
        _check_uncovered_producers(plan, manifest, completes_property_branch)
    _check_derivation_signatures(plan)

    for resource, path in sorted(levels.items()):
        steps = _producing_steps(manifest, resource, path, plan.vertex)
        type_fields = _level_discriminators(steps)
        if type_fields is not None and len(type_fields) > 1:
            raise _across_roles(resource, plan.vertex, type_fields)
        unguarded = (
            _unguarded_names(plan, resource)
            if _class_guard(steps, plan.vertex) is None
            else []
        )
        for step in steps:
            if unguarded:
                _check_sibling_classes(plan, manifest, step, resource, unguarded)
            _warn_on_one_derivation_for_several_members(plan, step, resource)
    if sides is not None and cluster_members is not None:
        for resource in productions:
            _warn_on_partial_member_coverage(plan, resource, sides, cluster_members)

    if plan.local_key is None and not plan.raw:
        logger.warning(
            "identity for %r has no local_key branch: records deriving none of "
            "its attributes complete no funnel branch and are dropped",
            plan.vertex,
        )


def _raw_properties(
    resource: str,
    sides: SideManifests | None,
    cluster_members: ClusterMembers | None,
) -> set[str]:
    """Property names the members *resource* produces declare on its own side.

    Those are columns its documents carry, whatever the other side renames
    onto the same spelling: aligning B's ``cname`` onto A's ``name`` makes
    ``name`` a rename target, and A's resource reading its own ``name`` is
    exactly right. Empty without the sides, which leaves the check global.
    """
    if sides is None:
        return set()
    hits = [
        manifest
        for manifest in sides.values()
        if resource in _resource_pipelines(manifest)
    ]
    if len(hits) != 1 or hits[0].graph_schema is None:
        return set()
    manifest = hits[0]
    schema = manifest.graph_schema
    assert schema is not None
    vertex_config = schema.core_schema.vertex_config
    produced = _produced_vertices(
        _resource_pipelines(manifest)[resource], known_vertices=_vertex_set(manifest)
    )
    if cluster_members is not None:
        members = {m for side in cluster_members.values() for m in side}
        produced &= members
    return {
        name
        for vertex in produced
        if vertex in vertex_config.vertex_set
        for name in vertex_config.property_names(vertex)
    }


def uncovered_producers(
    plan: IdentityPlan,
    manifest: GraphManifest,
    *,
    completes_property_branch: Callable[[str], bool] | None = None,
) -> list[str]:
    """Resources that upsert the class but can complete none of its key, sorted.

    Every record of the class keys on the funnel over the plan's branches. A
    resource no derived branch names derives none of their attributes, so
    each record it upserts completes no branch and is dropped -- the whole
    resource, and every edge it emits to the class -- unless it completes a
    property branch. *completes_property_branch* answers that per resource
    (merge knows each member's properties); without it, a plan with any
    property branch is taken to cover every resource. A resource whose steps
    producing the class only look it up (``lookup_only``) or find it by a
    secondary identity (``find``), on a vertex step or a router, creates
    nothing and is not listed.
    """
    covered = _referenced_resources(plan)
    known = _vertex_set(manifest)
    out: list[str] = []
    for resource, pipeline in sorted(_resource_pipelines(manifest).items()):
        if resource in covered:
            continue
        if not any(
            not step_looks_up(step, plan.vertex)
            and step_finds(step, plan.vertex, known_vertices=known) is None
            for step in _steps_producing_anywhere(pipeline, plan.vertex, known)
        ):
            continue
        if plan.raw and (
            completes_property_branch is None or completes_property_branch(resource)
        ):
            continue
        out.append(resource)
    return out


def _check_uncovered_producers(
    plan: IdentityPlan,
    manifest: GraphManifest,
    completes_property_branch: Callable[[str], bool] | None,
) -> None:
    """Refuse the :func:`uncovered_producers` of *plan*, all of them at once."""
    uncovered = uncovered_producers(
        plan, manifest, completes_property_branch=completes_property_branch
    )
    if uncovered:
        raise _conflict(
            "uncovered producer",
            f"resources {uncovered} produce {plan.vertex!r} but derive none of "
            "its identity branches there, so every record they upsert would "
            "complete no branch and be dropped",
            "Add a resource whose rows carry the inputs to a derived branch's "
            "sources; give the step of one that writes onto the class by "
            "another key `find: <secondary identity>` (on a vertex_router, "
            f"`find: {{{plan.vertex}: <secondary identity>}}`), and mark one "
            "that only references it `lookup_only`. Merge attaches it by its "
            "own key itself.",
        )


def _steps_producing_anywhere(
    pipeline: list[Any], vertex: str, known: Collection[str]
) -> list[dict[str, Any]]:
    """Normalized steps producing *vertex* at any level of *pipeline*."""
    out: list[dict[str, Any]] = []
    for step in pipeline:
        if not isinstance(step, dict):
            continue
        normalized = normalize_actor_step(dict(step))
        if vertex in step_produces_vertices(normalized, known_vertices=known):
            out.append(normalized)
        if normalized.get("type") == "descend":
            nested = normalized.get("pipeline")
            if isinstance(nested, list):
                out.extend(_steps_producing_anywhere(nested, vertex, known))
    return out


def _check_derivation_signatures(plan: IdentityPlan) -> None:
    """Refuse a derivation whose function cannot take its inputs and parameters.

    The call is ``foo(*input, **params)``. One that cannot bind fails for
    every document at ingestion, the attribute is never derived, and every
    record falls through to a lower funnel branch or is dropped — with nothing
    at merge time to say so. Binding against the signature here turns that
    into a refusal naming the call.
    """
    import importlib
    import inspect

    for branch in plan.derived:
        for resource in branch.sources:
            for spec in branch.specs_for(resource):
                try:
                    function = getattr(importlib.import_module(spec.module), spec.foo)
                except (ImportError, AttributeError) as exc:
                    raise _conflict(
                        "derivation signature",
                        f"resource {resource!r} derives {branch.name!r} with "
                        f"{spec.module}.{spec.foo}, which cannot be imported ({exc})",
                        "Name a function the module defines.",
                    ) from exc
                try:
                    signature = inspect.signature(function)
                except (TypeError, ValueError):
                    continue  # a builtin without introspectable signature
                try:
                    signature.bind(*spec.input, **spec.params)
                except TypeError as exc:
                    raise _conflict(
                        "derivation signature",
                        f"resource {resource!r} derives {branch.name!r} as "
                        f"{spec.foo}(*{spec.input}, **{spec.params}), which "
                        f"{spec.foo}{signature} cannot take ({exc})",
                        "Give `input` one field per positional parameter — "
                        "`normalized_key` (the default) takes one, "
                        "`gated_normalized_key` a gate and a value. A key "
                        "over several columns derives each part as its own "
                        "attribute in `derive` and lists them as one "
                        "composite branch.",
                    ) from exc


def _check_sibling_classes(
    plan: IdentityPlan,
    manifest: GraphManifest,
    step: dict,
    resource: str,
    into_names: list[str],
) -> None:
    """Refuse a derived attribute name another routed class also declares.

    Only for a derivation that lowers unguarded — the level also produces the
    class through a plain ``vertex`` step, or its routers read different
    discriminators, and the spec sets no ``when`` of its own. A router hands
    the whole merged observation to whichever class it selects, and
    extraction keeps a class's declared properties, so a sibling class
    declaring one of the names would silently absorb the value derived for
    this class. A guarded derivation never runs for the sibling's documents
    and needs none of this.
    """
    if step.get("type") != "vertex_router":
        return
    schema = manifest.graph_schema
    assert schema is not None
    vertex_config = schema.core_schema.vertex_config
    siblings = step_produces_vertices(step, known_vertices=vertex_config.vertex_set) - {
        plan.vertex
    }
    for sibling in sorted(siblings):
        if sibling not in vertex_config.vertex_set:
            continue
        shared = sorted(set(into_names) & set(vertex_config.property_names(sibling)))
        if shared:
            raise _conflict(
                "canonical attribute claimed by a sibling class",
                f"resource {resource!r} routes to {sibling!r} at the same level "
                f"as {plan.vertex!r}, and {sibling!r} declares {shared}",
                f"A router passes one observation to whichever class it picks, "
                f"so {sibling!r} would absorb the derived value. Rename the "
                f"derived attribute, or the property on {sibling!r}.",
            )


def _warn_on_one_derivation_for_several_members(
    plan: IdentityPlan, step: dict, resource: str
) -> None:
    """Flag a router folding several members onto the class with one derivation.

    Legitimate when the members share a key column and a marker convention;
    wrong when each carries its own, which needs a derivation per member.
    """
    if step.get("type") != "vertex_router":
        return
    type_map = step.get("type_map") or {}
    routed = sorted(k for k, v in type_map.items() if v == plan.vertex)
    if len(routed) < 2:
        return
    single = [
        branch.name
        for branch in plan.stepped
        if resource in branch.sources and branch.members_for(resource) is None
    ]
    if single:
        logger.warning(
            "identity for %r: resource %r routes %s onto %r but derives %s one "
            "way — correct when those members share a key column, otherwise "
            "key the derivation by member",
            plan.vertex,
            resource,
            routed,
            plan.vertex,
            single,
        )


def _warn_on_partial_member_coverage(
    plan: IdentityPlan,
    resource: str,
    sides: SideManifests,
    cluster_members: ClusterMembers,
) -> None:
    """Flag a member the resource produces that no keyed derivation covers.

    Its records derive nothing for that attribute and key on whatever lower
    funnel branch they complete — possibly intended, never silent.
    """
    side, manifest = _side_of(resource, sides)
    produced = _produced_vertices(
        _resource_pipelines(manifest)[resource], known_vertices=_vertex_set(manifest)
    )
    in_cluster = produced & set(cluster_members.get(side, ()))
    for branch in plan.stepped:
        members = branch.members_for(resource)
        if members is None:
            continue
        uncovered = sorted(in_cluster - set(members))
        if uncovered:
            logger.warning(
                "identity for %r: resource %r produces %s but derives %r only "
                "for %s — records of the uncovered members carry no %r",
                plan.vertex,
                resource,
                sorted(in_cluster),
                branch.name,
                sorted(members),
                branch.name,
            )


# --------------------------------------------------------------------------- #
# Lowering
# --------------------------------------------------------------------------- #


def identity_to_ops(
    plan: IdentityPlan,
    *,
    manifest: GraphManifest | None = None,
    canonical_maps: Sequence[VocabularyMap] = (),
    sides: SideManifests | None = None,
    cluster_members: ClusterMembers | None = None,
    member_identity: Collection[str] | None = None,
    uncovered_producers: Literal["refuse", "allow"] = "refuse",
    completes_property_branch: Callable[[str], bool] | None = None,
    origins: Mapping[Side, str] | None = None,
) -> list[ManifestOp]:
    """Lower *plan* to an ordered list of fundamental ops.

    Apply the result to the merged union with
    :func:`~graflo.architecture.evolution.apply.apply_evolution`. When
    *manifest* is given, :func:`validate_identity` runs first. Member-keyed
    sources need *sides* (the pre-merge manifests) to resolve how each
    resource produces each member; ``merge_manifests`` passes them. A
    ``local_key`` source with no ``tag`` is tagged with the origin of its
    resource's side: *origins* keyed by side, resolved through *sides*.
    """
    if manifest is not None:
        validate_identity(
            plan,
            manifest,
            canonical_maps=canonical_maps,
            sides=sides,
            cluster_members=cluster_members,
            member_identity=member_identity,
            uncovered_producers=uncovered_producers,
            completes_property_branch=completes_property_branch,
        )
    sides = _require_sides(plan, sides)
    productions = resolve_member_productions(plan, sides) if sides is not None else {}

    ops: list[ManifestOp] = []
    into_names = plan.derived_names()
    if into_names:
        ops.append(AddVertexPropertiesOp(additions={plan.vertex: list(into_names)}))

    if manifest is not None:
        levels = resolve_derivation_levels(plan, manifest, productions=productions)
    else:
        levels = {
            resource: list(next(iter(per_member.values())).level)
            for resource, per_member in productions.items()
        }

    # Behind a router, an unkeyed derivation is guarded by the class as a
    # whole; without the manifest there is no router to read, so nothing is.
    class_guards: dict[str, dict[str, Any] | None] = {}
    if manifest is not None:
        for resource, path in levels.items():
            steps = _producing_steps(manifest, resource, path, plan.vertex)
            class_guards[resource] = _class_guard(steps, plan.vertex)

    additions: dict[str, list[dict[str, Any]]] = {}
    for branch in plan.stepped:
        for resource in branch.sources:
            additions.setdefault(resource, []).extend(
                _branch_steps(
                    branch,
                    resource,
                    productions=productions,
                    class_guard=class_guards.get(resource),
                    sides=sides,
                    origins=origins,
                )
            )
    if additions:
        ops.append(
            AddResourceTransformsOp(
                additions=additions,
                at={
                    resource: path
                    for resource, path in levels.items()
                    if path and resource in additions
                },
            )
        )

    if manifest is not None and into_names:
        ensure = _ensure_extracted_fields_op(plan, manifest, levels, into_names)
        if ensure is not None:
            ops.append(ensure)

    # One property branch is a natural key; anything else is a funnel, because
    # a derived attribute is never guaranteed present and include_branch_id
    # keeps branches over equal values apart.
    if len(plan.branches) == 1 and not plan.stepped:
        target: FunnelIdentityTarget | NaturalIdentityTarget = NaturalIdentityTarget(
            identity=list(plan.raw[0])
        )
    else:
        target = FunnelIdentityTarget(
            funnel=plan.funnel(), digest_field=plan.digest_field
        )
    ops.append(
        ReplaceIdentityOp(
            replacements={
                plan.vertex: IdentityReplacement(
                    to=target,
                    # The pre-lowering identity on a merged class is the
                    # merged union of the side keys — a field-set no record
                    # carries. Demoting it would index nothing; merge demotes
                    # the per-member keys once this funnel is in place.
                    retire="keep",
                )
            }
        )
    )
    return ops


def _call_step(
    *,
    module: str,
    foo: str,
    params: dict[str, Any],
    output: str,
    input_fields: list[str],
    when: dict[str, Any] | None = None,
) -> dict[str, Any]:
    call: dict[str, Any] = {
        "module": module,
        "foo": foo,
        "params": params,
        "output": [output],
        "input": list(input_fields),
    }
    transform: dict[str, Any] = {"call": call}
    if when is not None:
        transform["when"] = when
    return {"transform": transform}


def _origin_tag(
    resource: str, sides: SideManifests | None, origins: Mapping[Side, str] | None
) -> str:
    """The tag an untagged ``local_key`` source of *resource* takes: its side's origin."""
    if sides is None or origins is None:
        raise _conflict(
            "local key without a tag",
            f"the local_key source for resource {resource!r} sets no tag, and "
            "only a merge knows the origin it defaults to",
            "Set `tag`, or `tag: null` to keep the raw values.",
        )
    side, _ = _side_of(resource, sides)
    return origins[cast(Side, side)]


def _branch_steps(
    branch: SteppedBranch,
    resource: str,
    *,
    productions: MemberProductions,
    class_guard: dict[str, Any] | None,
    sides: SideManifests | None = None,
    origins: Mapping[Side, str] | None = None,
) -> list[dict[str, Any]]:
    """The steps *resource* derives *branch*'s attribute with, in order.

    One step per spec, each the single writer of the attribute for the
    documents its guard admits: a guarded step that does not fire writes
    nothing, so nothing clobbers behind a ``vertex_router`` (which merges the
    transform buffer into one observation dict, where a later ``None`` would
    overwrite an earlier real value). A member-keyed spec is guarded by its
    member's production; an unkeyed one by its own ``when``, else by the class
    guard a router-produced level supplies.
    """
    members = branch.members_for(resource)
    specs = _specs(branch, resource)
    if members is not None:
        guards = [productions[resource][member].guard() for member in members]
    else:
        guards = [
            _guard_dict(spec.when) if spec.when is not None else class_guard
            for spec in specs
        ]
    steps: list[dict[str, Any]] = []
    for spec, guard in zip(specs, guards, strict=True):
        if isinstance(spec, DerivationSpec):
            steps.append(
                _call_step(
                    module=spec.module,
                    foo=spec.foo,
                    params=dict(spec.params),
                    output=branch.name,
                    input_fields=list(spec.input),
                    when=guard,
                )
            )
        else:
            assert isinstance(branch, LocalKeyBranch)
            tag = (
                _origin_tag(resource, sides, origins)
                if spec.tag_omitted
                else spec.tag or ""
            )
            steps.append(
                _call_step(
                    module="graflo.util.transform",
                    foo="tagged_key",
                    params={"tag": tag, "sep": branch.sep},
                    output=branch.name,
                    input_fields=[spec.field],
                    when=guard,
                )
            )
    return steps


def _ensure_extracted_fields_op(
    plan: IdentityPlan,
    manifest: GraphManifest,
    levels: dict[str, list[int]],
    into_names: list[str],
) -> EnsureExtractedFieldsOp | None:
    """Widen restrictive routers so the derived attributes reach the class.

    Only routers need this. A router's child ``VertexActor`` runs at a
    ``LocationIndex`` whose transform buffer is empty, so derived attributes
    arrive through the merged observation — subject to ``keep_fields`` and
    ``extraction_scope``. A plain ``vertex`` step reads the buffer directly and
    is unaffected, so it gets no entry.
    """
    additions: dict[str, list[EnsureExtractedFields]] = {}
    for resource, path in sorted(levels.items()):
        for step in _producing_steps(manifest, resource, path, plan.vertex):
            if step.get("type") != "vertex_router":
                continue
            if step.get("keep_fields") is None and (
                step.get("extraction_scope") != "mapped_only"
            ):
                continue
            additions.setdefault(resource, []).append(
                EnsureExtractedFields(
                    vertex=plan.vertex, fields=list(into_names), at=list(path)
                )
            )
    if not additions:
        return None
    return EnsureExtractedFieldsOp(additions=additions)
