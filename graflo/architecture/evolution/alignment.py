"""Identity alignment: compose an equivalence identity from fundamental ops.

An :class:`IdentityAlignment` states, for one canonical class, which canonical
attributes carry cross-source entity equivalence and how each resource derives
them. It is a *composer*, not a mechanism: :func:`alignment_to_ops` emits only
fundamental ops —

1. ``AddVertexPropertiesOp`` — declare the canonical attributes on the class;
2. ``AddResourceTransformsOp`` — per-resource derivation steps (gating,
   normalization, local-key namespacing) appended to the pipelines;
3. ``ReplaceIdentityOp`` — a priority funnel over the canonical attributes,
   in declared order, with the namespaced ``local_key`` as the last branch;
4. ``AddSecondaryIdentitiesOp`` — the retired side keys as lookup-only
   secondary identities.

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
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from graflo.architecture.contract.ingestion.resource import (
    find_vertex_producing_levels,
    resolve_pipeline_level,
    step_produces_vertices,
)
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.architecture.schema.vertex import SecondaryIdentity

from .canonical import CanonicalMap
from .equivalence import Side
from .ops import (
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexPropertiesOp,
    AlignmentAttribute,
    DerivationSpec,
    EnsureExtractedFields,
    EnsureExtractedFieldsOp,
    FunnelIdentityTarget,
    IdentityAlignment,
    IdentityReplacement,
    LocalKeySource,
    LocalKeySpec,
    ManifestOp,
    ReplaceIdentityOp,
    SharedDerivation,
)

__all__ = [
    "AlignmentAttribute",
    "AlignmentConflictError",
    "AlignmentRow",
    "DerivationSpec",
    "IdentityAlignment",
    "LocalKeySource",
    "LocalKeySpec",
    "SharedDerivation",
    "alignment_to_ops",
    "validate_alignment",
]

logger = logging.getLogger(__name__)

#: Deprecated spelling of :class:`AlignmentAttribute`. "Row" collided with the
#: ingestion data unit (an observation at a ``LocationIndex``); an entry here is
#: one canonical attribute.
AlignmentRow = AlignmentAttribute

#: Pre-merge side manifests keyed by side (``"left"`` / ``"right"``), as
#: ``compose_manifests`` holds them: after its resource rename policy, before
#: it relabels and merges the clusters.
SideManifests = Mapping[str, GraphManifest]

#: The aligned cluster's member classes per side.
ClusterMembers = Mapping[str, Collection[str]]


class AlignmentConflictError(ValueError):
    """An identity alignment contradicts the union manifest or canonical maps."""


def _conflict(check: str, detail: str, hint: str) -> AlignmentConflictError:
    return AlignmentConflictError(
        f"identity alignment conflict ({check}): {detail}. {hint}"
    )


def _canonical_rename_targets(canonical_maps: tuple[CanonicalMap, ...]) -> set[str]:
    """Property names that exist only post-rename — absent from raw documents."""
    targets: set[str] = set()
    for cm in canonical_maps:
        for attr_map in cm.properties.values():
            targets.update(new for old, new in attr_map.items() if old != new)
    return targets


def _scratch_name(into: str, index: int) -> str:
    """Scratch field one column-presence derivation writes before the coalesce."""
    return f"_{into}__{index}"


def _resource_pipelines(manifest: GraphManifest) -> dict[str, list]:
    im = manifest.ingestion_model
    if im is None:
        return {}
    return {resource.name: list(resource.pipeline) for resource in im.resources}


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


def _member_keyed_resources(alignment: IdentityAlignment) -> dict[str, set[str]]:
    """Member classes each resource keys any of its sources by."""
    out: dict[str, set[str]] = {}
    for attribute in alignment.attributes:
        for resource in attribute.sources:
            members = attribute.members_for(resource)
            if members is not None:
                out.setdefault(resource, set()).update(members)
    if alignment.local_key is not None:
        for resource in alignment.local_key.sources:
            members = alignment.local_key.members_for(resource)
            if members is not None:
                out.setdefault(resource, set()).update(members)
    return out


def _side_of(resource: str, sides: SideManifests) -> tuple[str, GraphManifest]:
    """The side whose ingestion model names *resource*.

    Unique once compose has applied its resource rename policy; a resource
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
            "Resolve the collision with the compose op's resource policy first.",
        )
    return hits[0]


def _produced_vertices(steps: list[Any]) -> set[str]:
    """Every class any step of *steps* (recursively) produces."""
    out: set[str] = set()
    for step in steps:
        if not isinstance(step, dict):
            continue
        out |= step_produces_vertices(step)
        normalized = normalize_actor_step(dict(step))
        if normalized.get("type") == "descend":
            sub = normalized.get("pipeline")
            if isinstance(sub, list):
                out |= _produced_vertices(sub)
    return out


def _resolve_member_production(
    alignment: IdentityAlignment,
    resource: str,
    member: str,
    sides: SideManifests,
) -> _MemberProduction:
    side, manifest = _side_of(resource, sides)
    pipeline = _resource_pipelines(manifest)[resource]
    candidates = find_vertex_producing_levels(pipeline, member)
    if not candidates:
        raise _conflict(
            "resource does not produce the member",
            f"resource {resource!r} ({side}) has no pipeline step producing {member!r}",
            "A member key names a class the resource produces on its side — "
            "through compose, by its own name or its canonical one: "
            f"{sorted(_produced_vertices(pipeline))}.",
        )
    if resource in alignment.at:
        path = list(alignment.at[resource])
        if path not in candidates:
            raise _conflict(
                "level produces nothing",
                f"`at` sends resource {resource!r} derivations to level "
                f"{path or 'root'}, which produces no {member!r}",
                f"{member!r} is produced at {candidates}.",
            )
    elif len(candidates) > 1:
        raise _conflict(
            "ambiguous level",
            f"resource {resource!r} produces {member!r} at levels {candidates}",
            f"Pick one with IdentityAlignment(at={{{resource!r}: {candidates[0]}}}).",
        )
    else:
        path = candidates[0]

    steps = _producing_steps(manifest, resource, path, member)
    if any(step.get("type") == "vertex" for step in steps):
        return _MemberProduction(level=path, type_field=None, values=())
    routers = [step for step in steps if step.get("type") == "vertex_router"]
    type_fields = sorted({str(step.get("type_field")) for step in routers})
    if len(type_fields) != 1:
        raise _conflict(
            "ambiguous discriminator",
            f"resource {resource!r} produces {member!r} through routers "
            f"discriminated by {type_fields}",
            "One guard reads one field; route the member through one router.",
        )
    values: set[str] = set()
    for step in routers:
        type_map = step.get("type_map") or {}
        values.update(key for key, target in type_map.items() if target == member)
    # A router without a ``type_map`` entry for the member routes the raw
    # discriminator value that *is* the member's name.
    if not values:
        values.add(member)
    return _MemberProduction(
        level=path, type_field=type_fields[0], values=tuple(sorted(values))
    )


def resolve_member_productions(
    alignment: IdentityAlignment, sides: SideManifests
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
    for resource, members in sorted(_member_keyed_resources(alignment).items()):
        per_member = {
            member: _resolve_member_production(alignment, resource, member, sides)
            for member in sorted(members)
        }
        levels = {tuple(p.level) for p in per_member.values()}
        if len(levels) > 1:
            raise _conflict(
                "members at different levels",
                f"resource {resource!r} produces "
                f"{ {m: p.level for m, p in per_member.items()} }",
                "Derivations are appended per resource at one level; produce "
                "the members at one level or align them through separate "
                "resources.",
            )
        out[resource] = per_member
    return out


def rekey_members(
    alignment: IdentityAlignment,
    *,
    sides: SideManifests,
    resolve: Callable[[Side, str], str],
) -> IdentityAlignment:
    """Name every member key as its side names the member.

    *resolve* maps ``(side, key)`` to the member it names — ``compose_manifests``
    passes the aligned cluster's resolution, so a member may be keyed by its
    own name or its canonical one. Unresolved keys pass through for the
    validator to report. Two keys naming one member under one resource are
    refused.
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

    attributes: list[AlignmentAttribute] = []
    for attribute in alignment.attributes:
        sources: dict[
            str,
            DerivationSpec
            | SharedDerivation
            | list[DerivationSpec]
            | dict[str, DerivationSpec],
        ] = {}
        for resource, spec in attribute.sources.items():
            if isinstance(spec, SharedDerivation):
                names = _names(resource, spec.members)
                members: list[str] | dict[str, dict[str, Any]] = (
                    [names[m] for m in spec.members]
                    if isinstance(spec.members, list)
                    else {names[m]: p for m, p in spec.members.items()}
                )
                sources[resource] = spec.model_copy(update={"members": members})
            elif isinstance(spec, dict):
                names = _names(resource, spec)
                sources[resource] = {names[m]: v for m, v in spec.items()}
            else:
                sources[resource] = spec
        attributes.append(attribute.model_copy(update={"sources": sources}))

    local_key = alignment.local_key
    if local_key is not None:
        local_sources: dict[
            str, LocalKeySource | list[LocalKeySource] | dict[str, LocalKeySource]
        ] = {}
        for resource, entry in local_key.sources.items():
            if isinstance(entry, dict):
                names = _names(resource, entry)
                local_sources[resource] = {names[m]: v for m, v in entry.items()}
            else:
                local_sources[resource] = entry
        local_key = local_key.model_copy(update={"sources": local_sources})

    return alignment.model_copy(
        update={"attributes": attributes, "local_key": local_key}
    )


def _require_sides(
    alignment: IdentityAlignment, sides: SideManifests | None
) -> SideManifests | None:
    if sides is None and _member_keyed_resources(alignment):
        raise _conflict(
            "member-keyed sources without sides",
            "sources keyed by member class need the pre-merge side manifests "
            "to resolve how each resource produces the member",
            "compose_manifests supplies them; when calling directly, pass "
            "sides={'left': ..., 'right': ...}.",
        )
    return sides


# --------------------------------------------------------------------------- #
# Levels
# --------------------------------------------------------------------------- #


def resolve_derivation_levels(
    alignment: IdentityAlignment,
    manifest: GraphManifest,
    *,
    productions: MemberProductions | None = None,
) -> dict[str, list[int]]:
    """Pipeline level each referenced resource derives at, keyed by resource.

    A derivation must land at the level that produces the aligned class: an
    actor reads its transform buffer at its own ``LocationIndex`` with no
    ancestor fallback, and a ``descend`` subtree runs before its own level's
    transforms. Placing it anywhere else derives nothing, silently.

    ``IdentityAlignment.at`` overrides the lookup. A member-keyed resource
    takes the level its members are produced at (see
    :func:`resolve_member_productions`). Otherwise a resource must produce the
    class at exactly one level — zero and several are both
    :class:`AlignmentConflictError`, because either answer the resolver could
    pick would be a guess about where the source fields live.
    """
    pipelines = _resource_pipelines(manifest)
    levels: dict[str, list[int]] = {}
    for resource in sorted(_referenced_resources(alignment)):
        pipeline = pipelines.get(resource, [])
        if resource in alignment.at:
            path = list(alignment.at[resource])
            try:
                level = resolve_pipeline_level(list(pipeline), path)
            except ValueError as exc:
                raise _conflict(
                    "unresolvable level",
                    f"`at` for resource {resource!r}: {exc}",
                    "Each index must address a descend step; [] is the root level.",
                ) from exc
            # An override that resolves but produces nothing is the failure this
            # resolution exists to prevent: the derivations would be appended,
            # run, find no inputs, and skip without a word.
            if not any(
                isinstance(step, dict)
                and alignment.vertex in step_produces_vertices(step)
                for step in level
            ):
                candidates = find_vertex_producing_levels(pipeline, alignment.vertex)
                raise _conflict(
                    "level produces nothing",
                    f"`at` sends resource {resource!r} derivations to level "
                    f"{path or 'root'}, which produces no {alignment.vertex!r}",
                    (
                        f"A transform is only visible to actors at its own "
                        f"level; {alignment.vertex!r} is produced at {candidates}."
                    )
                    if candidates
                    else f"This resource never produces {alignment.vertex!r}.",
                )
            levels[resource] = path
            continue

        if productions and resource in productions:
            # The members resolved to one level on the side; the union's
            # pipeline for this resource is the same object graph.
            levels[resource] = list(next(iter(productions[resource].values())).level)
            continue

        candidates = find_vertex_producing_levels(pipeline, alignment.vertex)
        if not candidates:
            raise _conflict(
                "resource does not produce the class",
                f"resource {resource!r} has no pipeline step producing "
                f"{alignment.vertex!r}",
                "An alignment derives canonical attributes for the documents "
                "that become this class; a resource that never produces it has "
                "nothing to derive.",
            )
        if len(candidates) > 1:
            raise _conflict(
                "ambiguous level",
                f"resource {resource!r} produces {alignment.vertex!r} at "
                f"levels {candidates}",
                f"Derivation inputs live at one level. Pick it with "
                f"IdentityAlignment(at={{{resource!r}: {candidates[0]}}}).",
            )
        levels[resource] = candidates[0]
    return levels


def _referenced_resources(alignment: IdentityAlignment) -> set[str]:
    names: set[str] = set(alignment.at)
    for attribute in alignment.attributes:
        names.update(attribute.sources)
    if alignment.local_key is not None:
        names.update(alignment.local_key.sources)
    return names


def _producing_steps(
    manifest: GraphManifest, resource: str, path: list[int], vertex: str
) -> list[dict]:
    """Normalized steps at *path* in *resource* that produce *vertex*."""
    pipeline = list(_resource_pipelines(manifest).get(resource, []))
    level = resolve_pipeline_level(pipeline, list(path))
    return [
        normalize_actor_step(dict(step))
        for step in level
        if isinstance(step, dict) and vertex in step_produces_vertices(step)
    ]


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def validate_alignment(
    alignment: IdentityAlignment,
    manifest: GraphManifest,
    *,
    canonical_maps: tuple[CanonicalMap, ...] | list[CanonicalMap] = (),
    sides: SideManifests | None = None,
    cluster_members: ClusterMembers | None = None,
) -> None:
    """Fail loudly when *alignment* contradicts *manifest* or the canonical maps.

    *manifest* is the composed union the alignment ops will be applied to.
    Pass the :class:`CanonicalMap`\\ s used to canonicalize the sides to catch
    derivation inputs written in canonical vocabulary: renamed documents still
    carry their raw field names, so a rename *target* used as a derivation
    input reads an absent field and silently derives nothing.

    *sides* are the pre-merge side manifests, required by member-keyed
    sources; *cluster_members* are the aligned cluster's members per side,
    which lets a member key be checked against the cluster it claims.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise AlignmentConflictError("identity alignment requires graph_schema")
    vertex_config = schema.core_schema.vertex_config
    if alignment.vertex not in vertex_config.vertex_set:
        raise _conflict(
            "unknown vertex",
            f"{alignment.vertex!r} is not defined in the manifest",
            f"Defined: {sorted(vertex_config.vertex_set)}.",
        )
    if manifest.ingestion_model is None:
        raise AlignmentConflictError(
            "identity alignment requires ingestion_model — derivations are "
            "resource pipeline steps"
        )
    known_resources = {r.name for r in manifest.ingestion_model.resources}

    resource_refs: set[str] = set()
    raw_inputs: dict[str, list[str]] = {}
    for attribute in alignment.attributes:
        resource_refs.update(attribute.sources)
        for resource in attribute.sources:
            for spec in attribute.specs_for(resource):
                raw_inputs.setdefault(resource, []).extend(spec.input)
    if alignment.local_key is not None:
        resource_refs.update(alignment.local_key.sources)
        for resource in alignment.local_key.sources:
            for src in alignment.local_key.sources_for(resource):
                raw_inputs.setdefault(resource, []).append(src.field)
                if src.gate is not None:
                    raw_inputs.setdefault(resource, []).append(src.gate)
    resource_refs.update(alignment.at)

    missing = sorted(resource_refs - known_resources)
    if missing:
        raise _conflict(
            "unknown resources",
            f"{missing} are not defined in the manifest",
            f"Defined: {sorted(known_resources)}.",
        )

    sides = _require_sides(alignment, sides)
    productions = (
        resolve_member_productions(alignment, sides) if sides is not None else {}
    )
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
                    f"{alignment.vertex!r} cluster on that side",
                    f"Members on the {side}: {sorted(allowed)}.",
                )
        for production in per_member.values():
            if production.type_field is not None:
                raw_inputs.setdefault(resource, []).append(production.type_field)

    current_identity = set(vertex_config.identity_fields(alignment.vertex))
    into_names = [attribute.into for attribute in alignment.attributes]
    if alignment.local_key is not None:
        into_names.append(alignment.local_key.into)
    colliding = sorted(set(into_names) & current_identity)
    if colliding:
        raise _conflict(
            "identity collision",
            f"target attributes {colliding} are already primary-identity "
            f"fields of {alignment.vertex!r}",
            "Pick canonical attribute names distinct from the current key; "
            "the alignment replaces the identity wholesale.",
        )

    declared = set(vertex_config.property_names(alignment.vertex))
    for name, fields in alignment.secondary_identities.items():
        undeclared = sorted(set(fields) - declared)
        if undeclared:
            raise _conflict(
                "undeclared secondary fields",
                f"secondary identity {name!r} references {undeclared}, not "
                f"declared on {alignment.vertex!r}",
                "Secondary identities index existing properties.",
            )

    rename_targets = _canonical_rename_targets(tuple(canonical_maps))
    if rename_targets:
        for resource, fields in raw_inputs.items():
            canonical_used = sorted(set(fields) & rename_targets)
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

    levels = resolve_derivation_levels(alignment, manifest, productions=productions)

    # Scratch fields exist only for the column-presence (list) form; a
    # member-keyed derivation is the single writer of its attribute.
    scratch_names = {
        _scratch_name(attribute.into, index)
        for attribute in alignment.attributes
        for resource in attribute.sources
        if attribute.members_for(resource) is None
        and len(attribute.specs_for(resource)) > 1
        for index in range(len(attribute.specs_for(resource)))
    }
    if alignment.local_key is not None:
        scratch_names |= {
            _scratch_name(alignment.local_key.into, index)
            for resource in alignment.local_key.sources
            if alignment.local_key.members_for(resource) is None
            and len(alignment.local_key.sources_for(resource)) > 1
            for index in range(len(alignment.local_key.sources_for(resource)))
        }
    colliding_scratch = sorted(scratch_names & declared)
    if colliding_scratch:
        raise _conflict(
            "scratch name collision",
            f"multi-branch derivations would write {colliding_scratch}, which "
            f"are declared properties of {alignment.vertex!r}",
            "Rename the property, or the canonical attribute the scratch "
            "names are derived from.",
        )

    for resource, path in sorted(levels.items()):
        for step in _producing_steps(manifest, resource, path, alignment.vertex):
            _check_sibling_classes(alignment, manifest, step, resource, into_names)
            _warn_on_one_derivation_for_several_members(alignment, step, resource)
    if sides is not None and cluster_members is not None:
        for resource in productions:
            _warn_on_partial_member_coverage(
                alignment, resource, sides, cluster_members
            )

    if alignment.local_key is None:
        logger.warning(
            "identity alignment for %r has no local_key: records matching no "
            "aligned attribute complete no funnel branch and are dropped",
            alignment.vertex,
        )


def _check_sibling_classes(
    alignment: IdentityAlignment,
    manifest: GraphManifest,
    step: dict,
    resource: str,
    into_names: list[str],
) -> None:
    """Refuse a canonical attribute name another routed class also declares.

    A router hands the whole merged observation to whichever class it selects,
    and extraction keeps a class's declared properties. So a sibling class
    declaring one of the canonical attribute names would silently absorb the
    value derived for the aligned class.
    """
    if step.get("type") != "vertex_router":
        return
    schema = manifest.graph_schema
    assert schema is not None
    vertex_config = schema.core_schema.vertex_config
    siblings = step_produces_vertices(step) - {alignment.vertex}
    for sibling in sorted(siblings):
        if sibling not in vertex_config.vertex_set:
            continue
        shared = sorted(set(into_names) & set(vertex_config.property_names(sibling)))
        if shared:
            raise _conflict(
                "canonical attribute claimed by a sibling class",
                f"resource {resource!r} routes to {sibling!r} at the same level "
                f"as {alignment.vertex!r}, and {sibling!r} declares {shared}",
                f"A router passes one observation to whichever class it picks, "
                f"so {sibling!r} would absorb the derived value. Rename the "
                f"canonical attribute, or the property on {sibling!r}.",
            )


def _warn_on_one_derivation_for_several_members(
    alignment: IdentityAlignment, step: dict, resource: str
) -> None:
    """Flag a router folding several members onto the class with one derivation.

    Legitimate when the members share a key column and a marker convention;
    wrong when each carries its own, which needs a derivation per member.
    """
    if step.get("type") != "vertex_router":
        return
    type_map = step.get("type_map") or {}
    branches = sorted(k for k, v in type_map.items() if v == alignment.vertex)
    if len(branches) < 2:
        return
    single = [
        attribute.into
        for attribute in alignment.attributes
        if attribute.members_for(resource) is None
        and len(attribute.specs_for(resource)) == 1
    ]
    local_key = alignment.local_key
    if (
        local_key is not None
        and local_key.members_for(resource) is None
        and len(local_key.sources_for(resource)) == 1
    ):
        single.append(local_key.into)
    if single:
        logger.warning(
            "identity alignment for %r: resource %r routes %s onto %r but "
            "derives %s one way — correct when those members share a key "
            "column, otherwise key the derivation by member",
            alignment.vertex,
            resource,
            branches,
            alignment.vertex,
            single,
        )


def _warn_on_partial_member_coverage(
    alignment: IdentityAlignment,
    resource: str,
    sides: SideManifests,
    cluster_members: ClusterMembers,
) -> None:
    """Flag a member the resource produces that no keyed derivation covers.

    Its records derive nothing for that attribute and key on whatever lower
    funnel branch they complete — possibly intended, never silent.
    """
    side, manifest = _side_of(resource, sides)
    produced = _produced_vertices(_resource_pipelines(manifest)[resource])
    in_cluster = produced & set(cluster_members.get(side, ()))
    targets: list[tuple[str, list[str] | None]] = [
        (attribute.into, attribute.members_for(resource))
        for attribute in alignment.attributes
    ]
    if alignment.local_key is not None:
        targets.append(
            (alignment.local_key.into, alignment.local_key.members_for(resource))
        )
    for into, members in targets:
        if members is None:
            continue
        uncovered = sorted(in_cluster - set(members))
        if uncovered:
            logger.warning(
                "identity alignment for %r: resource %r produces %s but derives "
                "%r only for %s — records of the uncovered members carry no %r",
                alignment.vertex,
                resource,
                sorted(in_cluster),
                into,
                sorted(members),
                into,
            )


# --------------------------------------------------------------------------- #
# Lowering
# --------------------------------------------------------------------------- #


def alignment_to_ops(
    alignment: IdentityAlignment,
    *,
    manifest: GraphManifest | None = None,
    canonical_maps: tuple[CanonicalMap, ...] | list[CanonicalMap] = (),
    sides: SideManifests | None = None,
    cluster_members: ClusterMembers | None = None,
) -> list[ManifestOp]:
    """Compose the alignment into an ordered list of fundamental ops.

    Apply the result to the composed union with
    :func:`~graflo.architecture.evolution.apply.apply_evolution`. When
    *manifest* is given, :func:`validate_alignment` runs first. Member-keyed
    sources need *sides* (the pre-merge manifests) to resolve how each
    resource produces each member; ``compose_manifests`` passes them.
    """
    if manifest is not None:
        validate_alignment(
            alignment,
            manifest,
            canonical_maps=canonical_maps,
            sides=sides,
            cluster_members=cluster_members,
        )
    sides = _require_sides(alignment, sides)
    productions = (
        resolve_member_productions(alignment, sides) if sides is not None else {}
    )

    ops: list[ManifestOp] = []

    into_names = [attribute.into for attribute in alignment.attributes]
    if alignment.local_key is not None:
        into_names.append(alignment.local_key.into)
    ops.append(AddVertexPropertiesOp(additions={alignment.vertex: list(into_names)}))

    if manifest is not None:
        levels = resolve_derivation_levels(alignment, manifest, productions=productions)
    else:
        levels = {
            resource: list(next(iter(per_member.values())).level)
            for resource, per_member in productions.items()
        }

    additions: dict[str, list[dict[str, Any]]] = {}
    for attribute in alignment.attributes:
        for resource in attribute.sources:
            additions.setdefault(resource, []).extend(
                _derivation_steps(
                    attribute.into,
                    attribute.specs_for(resource),
                    guards=_guards(
                        productions, resource, attribute.members_for(resource)
                    ),
                )
            )
    if alignment.local_key is not None:
        local_key = alignment.local_key
        for resource in local_key.sources:
            additions.setdefault(resource, []).extend(
                _local_key_steps(
                    local_key,
                    local_key.sources_for(resource),
                    guards=_guards(
                        productions, resource, local_key.members_for(resource)
                    ),
                )
            )
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

    if manifest is not None:
        ensure = _ensure_extracted_fields_op(alignment, manifest, levels, into_names)
        if ensure is not None:
            ops.append(ensure)

    # Always a funnel, even for a single attribute: gating means presence is never
    # guaranteed, and include_branch_id keeps branches collision-free.
    branches = [IdentityBranch(id=name, fields=[name]) for name in into_names]
    ops.append(
        ReplaceIdentityOp(
            replacements={
                alignment.vertex: IdentityReplacement(
                    to=FunnelIdentityTarget(funnel=IdentityFunnel(branches=branches)),
                    # The pre-alignment identity on a composed class is the
                    # merged union of the side keys — a field-set no record
                    # carries. Demoting it would index nothing; the per-side
                    # keys are demoted explicitly below instead.
                    retire="keep",
                )
            }
        )
    )

    if alignment.secondary_identities:
        ops.append(
            AddSecondaryIdentitiesOp(
                additions={
                    alignment.vertex: [
                        SecondaryIdentity(name=name, fields=list(fields))
                        for name, fields in sorted(
                            alignment.secondary_identities.items()
                        )
                    ]
                }
            )
        )

    return ops


def _guards(
    productions: MemberProductions, resource: str, members: list[str] | None
) -> list[dict[str, Any] | None] | None:
    """One guard per keyed member (``None`` for a plain ``vertex`` producer)."""
    if members is None:
        return None
    return [productions[resource][member].guard() for member in members]


def _call_step(
    *,
    module: str,
    foo: str,
    params: dict[str, Any],
    output: str,
    input_fields: list[str] | None = None,
    strategy: str | None = None,
    when: dict[str, Any] | None = None,
) -> dict[str, Any]:
    call: dict[str, Any] = {
        "module": module,
        "foo": foo,
        "params": params,
        "output": [output],
    }
    if input_fields is not None:
        call["input"] = list(input_fields)
    if strategy is not None:
        call["strategy"] = strategy
    transform: dict[str, Any] = {"call": call}
    if when is not None:
        transform["when"] = when
    return {"transform": transform}


def _coalesce_step(into: str, count: int) -> dict[str, Any]:
    """The single writer of *into*, picking whichever column-presence branch fired.

    ``strategy: all`` hands the function the whole merged observation and
    empties the missing-input guard, so a branch whose own columns are absent
    from a document skips without taking the coalesce down with it.
    """
    return _call_step(
        module="graflo.util.transform",
        foo="coalesce_fields",
        params={"fields": [_scratch_name(into, i) for i in range(count)]},
        output=into,
        strategy="all",
    )


def _derivation_steps(
    into: str,
    specs: list[DerivationSpec],
    *,
    guards: list[dict[str, Any] | None] | None = None,
) -> list[dict[str, Any]]:
    """Steps deriving *into* from *specs*, in order.

    Member-keyed (*guards* given): one step per member, each the single writer
    of *into* for the documents its guard admits — a guarded step that does
    not fire writes nothing, so nothing clobbers.

    Otherwise one spec writes ``into`` directly, and several (the
    column-presence form) write scratch fields that a coalesce reduces: two
    steps writing ``into`` would clobber behind a ``vertex_router``, which
    merges the transform buffer into one observation dict where a later
    ``None`` overwrites an earlier real value.
    """
    if guards is not None:
        return [
            _call_step(
                module=spec.module,
                foo=spec.foo,
                params=dict(spec.params),
                output=into,
                input_fields=list(spec.input),
                when=guard,
            )
            for spec, guard in zip(specs, guards, strict=True)
        ]
    if len(specs) == 1:
        spec = specs[0]
        return [
            _call_step(
                module=spec.module,
                foo=spec.foo,
                params=dict(spec.params),
                output=into,
                input_fields=list(spec.input),
            )
        ]
    steps = [
        _call_step(
            module=spec.module,
            foo=spec.foo,
            params=dict(spec.params),
            output=_scratch_name(into, index),
            input_fields=list(spec.input),
        )
        for index, spec in enumerate(specs)
    ]
    steps.append(_coalesce_step(into, len(specs)))
    return steps


def _local_key_call(src: LocalKeySource, sep: str) -> tuple[str, dict, list[str]]:
    """``(foo, params, input)`` for one local-key source, gated or not."""
    if src.gate is None:
        return "tagged_key", {"tag": src.tag, "sep": sep}, [src.field]
    return (
        "gated_tagged_key",
        {"tag": src.tag, "sep": sep, "prefix": src.gate_prefix},
        [src.gate, src.field],
    )


def _local_key_steps(
    local_key: LocalKeySpec,
    sources: list[LocalKeySource],
    *,
    guards: list[dict[str, Any] | None] | None = None,
) -> list[dict[str, Any]]:
    """Steps filling the local-key fallback for one resource, in order."""
    if guards is not None:
        steps = []
        for src, guard in zip(sources, guards, strict=True):
            foo, params, input_fields = _local_key_call(src, local_key.sep)
            steps.append(
                _call_step(
                    module="graflo.util.transform",
                    foo=foo,
                    params=params,
                    output=local_key.into,
                    input_fields=input_fields,
                    when=guard,
                )
            )
        return steps
    if len(sources) == 1:
        foo, params, input_fields = _local_key_call(sources[0], local_key.sep)
        return [
            _call_step(
                module="graflo.util.transform",
                foo=foo,
                params=params,
                output=local_key.into,
                input_fields=input_fields,
            )
        ]
    steps = []
    for index, src in enumerate(sources):
        foo, params, input_fields = _local_key_call(src, local_key.sep)
        steps.append(
            _call_step(
                module="graflo.util.transform",
                foo=foo,
                params=params,
                output=_scratch_name(local_key.into, index),
                input_fields=input_fields,
            )
        )
    steps.append(_coalesce_step(local_key.into, len(sources)))
    return steps


def _ensure_extracted_fields_op(
    alignment: IdentityAlignment,
    manifest: GraphManifest,
    levels: dict[str, list[int]],
    into_names: list[str],
) -> EnsureExtractedFieldsOp | None:
    """Widen restrictive routers so the canonical attributes reach the class.

    Only routers need this. A router's child ``VertexActor`` runs at a
    ``LocationIndex`` whose transform buffer is empty, so derived attributes
    arrive through the merged observation — subject to ``keep_fields`` and
    ``extraction_scope``. A plain ``vertex`` step reads the buffer directly and
    is unaffected, so it gets no entry.
    """
    additions: dict[str, list[EnsureExtractedFields]] = {}
    for resource, path in sorted(levels.items()):
        for step in _producing_steps(manifest, resource, path, alignment.vertex):
            if step.get("type") != "vertex_router":
                continue
            if step.get("keep_fields") is None and (
                step.get("extraction_scope") != "mapped_only"
            ):
                continue
            additions.setdefault(resource, []).append(
                EnsureExtractedFields(
                    vertex=alignment.vertex, fields=list(into_names), at=list(path)
                )
            )
    if not additions:
        return None
    return EnsureExtractedFieldsOp(additions=additions)
