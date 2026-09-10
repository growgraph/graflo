"""Canonical vocabulary maps: one composite map per side, applied once.

A :class:`~graflo.architecture.evolution.ops.CanonicalMap` is the author's
translation of a source vocabulary into canonical names — a partial function
on names, identity where unmapped. An equivalence cluster on a
:class:`~graflo.architecture.evolution.ops.ComposeManifestsOp` is the other
half of the same object: it says *which* classes are one (a partition), and
may leave the composed name to the map. Renames compose, so "canonicalize,
then declare equivalences in canonical names" and "declare equivalences in
raw names, then canonicalize" are the same function; :func:`resolve_clusters`
computes it directly — one composite ``CanonicalMap`` per side, lowered
through :func:`canonical_map_to_ops` to a single
:class:`~graflo.architecture.evolution.ops.CanonicalizeOp` — so there is no
intermediate vocabulary an author has to write in.

Consistency is one rule: the canonical map and the equivalences must agree on
where every name goes, and a canonical target is a fixed point neither may
re-map. Every refusal below is an instance of that rule, plus the
cluster-shape checks of :mod:`~graflo.architecture.evolution.equivalence`.
:func:`merge_canonical_maps` is the union primitive two author maps for one
scope reconcile through.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from graflo.architecture.contract.manifest import GraphManifest

from .equivalence import (
    Cluster,
    ClusterConflictError,
    ClusterIndex,
    ClusterSpec,
    Side,
    check_member_existence,
    index_clusters,
)
from .ops import (
    CanonicalizeOp,
    CanonicalMap,
    ComposeManifestsOp,
    ManifestOp,
    RelationEquivalence,
    VertexEquivalence,
)

__all__ = [
    "CanonicalMap",
    "ClusterResolution",
    "ComposeCanonicalConflictError",
    "Scope",
    "SideMaps",
    "SideNames",
    "canonical_map_to_ops",
    "clusters_to_side_maps",
    "merge_canonical_maps",
    "resolve_clusters",
    "validate_and_complete_canonical_map",
]

logger = logging.getLogger(__name__)

#: Where a canonical map applies: one side's own names, or either side's.
Scope = Literal["left", "right", "both"]

_SIDES: tuple[Side, ...] = ("left", "right")


class ComposeCanonicalConflictError(ValueError):
    """A compose op's declared clusters contradict the canonical map(s) they were authored against."""


@dataclass(frozen=True)
class SideMaps:
    """A ``(left, right)`` :class:`CanonicalMap` pair for one compose op."""

    left: CanonicalMap
    right: CanonicalMap

    def __getitem__(self, side: Side) -> CanonicalMap:
        return self.left if side == "left" else self.right


def _conflict(check: str, detail: str, hint: str) -> ComposeCanonicalConflictError:
    return ComposeCanonicalConflictError(
        f"compose contradicts the canonical map ({check}): {detail}. {hint}"
    )


def merge_canonical_maps(base: CanonicalMap, extension: CanonicalMap) -> CanonicalMap:
    """Partial-function union: *base* wins, its targets are fixed points.

    Every source named by *base* or *extension* maps to exactly one target; a
    source the two disagree on raises :class:`ComposeCanonicalConflictError`.
    A **target** of *base* is additionally a fixed point — *extension* may not
    re-map it to anything else — so two author maps for one scope reconcile
    without one silently overriding the other. ``properties`` union the same
    way per source class.
    """
    vertices = dict(base.vertices)
    for source, target in extension.vertices.items():
        existing = vertices.get(source)
        if existing is not None and existing != target:
            raise _conflict(
                "canonical vertex clash",
                f"{source!r} maps to both {existing!r} and {target!r}",
                "Reconcile the canonical maps.",
            )
        vertices[source] = target
    for fixed in base.vertices.values():
        remapped = extension.vertices.get(fixed, fixed)
        if remapped != fixed:
            raise _conflict(
                "canonical vertex re-target",
                f"the canonical map's target {fixed!r} would be re-mapped to "
                f"{remapped!r}",
                "Canonical targets are fixed points.",
            )

    relations = dict(base.relations)
    for source, target in extension.relations.items():
        existing = relations.get(source)
        if existing is not None and existing != target:
            raise _conflict(
                "canonical relation clash",
                f"{source!r} maps to both {existing!r} and {target!r}",
                "Reconcile the canonical maps.",
            )
        relations[source] = target
    for fixed in base.relations.values():
        remapped = extension.relations.get(fixed, fixed)
        if remapped != fixed:
            raise _conflict(
                "canonical relation re-target",
                f"the canonical map's target {fixed!r} would be re-mapped to "
                f"{remapped!r}",
                "Canonical targets are fixed points.",
            )

    properties: dict[str, dict[str, str]] = {
        cls: dict(attrs) for cls, attrs in base.properties.items()
    }
    for source_class, attr_map in extension.properties.items():
        bucket = properties.setdefault(source_class, {})
        for old, new in attr_map.items():
            existing = bucket.get(old)
            if existing is not None and existing != new:
                raise _conflict(
                    "canonical property clash",
                    f"{source_class}.{old} maps to both {existing!r} and {new!r}",
                    "Reconcile the canonical maps.",
                )
            bucket[old] = new

    return CanonicalMap(
        vertices=vertices,
        relations=relations,
        properties=properties,
        allow_merges=base.allow_merges or extension.allow_merges,
    )


def canonical_map_to_ops(
    cm: CanonicalMap,
    *,
    allow_self_relations: bool = False,
    allow_observation_fusion: bool = False,
) -> list[ManifestOp]:
    """Lower a canonical map to its single op.

    A canonical map is a function on names, and
    :class:`~graflo.architecture.evolution.ops.CanonicalizeOp` applies exactly
    that function in one step: attribute renames (keyed by the source class)
    first, then classes and relations simultaneously, so no op order can leak
    into the result. A map with no effective entry lowers to no op at all. A
    group of more than one class or relation is a merge and is refused unless
    ``allow_merges`` is set.
    """
    effective = (
        any(source != target for source, target in cm.vertices.items())
        or any(source != target for source, target in cm.relations.items())
        or any(
            old != new
            for attr_map in cm.properties.values()
            for old, new in attr_map.items()
        )
    )
    if not effective:
        return []
    return [
        CanonicalizeOp(
            vertices=dict(cm.vertices),
            properties={cls: dict(attrs) for cls, attrs in cm.properties.items()},
            relations=dict(cm.relations),
            allow_merges=cm.allow_merges,
            allow_self_relations=allow_self_relations,
            allow_observation_fusion=allow_observation_fusion,
        )
    ]


# ── resolution ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SideNames:
    """The class and relation names one side actually declares."""

    vertices: frozenset[str]
    relations: frozenset[str]

    @classmethod
    def of(cls, manifest: GraphManifest) -> SideNames:
        schema = manifest.graph_schema
        if schema is None:
            return cls(vertices=frozenset(), relations=frozenset())
        return cls(
            vertices=frozenset(schema.core_schema.vertex_config.vertex_set),
            relations=frozenset(
                edge.relation
                for edge in schema.core_schema.edge_config.edges
                if edge.relation is not None
            ),
        )


@dataclass(frozen=True)
class ClusterResolution:
    """What compose applies: the resolved clusters and one composite map per side.

    ``author`` is the folded author vocabulary as seen from each side
    (``both`` under the side's own map); ``side_maps`` is the composite —
    every cluster member onto its composed name, every other author entry as
    declared — which :func:`canonical_map_to_ops` lowers to one op per side.
    """

    index: ClusterIndex
    side_maps: SideMaps
    author: SideMaps


def _fold_author_maps(
    op: ComposeManifestsOp, extra: Sequence[tuple[Side, CanonicalMap]]
) -> SideMaps:
    scoped: dict[str, CanonicalMap] = {
        "left": CanonicalMap(),
        "right": CanonicalMap(),
        "both": CanonicalMap(),
    }
    for scope, cm in op.canonical_maps.items():
        scoped[scope] = merge_canonical_maps(scoped[scope], cm)
    for side, cm in extra:
        scoped[side] = merge_canonical_maps(scoped[side], cm)
    return SideMaps(
        left=merge_canonical_maps(scoped["both"], scoped["left"]),
        right=merge_canonical_maps(scoped["both"], scoped["right"]),
    )


def _mapping(cm: CanonicalMap, kind: str) -> dict[str, str]:
    return cm.vertices if kind == "vertex" else cm.relations


def _resolve_member(
    declared: str,
    *,
    names: frozenset[str],
    mapping: Mapping[str, str],
    side: Side,
    kind: str,
) -> str:
    """A member spelled by its own name, or by its canonical name when that names one source."""
    if declared in names:
        return declared
    sources = sorted(
        source
        for source, target in mapping.items()
        if target == declared and source != target and source in names
    )
    if len(sources) == 1:
        return sources[0]
    if len(sources) > 1:
        raise _conflict(
            f"ambiguous {kind} member",
            f"{side} {kind} {declared!r} is the canonical name of {sources}",
            "Name the member by its own spelling.",
        )
    return declared  # reported by the existence check


def _resolve_cluster(
    declaration: VertexEquivalence | RelationEquivalence,
    *,
    kind: str,
    author: SideMaps,
    names: Mapping[Side, SideNames],
) -> ClusterSpec:
    """Resolve one declaration's members and composed name against the author maps.

    The composed name is ``into`` (translated through the maps when they map
    it), else the canonical name the maps give a member, else the one
    spelling every member shares. Then the commutation rule: every member the
    maps have an opinion about — a mapped source, or a canonical target, which
    is a fixed point — must agree with that name.
    """
    mapping = {side: _mapping(author[side], kind) for side in _SIDES}
    side_names = {
        side: names[side].vertices if kind == "vertex" else names[side].relations
        for side in _SIDES
    }
    targets = {side: {t for s, t in mapping[side].items() if s != t} for side in _SIDES}

    members: dict[Side, list[str]] = {}
    aliases: dict[Side, dict[str, str]] = {}
    for side in _SIDES:
        resolved: list[str] = []
        for declared in declaration.members(side):
            name = _resolve_member(
                declared,
                names=side_names[side],
                mapping=mapping[side],
                side=side,
                kind=kind,
            )
            resolved.append(name)
            if name != declared:
                aliases.setdefault(side, {})[declared] = name
        members[side] = resolved
        # A member may also be named by its canonical name wherever the
        # declaration keys by member — unless that name is a real class on the
        # side, or the canonical name of two members.
        canonical = [mapping[side].get(m, m) for m in resolved]
        for member, name in zip(resolved, canonical, strict=True):
            if name == member or name in side_names[side] or canonical.count(name) > 1:
                continue
            aliases.setdefault(side, {}).setdefault(name, member)

    opinions: dict[str, list[str]] = {}
    for side in _SIDES:
        for member in members[side]:
            mapped = mapping[side].get(member)
            if mapped is not None and mapped != member:
                opinions.setdefault(mapped, []).append(f"{side}:{member}")
            elif member in targets[side]:
                opinions.setdefault(member, []).append(f"{side}:{member}")
    if len(opinions) > 1:
        raise _conflict(
            f"{kind} disagreement",
            "the canonical maps name the members of one cluster differently: "
            f"{dict(sorted(opinions.items()))}",
            "One composed class has one canonical name — fix the canonical "
            "map, or split the cluster.",
        )

    if declaration.into is not None:
        translated = {
            mapping[side][declaration.into]
            for side in _SIDES
            if mapping[side].get(declaration.into, declaration.into) != declaration.into
        }
        if len(translated) > 1:
            raise _conflict(
                f"ambiguous {kind} label",
                f"the canonical maps disagree on {declaration.into!r}: "
                f"{sorted(translated)}",
                "Reconcile the maps, or name the composed class by its canonical name.",
            )
        label = translated.pop() if translated else declaration.into
    elif opinions:
        label = next(iter(opinions))
    else:
        spellings = {member for side in _SIDES for member in members[side]}
        if len(spellings) != 1:
            raise _conflict(
                f"unnamed {kind} cluster",
                f"cluster {members['left']} ~ {members['right']} has no composed name",
                "Give it `into`, or map a member in a canonical map.",
            )
        label = spellings.pop()

    if opinions and label not in opinions:
        ((canonical, who),) = opinions.items()
        raise _conflict(
            f"{kind} disagreement",
            f"the canonical map says {', '.join(who)} is {canonical!r}, but the "
            f"equivalence names the composed {kind} {label!r}",
            "The two declarations must agree on where a name goes; canonical "
            "names are fixed points, so set `into` to the canonical name or "
            "fix the canonical map.",
        )
    return ClusterSpec(
        left=tuple(members["left"]),
        right=tuple(members["right"]),
        into=label,
        aliases=aliases,
    )


def _cluster_maps(index: ClusterIndex, side: Side) -> CanonicalMap:
    """Every cluster member onto its composed name, with the declared attribute maps."""
    vertices: dict[str, str] = {}
    properties: dict[str, dict[str, str]] = {}
    for cluster in index.vertices:
        for member in cluster.members(side):
            vertices[member] = cluster.into
        for member, attr_map in cluster.property_maps(side).items():
            properties.setdefault(member, {}).update(attr_map)
    relations: dict[str, str] = {}
    for cluster in index.relations:
        for member in cluster.members(side):
            relations[member] = cluster.into
    return CanonicalMap(
        vertices=vertices, relations=relations, properties=properties, allow_merges=True
    )


def clusters_to_side_maps(index: ClusterIndex, *, allow_merges: bool) -> SideMaps:
    """Lower every cluster of *index* into a pair of per-side canonical maps.

    Every member maps to its cluster's composed name, **including** a member
    that already equals it — that self entry declares the name a member of its
    own group, so :class:`~graflo.architecture.evolution.ops.CanonicalizeOp`
    merges into it rather than refusing an occupied target.
    """
    return SideMaps(
        left=_cluster_maps(index, "left").model_copy(
            update={"allow_merges": allow_merges}
        ),
        right=_cluster_maps(index, "right").model_copy(
            update={"allow_merges": allow_merges}
        ),
    )


def _extend_with_author(
    out: dict[str, str],
    mapping: Mapping[str, str],
    names: frozenset[str],
    labels: frozenset[str],
    *,
    side: Side,
    kind: str,
) -> None:
    """Add the author's entries for non-members to a side's composite map."""
    for source, target in mapping.items():
        if source in out:
            continue  # a member: the commutation rule already holds
        if source not in names:
            if target in names or source in labels:
                # Already applied by the caller, or a composed name spelled in
                # the author's raw terms: nothing left to do on this side.
                continue
            raise _conflict(
                "dangling entry",
                f"the canonical map's {side} entry {source!r} -> {target!r} "
                f"matches no {kind} on that side",
                "Check the spelling, or drop the entry.",
            )
        if target in labels and target != source:
            raise _conflict(
                f"{kind} merge into a composed class",
                f"the canonical map merges {side}:{source!r} into {target!r}, "
                "the composed name of a declared cluster",
                f"Declare {source!r} as a member of that cluster — a {kind} "
                "joining a composed class is governed by the cluster's identity "
                "and property maps.",
            )
        out[source] = target


def _composite_side_maps(
    index: ClusterIndex,
    author: SideMaps,
    names: Mapping[Side, SideNames],
    *,
    allow_merges: bool,
) -> SideMaps:
    maps: dict[Side, CanonicalMap] = {}
    for side in _SIDES:
        cm = author[side]
        base = _cluster_maps(index, side)
        vertices = dict(base.vertices)
        relations = dict(base.relations)
        properties = {cls: dict(attrs) for cls, attrs in base.properties.items()}
        _extend_with_author(
            vertices,
            cm.vertices,
            names[side].vertices,
            index.labels,
            side=side,
            kind="vertex",
        )
        _extend_with_author(
            relations,
            cm.relations,
            names[side].relations,
            index.relation_labels,
            side=side,
            kind="relation",
        )
        for cls, attrs in cm.properties.items():
            if cls not in names[side].vertices:
                if (
                    cm.canonical_class(cls) in names[side].vertices
                    or cls in index.labels
                ):
                    continue  # renamed away already, or a composed name in raw terms
                raise _conflict(
                    "dangling entry",
                    f"the canonical map's {side} attribute map for {cls!r} "
                    "matches no class on that side",
                    "Check the spelling, or drop the entry.",
                )
            bucket = properties.setdefault(cls, {})
            for old, new in attrs.items():
                existing = bucket.get(old)
                if existing is not None and existing != new:
                    raise _conflict(
                        "property disagreement",
                        f"the canonical map says {side}:{cls}.{old} -> {new!r}, "
                        f"but the equivalence maps it to {existing!r}",
                        "The two declarations must agree on where an attribute "
                        "goes; fix one of them.",
                    )
                bucket[old] = new
        maps[side] = CanonicalMap(
            vertices=vertices,
            relations=relations,
            properties=properties,
            allow_merges=allow_merges or cm.allow_merges,
        )
    return SideMaps(left=maps["left"], right=maps["right"])


def _check_property_fields_exist(
    manifest: GraphManifest, cluster: Cluster, *, side: Side, author: CanonicalMap
) -> None:
    """Every field a property equivalence names must exist on its member, as spelled."""
    schema = manifest.graph_schema
    if schema is None or not isinstance(cluster.declaration, VertexEquivalence):
        return
    vertex_config = schema.core_schema.vertex_config
    for pe in cluster.declaration.properties:
        spec = pe.left if side == "left" else pe.right
        if spec is None:
            continue
        per_member = (
            {member: spec for member in cluster.members(side)}
            if isinstance(spec, str)
            else {cluster.resolved(side, m): f for m, f in spec.items()}
        )
        for member, field in per_member.items():
            if member not in vertex_config.vertex_set:
                continue  # the existence check reports the member
            if field in vertex_config.property_names(member):
                continue
            sources = sorted(
                old
                for old, new in author.properties.get(member, {}).items()
                if new == field and old != new
            )
            hint = (
                f"{field!r} is the canonical name of {member}.{sources[0]!r} — "
                "name the source field"
                if sources
                else "Check the spelling, or declare the property first with "
                "AddVertexPropertiesOp"
            )
            raise _conflict(
                "unknown property",
                f"property equivalence into {pe.into!r} names "
                f"{side}:{member}.{field!r}, which is not a declared property",
                hint + ".",
            )


def _check_attribute_fixed_points(
    cluster: Cluster, *, side: Side, author: CanonicalMap
) -> None:
    """A canonical attribute the map established on a member may not be renamed by the cluster."""
    for member, attr_map in cluster.property_maps(side).items():
        canonical = author.canonical_property_names(member)
        for old, new in attr_map.items():
            if old in canonical and new != old:
                raise _conflict(
                    "property re-target",
                    f"the canonical map established {old!r} on {side}:{member}, "
                    f"but the equivalence renames it to {new!r}",
                    "Canonical attributes are fixed points: align the other "
                    "members onto the canonical name, or fix the canonical map.",
                )


def _check_property_maps_against_manifest(
    manifest: GraphManifest, cm: CanonicalMap, *, side: Side
) -> None:
    """Refuse a property rename whose old name is absent or whose new name collides.

    A collision would otherwise drop the losing field at apply time; this
    turns it into a loud compose-time error instead of a quiet data loss.
    """
    schema = manifest.graph_schema
    if schema is None:
        return
    vertex_config = schema.core_schema.vertex_config
    for member, attr_map in cm.properties.items():
        if member not in vertex_config.vertex_set:
            continue  # reported by the existence check
        existing = set(vertex_config.property_names(member))
        surviving = existing - set(attr_map)
        for old, new in attr_map.items():
            if old == new:
                continue
            if old not in existing:
                raise _conflict(
                    "unknown property",
                    f"the map renames {side}:{member}.{old!r}, which is not a "
                    "declared property",
                    "Check the spelling, or declare the property first with "
                    "AddVertexPropertiesOp.",
                )
            if new in surviving:
                raise _conflict(
                    "property rename collision",
                    f"{side}:{member}.{old!r} -> {new!r} collides with an "
                    "existing property of that name",
                    "A property rename cannot merge fields — align them "
                    "explicitly via PropertyEquivalence on both sides instead.",
                )


def _occupancy(names: frozenset[str], mapping: Mapping[str, str]) -> frozenset[str]:
    """Names a composed name could collide with: those the author map does not move away."""
    return names - {source for source, target in mapping.items() if source != target}


def resolve_clusters(
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> ClusterResolution:
    """Resolve *op*'s clusters against its canonical maps and build the per-side composite.

    *canonical_maps* are extra ``(side, map)`` pairs folded into
    ``op.canonical_maps``. *left* / *right* are the manifests about to be
    composed, in whatever vocabulary they are in: an author entry whose source
    is absent on its side but whose target is present is already applied and
    is a no-op; one matching nothing is refused as a typo.

    Raises :class:`~graflo.architecture.evolution.equivalence.ClusterConflictError`
    when the declared clusters themselves conflict, and
    :class:`ComposeCanonicalConflictError` when a map and the equivalences
    disagree: a member the map sends elsewhere than the composed name, a
    canonical class or attribute re-targeted by a cluster, a cluster with no
    name, a map entry merging a non-member into a composed class, a dangling
    entry, and property equivalences naming absent or colliding fields.
    """
    author = _fold_author_maps(op, canonical_maps)
    names: dict[Side, SideNames] = {
        "left": SideNames.of(left),
        "right": SideNames.of(right),
    }

    vertex_specs = [
        _resolve_cluster(v, kind="vertex", author=author, names=names)
        for v in op.vertex_equivalences
    ]
    relation_specs = [
        _resolve_cluster(r, kind="relation", author=author, names=names)
        for r in op.relation_equivalences
    ]
    check_member_existence(
        vertex_specs,
        relation_specs,
        left_vertex_names=names["left"].vertices,
        right_vertex_names=names["right"].vertices,
        left_relation_names=names["left"].relations,
        right_relation_names=names["right"].relations,
    )
    index = index_clusters(
        op,
        left_vertices=_occupancy(names["left"].vertices, author.left.vertices),
        right_vertices=_occupancy(names["right"].vertices, author.right.vertices),
        left_relations=_occupancy(names["left"].relations, author.left.relations),
        right_relations=_occupancy(names["right"].relations, author.right.relations),
        vertex_specs=vertex_specs,
        relation_specs=relation_specs,
    )
    manifests: dict[Side, GraphManifest] = {"left": left, "right": right}
    for side in _SIDES:
        for cluster in index.vertices:
            _check_property_fields_exist(
                manifests[side], cluster, side=side, author=author[side]
            )
            _check_attribute_fixed_points(cluster, side=side, author=author[side])
    side_maps = _composite_side_maps(index, author, names, allow_merges=op.allow_merges)
    for side in _SIDES:
        _check_property_maps_against_manifest(
            manifests[side], side_maps[side], side=side
        )
    return ClusterResolution(index=index, side_maps=side_maps, author=author)


def validate_and_complete_canonical_map(
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> SideMaps:
    """Validate *op* against its canonical maps and return the completed per-side maps.

    The composed name of every cluster is completed — from ``into``, the
    canonical map, or the members' shared spelling — and every member maps
    onto it; the author's remaining entries are carried as declared. Apply
    the result to each side with :func:`canonical_map_to_ops` before the
    schema/resource union, which is what
    :func:`~graflo.architecture.evolution.compose.compose_manifests` does.

    Raises :class:`ComposeCanonicalConflictError` for every refusal of
    :func:`resolve_clusters`, wrapping a
    :class:`~graflo.architecture.evolution.equivalence.ClusterConflictError`
    when the declared clusters themselves conflict.
    """
    try:
        return resolve_clusters(
            op, left=left, right=right, canonical_maps=canonical_maps
        ).side_maps
    except ClusterConflictError as exc:
        raise ComposeCanonicalConflictError(
            f"compose contradicts the canonical map (cluster conflict): {exc}"
        ) from exc
