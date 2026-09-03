"""Canonical vocabulary maps: derive rename ops and validate compose against them.

A :class:`CanonicalMap` is the author-declared translation of class, property
and relation names into a canonical target vocabulary. It is a *partial
function* ``{C1, C2, ...} -> C_canon`` — sources may come from either side of
a compose. Two moments of a union build consume it:

1. :func:`canonical_map_to_ops` turns the map into unary evolution ops that
   canonicalize one source manifest standalone (property renames first, then
   class/relation merges/renames), ready for
   :func:`~graflo.architecture.evolution.apply.apply_evolution`.
2. :func:`validate_and_complete_canonical_map` cross-checks a
   :class:`~graflo.architecture.evolution.ops.ComposeManifestsOp`'s *declared
   clusters* against maps from either side *before*
   :func:`~graflo.architecture.evolution.compose.compose_manifests` runs, and
   **lowers** every declared cluster into a per-side :class:`CanonicalMap`
   (:class:`SideMaps`) — the same shape a manually-authored ``CanonicalMap``
   has, so both moments share one lowering primitive
   (:func:`canonical_map_to_ops`) instead of compose re-implementing rename /
   merge resolution on its own.

:func:`merge_canonical_maps` is the one conflict primitive both moments rest
on: a side-aware partial-function union where a *target* of the first map is a
fixed point the second may not re-map. Like compose itself, this module
applies declared maps deterministically; it never infers semantic matches
beyond that union.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

from .equivalence import (
    Cluster,
    ClusterConflictError,
    ClusterIndex,
    Side,
    index_clusters,
)
from .ops import (
    ComposeManifestsOp,
    ManifestOp,
    MergeEdgesOp,
    MergeVerticesOp,
    RenameRelationsOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    validate_rename_map_is_injective,
)

logger = logging.getLogger(__name__)


class ComposeCanonicalConflictError(ValueError):
    """A compose op's declared clusters contradict the canonical map(s) they were authored against."""


class CanonicalMap(ConfigBaseModel):
    """Declared translation of a source vocabulary into canonical names.

    ``vertices`` maps source class names to canonical class names; a class
    absent from the map keeps its name (identity mapping). ``relations`` is
    the same shape for relation names. ``properties`` maps, per *source*
    class name, source attribute names to canonical attribute names —
    including for classes whose name does not change.

    When used with compose validation, source names may come from either
    manifest side; the side is supplied separately as
    ``(side, CanonicalMap)`` pairs to :func:`validate_and_complete_canonical_map`.
    """

    vertices: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Class rename map: ``{source_class: canonical_class}``.",
    )
    properties: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-source-class attribute rename map: "
            "``{source_class: {source_attr: canonical_attr}}``."
        ),
    )
    relations: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Relation rename map: ``{source_relation: canonical_relation}``.",
    )
    allow_merges: bool = PydanticField(
        default=False,
        description=(
            "Accept a non-injective ``vertices`` / ``relations`` map. Two "
            "sources sharing a canonical target is a *merge*, not a rename; "
            "it must be a stated intent because merging fuses entities and "
            "can create self-relations. When set, :func:`canonical_map_to_ops` "
            "emits ``MergeVerticesOp`` / ``MergeEdgesOp`` for the collapsed "
            "groups."
        ),
    )

    @model_validator(mode="after")
    def _validate_maps(self) -> CanonicalMap:
        if not self.allow_merges:
            # Identity entries (source == target) are excluded: a lowered
            # cluster map deliberately carries one for every member, including
            # `into` itself, so `canonical_map_to_ops` can build a merge group
            # rather than a colliding rename -- that self-entry must not read
            # as a collision.
            validate_rename_map_is_injective(
                {s: t for s, t in self.vertices.items() if s != t},
                kind="canonical vertex",
                merge_hint="CanonicalMap(allow_merges=True)",
            )
            validate_rename_map_is_injective(
                {s: t for s, t in self.relations.items() if s != t},
                kind="canonical relation",
                merge_hint="CanonicalMap(allow_merges=True)",
            )
        for source_class, attr_map in self.properties.items():
            validate_rename_map_is_injective(
                attr_map,
                kind=f"canonical property (class {source_class!r})",
                merge_hint="a transform that combines the fields upstream",
            )
        return self

    def canonical_class(self, source_class: str) -> str:
        """Canonical name of *source_class* (itself when unmapped)."""
        return self.vertices.get(source_class, source_class)

    def canonical_relation(self, source_relation: str) -> str:
        """Canonical name of *source_relation* (itself when unmapped)."""
        return self.relations.get(source_relation, source_relation)

    @property
    def stale_class_names(self) -> set[str]:
        """Source class names that no longer exist after canonicalization."""
        return {
            source
            for source, target in self.vertices.items()
            if source != target and source not in self.vertices.values()
        }

    @property
    def stale_relation_names(self) -> set[str]:
        """Source relation names that no longer exist after canonicalization."""
        return {
            source
            for source, target in self.relations.items()
            if source != target and source not in self.relations.values()
        }

    def stale_property_names(self, canonical_class: str) -> set[str]:
        """Source attr names retired on *canonical_class* after canonicalization."""
        stale: set[str] = set()
        for source_class, attr_map in self.properties.items():
            if self.canonical_class(source_class) != canonical_class:
                continue
            targets = set(attr_map.values())
            stale.update(
                source
                for source, target in attr_map.items()
                if source != target and source not in targets
            )
        return stale

    def canonical_property_names(self, canonical_class: str) -> set[str]:
        """Canonical attr names the map establishes on *canonical_class*."""
        names: set[str] = set()
        for source_class, attr_map in self.properties.items():
            if self.canonical_class(source_class) == canonical_class:
                names.update(attr_map.values())
        return names


@dataclass(frozen=True)
class SideMaps:
    """The lowered ``(left, right)`` :class:`CanonicalMap` pair for one compose op."""

    left: CanonicalMap
    right: CanonicalMap

    def __getitem__(self, side: Side) -> CanonicalMap:
        return self.left if side == "left" else self.right


def merge_canonical_maps(base: CanonicalMap, extension: CanonicalMap) -> CanonicalMap:
    """Side-aware partial-function union: *base* wins, its targets are fixed points.

    Every source named by *base* or *extension* maps to exactly one target; a
    source the two disagree on raises :class:`ComposeCanonicalConflictError`.
    A **target** of *base* is additionally a fixed point — *extension* may not
    re-map it to anything else. This is what lets an author-declared
    canonical map "seed" a lowered cluster map (or a second author map)
    without silently being overridden: ``Company`` established as a target by
    one map cannot be re-mapped to ``Party`` by another.

    ``properties`` union the same way per source class, without the
    fixed-point rule (that check is cluster-anchored — see
    :func:`~graflo.architecture.evolution.canonical._check_cluster_properties`
    — because a property's canonical target is meaningful only relative to
    the composed class it ends up on, not to the raw member name a
    ``properties`` dict happens to be keyed by).
    """
    vertices = dict(base.vertices)
    for source, target in extension.vertices.items():
        existing = vertices.get(source)
        if existing is not None and existing != target:
            raise _conflict(
                "canonical vertex clash",
                f"{source!r} maps to both {existing!r} and {target!r}",
                "Reconcile the canonical map with the equivalence cluster.",
            )
        vertices[source] = target
    for fixed in base.vertices.values():
        remapped = extension.vertices.get(fixed, fixed)
        if remapped != fixed:
            raise _conflict(
                "canonical vertex re-target",
                f"the canonical map's target {fixed!r} would be re-mapped to "
                f"{remapped!r}",
                "Canonical targets are fixed points — set `into` to the "
                "canonical name, or fix the canonical map.",
            )

    relations = dict(base.relations)
    for source, target in extension.relations.items():
        existing = relations.get(source)
        if existing is not None and existing != target:
            raise _conflict(
                "canonical relation clash",
                f"{source!r} maps to both {existing!r} and {target!r}",
                "Reconcile the canonical map with the equivalence cluster.",
            )
        relations[source] = target
    for fixed in base.relations.values():
        remapped = extension.relations.get(fixed, fixed)
        if remapped != fixed:
            raise _conflict(
                "canonical relation re-target",
                f"the canonical map's target {fixed!r} would be re-mapped to "
                f"{remapped!r}",
                "Canonical targets are fixed points — set `into` to the "
                "canonical name, or fix the canonical map.",
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
                    "Reconcile the canonical map with the equivalence cluster.",
                )
            bucket[old] = new

    return CanonicalMap(
        vertices=vertices,
        relations=relations,
        properties=properties,
        allow_merges=base.allow_merges or extension.allow_merges,
    )


def clusters_to_side_maps(index: ClusterIndex, *, allow_merges: bool) -> SideMaps:
    """Lower every declared cluster of *index* into a pair of per-side canonical maps.

    Every member maps to its cluster's ``into`` label, **including** a member
    that already equals ``into`` — that self entry is what makes
    :func:`canonical_map_to_ops` build a ``MergeVerticesOp`` group (rather
    than a colliding rename) when ``into`` is itself one of the members.
    """
    left_vertices: dict[str, str] = {}
    right_vertices: dict[str, str] = {}
    left_properties: dict[str, dict[str, str]] = {}
    right_properties: dict[str, dict[str, str]] = {}
    for cluster in index.vertices:
        for member in cluster.left:
            left_vertices[member] = cluster.into
        for member in cluster.right:
            right_vertices[member] = cluster.into
        for member, attr_map in cluster.declaration.property_maps("left").items():
            left_properties.setdefault(member, {}).update(attr_map)
        for member, attr_map in cluster.declaration.property_maps("right").items():
            right_properties.setdefault(member, {}).update(attr_map)

    left_relations: dict[str, str] = {}
    right_relations: dict[str, str] = {}
    for cluster in index.relations:
        for member in cluster.left:
            left_relations[member] = cluster.into
        for member in cluster.right:
            right_relations[member] = cluster.into

    return SideMaps(
        left=CanonicalMap(
            vertices=left_vertices,
            relations=left_relations,
            properties=left_properties,
            allow_merges=allow_merges,
        ),
        right=CanonicalMap(
            vertices=right_vertices,
            relations=right_relations,
            properties=right_properties,
            allow_merges=allow_merges,
        ),
    )


def canonical_map_to_ops(
    cm: CanonicalMap,
    *,
    allow_self_relations: bool = False,
    allow_row_fusion: bool = False,
) -> list[ManifestOp]:
    """Turn a canonical map into unary evolution ops.

    Order matters: property renames are keyed by the *source* class names, so
    they come first; class merges (when ``allow_merges``) and renames follow;
    relation merges and renames come last. Identity entries (``old == new``)
    are dropped — the returned list applies cleanly via
    :func:`~graflo.architecture.evolution.apply.apply_evolution` /
    :func:`~graflo.architecture.evolution.apply.apply_manifest_ops_inplace`.
    """
    ops: list[ManifestOp] = []

    property_renames = {
        source_class: {old: new for old, new in attr_map.items() if old != new}
        for source_class, attr_map in cm.properties.items()
    }
    property_renames = {k: v for k, v in property_renames.items() if v}
    if property_renames:
        ops.append(RenameVertexPropertiesOp(renames=property_renames))

    vertex_groups: dict[str, list[str]] = {}
    for source, target in cm.vertices.items():
        vertex_groups.setdefault(target, []).append(source)
    vertex_renames: dict[str, str] = {}
    for target, sources in sorted(vertex_groups.items()):
        merged_away = sorted(s for s in sources if s != target)
        if len(sources) > 1:
            if not cm.allow_merges:
                raise ValueError(
                    f"canonical_map_to_ops: collapsed group {sorted(sources)} "
                    f"-> {target!r} is a merge; set CanonicalMap(allow_merges=True)"
                )
            ops.append(
                MergeVerticesOp(
                    sources=merged_away,
                    into=target,
                    allow_self_relations=allow_self_relations,
                    allow_row_fusion=allow_row_fusion,
                )
            )
        elif merged_away:
            vertex_renames[merged_away[0]] = target
    if vertex_renames:
        ops.append(RenameVerticesOp(vertices=vertex_renames))

    relation_groups: dict[str, list[str]] = {}
    for source, target in cm.relations.items():
        relation_groups.setdefault(target, []).append(source)
    relation_renames: dict[str, str] = {}
    for target, sources in sorted(relation_groups.items()):
        merged_away = sorted(s for s in sources if s != target)
        if len(sources) > 1:
            if not cm.allow_merges:
                raise ValueError(
                    f"canonical_map_to_ops: collapsed relation group "
                    f"{sorted(sources)} -> {target!r} is a merge; set "
                    "CanonicalMap(allow_merges=True)"
                )
            ops.append(MergeEdgesOp(sources=merged_away, into=target))
        elif merged_away:
            relation_renames[merged_away[0]] = target
    if relation_renames:
        ops.append(RenameRelationsOp(relations=relation_renames))

    return ops


def _conflict(check: str, detail: str, hint: str) -> ComposeCanonicalConflictError:
    return ComposeCanonicalConflictError(
        f"compose contradicts the canonical map ({check}): {detail}. {hint}"
    )


def _relation_names(manifest: GraphManifest) -> set[str]:
    schema = manifest.graph_schema
    if schema is None:
        return set()
    return {
        edge.relation
        for edge in schema.core_schema.edge_config.edges
        if edge.relation is not None
    }


def _check_cluster_properties(
    cluster: Cluster, *, side: Side, author: CanonicalMap
) -> None:
    """Stale / re-target property checks for one cluster against one side's author map.

    Anchors on every member's own (pre-cluster) name and on ``into`` — the
    same two anchors the single-edge check used before clusters were n-ary,
    generalized over an arbitrary member list. A member whose name equals
    ``into`` (the common case for an identity-mapped side) anchors only once,
    exactly as when the two coincided in the original single-edge check.
    """
    anchors = {*cluster.members(side), cluster.into}
    for anchor in anchors:
        stale_attrs = author.stale_property_names(anchor)
        canonical_attrs = author.canonical_property_names(anchor)
        for pe in cluster.declaration.properties:
            spec = pe.left if side == "left" else pe.right
            if spec is None:
                old_name = None
            elif isinstance(spec, str):
                old_name = spec
            else:
                old_name = spec.get(anchor)
            for name in (n for n in (old_name, pe.into) if n is not None):
                if name in stale_attrs:
                    raise _conflict(
                        "stale property name",
                        f"property equivalence into {cluster.into!r} "
                        f"references {name!r} on {side}:{anchor}, a "
                        "pre-canonical attribute name",
                        "Author property equivalences in the canonical vocabulary.",
                    )
            if (
                old_name is not None
                and old_name in canonical_attrs
                and pe.into != old_name
            ):
                raise _conflict(
                    "property re-target",
                    f"the canonical map routes an attribute of {side}:{anchor} "
                    f"onto {old_name!r}, but the equivalence renames it to "
                    f"{pe.into!r}",
                    "Keep canonical attributes stable on the composed class: "
                    "align the member onto the canonical name, or fix the "
                    "canonical map.",
                )


def _check_property_maps_against_manifest(
    manifest: GraphManifest, cm: CanonicalMap, *, side: Side
) -> None:
    """Refuse a property rename whose old name is absent or whose new name collides.

    ``apply_rename_vertex_properties`` silently drops the losing field on such
    a collision (first-in-list wins) rather than raising — this pre-check
    turns that into a loud compose-time error instead of a quiet data loss.
    """
    schema = manifest.graph_schema
    if schema is None:
        return
    vertex_config = schema.core_schema.vertex_config
    for member, attr_map in cm.properties.items():
        if member not in vertex_config.vertex_set:
            continue  # reported by compose's own "not in {side} manifest" check
        existing = set(vertex_config.property_names(member))
        surviving = existing - set(attr_map)
        for old, new in attr_map.items():
            if old == new:
                continue
            if old not in existing:
                raise _conflict(
                    "unknown property",
                    f"property equivalence renames {side}:{member}.{old!r}, "
                    "which is not a declared property",
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


def validate_and_complete_canonical_map(
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> SideMaps:
    """Validate *op*'s declared clusters against *canonical_maps* and lower them.

    *canonical_maps* is a sequence of ``(side, map)`` pairs — ``side`` says
    which manifest the map's source names belong to. *left* / *right* are the
    manifests about to be composed (typically already canonicalized on
    whichever side supplied a map — the map is used here for checking, not
    for applying).

    Raises :class:`ComposeCanonicalConflictError` — wrapping
    :class:`~graflo.architecture.evolution.equivalence.ClusterConflictError`
    when the declared clusters themselves conflict (overlap, a shared
    ``into``, an occupied ``into``) — on: a stale pre-canonical class,
    relation or attribute name referenced by an equivalence; an equivalence
    that re-targets a class or relation name the canonical map already fixed;
    a property equivalence that re-targets an attribute the map already
    routed, renames an undeclared property, or renames onto one that already
    exists. On success, returns the :class:`SideMaps` **lowered** from the
    declared clusters — apply :func:`canonical_map_to_ops` to each side to
    canonicalize it before the schema/resource union. (The union of the
    author's map and the lowered one is check-only and is not itself
    returned: once the author's map has been applied, its source names no
    longer exist on that side.)

    Deliberately not re-checked here: everything compose already raises on —
    missing equivalence endpoints, name collisions under
    ``name_conflict="error"``, incompatible property types, divergent
    funnels. That includes **undeclared canonical near-collisions** between
    the two vocabularies (``ComposeNameConflictError``).
    """
    left_schema = left.graph_schema
    right_schema = right.graph_schema
    left_vertex_names = (
        set(left_schema.core_schema.vertex_config.vertex_set)
        if left_schema is not None
        else set()
    )
    right_vertex_names = (
        set(right_schema.core_schema.vertex_config.vertex_set)
        if right_schema is not None
        else set()
    )
    left_relation_names = _relation_names(left)
    right_relation_names = _relation_names(right)

    try:
        index = index_clusters(
            op,
            left_vertices=left_vertex_names,
            right_vertices=right_vertex_names,
            left_relations=left_relation_names,
            right_relations=right_relation_names,
        )
    except ClusterConflictError as exc:
        raise ComposeCanonicalConflictError(
            f"compose contradicts the canonical map (cluster conflict): {exc}"
        ) from exc

    author: dict[Side, CanonicalMap] = {"left": CanonicalMap(), "right": CanonicalMap()}
    for side, cm in canonical_maps:
        author[side] = merge_canonical_maps(author[side], cm)

    lowered = clusters_to_side_maps(index, allow_merges=op.allow_merges)

    vertex_intos = index.labels
    relation_intos = index.relation_labels()

    for side, manifest in (("left", left), ("right", right)):
        a = author[side]

        stale_classes = sorted(
            a.stale_class_names & (index.vertex_members(side) | vertex_intos)
        )
        if stale_classes:
            raise _conflict(
                "stale class name",
                f"equivalence references pre-canonical class name(s) "
                f"{stale_classes} on the {side} side",
                "Author the equivalence in the canonical vocabulary.",
            )
        stale_relations = sorted(
            a.stale_relation_names & (index.relation_members(side) | relation_intos)
        )
        if stale_relations:
            raise _conflict(
                "stale relation name",
                f"equivalence references pre-canonical relation name(s) "
                f"{stale_relations} on the {side} side",
                "Author the equivalence in the canonical vocabulary.",
            )

        for cluster in index.vertices:
            _check_cluster_properties(cluster, side=side, author=a)

        merge_canonical_maps(a, lowered[side])
        _check_property_maps_against_manifest(manifest, lowered[side], side=side)

    return lowered
