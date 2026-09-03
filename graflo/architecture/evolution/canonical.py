"""Canonical vocabulary maps: derive rename ops and validate compose against them.

A :class:`CanonicalMap` is the author-declared translation of class and property
names into a canonical target vocabulary. It is a *partial function*
``{C1, C2, ...} → C_canon`` — sources may come from either side of a compose.
Two moments of a union build consume it:

1. :func:`canonical_map_to_ops` turns the map into unary evolution ops that
   canonicalize one source manifest standalone (property renames first, then
   class merges/renames), ready for
   :func:`~graflo.architecture.evolution.apply.apply_evolution`.
2. :func:`validate_and_complete_canonical_map` cross-checks a
   :class:`~graflo.architecture.evolution.ops.ComposeManifestsOp` against maps
   from either side *before*
   :func:`~graflo.architecture.evolution.compose.compose_manifests` runs, and
   **completes** the map: a class that sits in an equivalence cluster with a
   canonically labelled peer inherits that label when it has none of its own.

Like compose itself, this module applies declared maps deterministically; it
never infers semantic matches beyond completing labels along declared
equivalence edges.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

from .equivalence import (
    ClusterConflictError,
    Node,
    Side,
    build_clusters,
    resolve_cluster_labels,
)
from .ops import (
    ComposeManifestsOp,
    ManifestOp,
    MergeVerticesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    VertexEquivalence,
    validate_rename_map_is_injective,
)

logger = logging.getLogger(__name__)


class ComposeCanonicalConflictError(ValueError):
    """A compose op contradicts the canonical map it was authored against."""


class CanonicalMap(ConfigBaseModel):
    """Declared translation of a source vocabulary into canonical names.

    ``vertices`` maps source class names to canonical class names; a class
    absent from the map keeps its name (identity mapping). ``properties`` maps,
    per *source* class name, source attribute names to canonical attribute
    names — including for classes whose name does not change.

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
    allow_merges: bool = PydanticField(
        default=False,
        description=(
            "Accept a non-injective ``vertices`` map. Two sources sharing a "
            "canonical target is a *merge*, not a rename; it must be a stated "
            "intent because merging fuses entities and can create "
            "self-relations. When set, :func:`canonical_map_to_ops` emits "
            "``MergeVerticesOp`` for the collapsed groups."
        ),
    )

    @model_validator(mode="after")
    def _validate_maps(self) -> CanonicalMap:
        if not self.allow_merges:
            validate_rename_map_is_injective(
                self.vertices,
                kind="canonical vertex",
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

    @property
    def stale_class_names(self) -> set[str]:
        """Source class names that no longer exist after canonicalization."""
        return {
            source
            for source, target in self.vertices.items()
            if source != target and source not in self.vertices.values()
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


def canonical_map_to_ops(cm: CanonicalMap) -> list[ManifestOp]:
    """Turn a canonical map into unary evolution ops.

    Order matters: property renames are keyed by the *source* class names, so
    they come first; class merges (when ``allow_merges``) and renames follow.
    Identity entries (``old == new``) are dropped — the returned list applies
    cleanly via :func:`~graflo.architecture.evolution.apply.apply_evolution`.
    """
    ops: list[ManifestOp] = []

    property_renames = {
        source_class: {old: new for old, new in attr_map.items() if old != new}
        for source_class, attr_map in cm.properties.items()
    }
    property_renames = {k: v for k, v in property_renames.items() if v}
    if property_renames:
        ops.append(RenameVertexPropertiesOp(renames=property_renames))

    groups: dict[str, list[str]] = {}
    for source, target in cm.vertices.items():
        groups.setdefault(target, []).append(source)

    renames: dict[str, str] = {}
    for target, sources in sorted(groups.items()):
        merged_away = sorted(s for s in sources if s != target)
        if len(sources) > 1:
            # Collapsed group: a merge, guarded by CanonicalMap.allow_merges.
            ops.append(MergeVerticesOp(sources=merged_away, into=target))
        elif merged_away:
            renames[merged_away[0]] = target
    if renames:
        ops.append(RenameVerticesOp(vertices=renames))

    return ops


def _conflict(check: str, detail: str, hint: str) -> ComposeCanonicalConflictError:
    return ComposeCanonicalConflictError(
        f"compose contradicts the canonical map ({check}): {detail}. {hint}"
    )


def _explicit_canonical_labels(
    canonical_maps: Sequence[tuple[Side, CanonicalMap]],
) -> dict[Node, str]:
    """``(side, name) → label`` for every *explicit* (non-identity) map entry.

    Identity entries (``source == target``) are omitted so they do not force a
    cluster label when a peer contributes a real rename.
    """
    labels: dict[Node, str] = {}
    for side, cm in canonical_maps:
        for source, target in cm.vertices.items():
            if source == target:
                continue
            node: Node = (side, source)
            existing = labels.get(node)
            if existing is not None and existing != target:
                raise _conflict(
                    "canonical label clash",
                    f"{side}:{source} maps to both {existing!r} and {target!r}",
                    "Supply at most one non-identity canonical label per "
                    "(side, class).",
                )
            labels[node] = target
        # Also record identity-mapped sources that appear only as property keys
        # so stale-property checks still find them — but not as cluster labels.
    return labels


def _merged_input_map(
    canonical_maps: Sequence[tuple[Side, CanonicalMap]],
) -> CanonicalMap:
    """Union of all supplied maps (vertices + properties) before completion."""
    vertices: dict[str, str] = {}
    properties: dict[str, dict[str, str]] = {}
    allow_merges = False
    for _side, cm in canonical_maps:
        allow_merges = allow_merges or cm.allow_merges
        for source, target in cm.vertices.items():
            existing = vertices.get(source)
            if existing is not None and existing != target:
                raise _conflict(
                    "canonical label clash",
                    f"{source!r} maps to both {existing!r} and {target!r} "
                    "across supplied maps",
                    "Reconcile the canonical maps before compose.",
                )
            vertices[source] = target
        for source_class, attr_map in cm.properties.items():
            bucket = properties.setdefault(source_class, {})
            for old, new in attr_map.items():
                existing = bucket.get(old)
                if existing is not None and existing != new:
                    raise _conflict(
                        "canonical property clash",
                        f"{source_class}.{old} maps to both {existing!r} and {new!r}",
                        "Reconcile the canonical maps before compose.",
                    )
                bucket[old] = new
    return CanonicalMap(
        vertices=vertices, properties=properties, allow_merges=allow_merges
    )


def validate_and_complete_canonical_map(
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
    allow_implicit_merge: bool = False,
) -> CanonicalMap:
    """Validate *op* against canonical maps and return a completed map.

    *canonical_maps* is a sequence of ``(side, map)`` pairs — ``side`` says
    which manifest the map's source names belong to. *left* / *right* are the
    manifests about to be composed (typically already canonicalized on the
    side that supplied a map).

    Raises :class:`ComposeCanonicalConflictError` (or wraps
    :class:`~graflo.architecture.evolution.equivalence.ClusterConflictError`)
    when cluster labels disagree, a stale pre-canonical name appears in the
    op, or an unacknowledged multi-source merge is declared. On success,
    returns a :class:`CanonicalMap` that includes labels *inferred* for
    unmapped cluster members (completion along equivalence edges).

    Deliberately not re-checked here: everything compose already raises on —
    missing equivalence endpoints, name collisions under
    ``name_conflict="error"``, incompatible property types, divergent funnels.
    That includes **undeclared canonical near-collisions** between the two
    vocabularies (``ComposeNameConflictError``).
    """
    maps = list(canonical_maps)
    base = _merged_input_map(maps)
    stale_classes = base.stale_class_names

    for veq in op.vertices:
        _check_stale_class(veq, stale_classes, base)
        _check_properties(veq, base)

    clusters = build_clusters(op.vertices)
    explicit_labels = _explicit_canonical_labels(maps)

    # A side that supplied a map already speaks the canonical vocabulary for
    # classes the map named as *targets* (and identity-mapped sources). Those
    # contribute ``(side, name) → name`` so an ``into`` that disagrees with an
    # already-canonical left class conflicts. Classes absent from the map are
    # deliberately free to join a cluster and be completed (e.g. Shop → Company).
    for side, cm in maps:
        targets = set(cm.vertices.values())
        identity_sources = {
            source for source, target in cm.vertices.items() if source == target
        }
        for name in targets | identity_sources:
            node: Node = (side, name)
            explicit_labels.setdefault(node, name)

    try:
        resolved = resolve_cluster_labels(clusters, explicit_labels)
    except ClusterConflictError as exc:
        raise ComposeCanonicalConflictError(
            f"compose contradicts the canonical map (cluster conflict): {exc}"
        ) from exc

    # Completions: unmapped members inherit the resolved cluster label.
    # Recompute unmapped against the *author-supplied* explicit entries only
    # (not identity seeds), so a right-side peer of a left-canonical class is
    # still completed.
    author_labels = _explicit_canonical_labels(maps)
    completed_vertices = dict(base.vertices)
    for item in resolved:
        for side, name in item.cluster.members:
            if (side, name) in author_labels:
                continue
            if name == item.label:
                continue
            existing = completed_vertices.get(name)
            if existing is not None and existing != item.label:
                raise _conflict(
                    "completion clash",
                    f"{side}:{name} would complete to {item.label!r} but the "
                    f"map already has {existing!r}",
                    "Reconcile the canonical map with the equivalence cluster.",
                )
            completed_vertices[name] = item.label

        for veq in item.cluster.edges:
            if veq.into != item.label:
                raise _conflict(
                    "into != cluster label",
                    f"equivalence maps {veq.left!r} ≡ {veq.right!r} into "
                    f"{veq.into!r}, but the cluster resolves to {item.label!r}",
                    "Set `into` to the resolved cluster label, or fix the "
                    "canonical map.",
                )

    if not allow_implicit_merge:
        # Aggregation is by declared ``into``, not by connected component:
        # two disjoint edges that both name the same ``into`` still collapse
        # those classes onto one composed type.
        into_by_left: dict[str, set[str]] = {}
        into_by_right: dict[str, set[str]] = {}
        for veq in op.vertices:
            into_by_left.setdefault(veq.into, set()).add(veq.left)
            into_by_right.setdefault(veq.into, set()).add(veq.right)
        for into, lefts in sorted(into_by_left.items()):
            if len(lefts) > 1:
                raise _conflict(
                    "implicit merge",
                    f"equivalences collapse left classes {sorted(lefts)} into {into!r}",
                    "Compose treats a shared `into` as a merge with the unary "
                    "guards bypassed; pass allow_implicit_merge=True to state "
                    "the intent.",
                )
        for into, rights in sorted(into_by_right.items()):
            if len(rights) > 1:
                raise _conflict(
                    "implicit merge",
                    f"equivalences collapse right classes {sorted(rights)} into "
                    f"{into!r}",
                    "Compose treats a shared `into` as a merge with the unary "
                    "guards bypassed; pass allow_implicit_merge=True to state "
                    "the intent.",
                )

    _warn_on_composed_self_relations(op, left=left, right=right)

    # Completing peers onto one label is a stated merge across sources — the
    # returned map must accept non-injective vertices.
    allow_merges = base.allow_merges
    by_target: dict[str, list[str]] = {}
    for source, target in completed_vertices.items():
        by_target.setdefault(target, []).append(source)
    if any(len(sources) > 1 for sources in by_target.values()):
        allow_merges = True
    if any(
        len(item.cluster.left_names) > 1 or len(item.cluster.right_names) > 1
        for item in resolved
    ):
        allow_merges = True

    return CanonicalMap(
        vertices=completed_vertices,
        properties=dict(base.properties),
        allow_merges=allow_merges,
    )


def validate_compose_against_canonical_map(
    cm: CanonicalMap,
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    allow_implicit_merge: bool = False,
) -> None:
    """Back-compat wrapper: left-side map only, discard the completed return.

    Prefer :func:`validate_and_complete_canonical_map` for new code.
    """
    validate_and_complete_canonical_map(
        op,
        left=left,
        right=right,
        canonical_maps=[("left", cm)],
        allow_implicit_merge=allow_implicit_merge,
    )


def _check_stale_class(
    veq: VertexEquivalence, stale_classes: set[str], cm: CanonicalMap
) -> None:
    for role, name in (("left", veq.left), ("into", veq.into), ("right", veq.right)):
        if name in stale_classes:
            raise _conflict(
                "stale class name",
                f"equivalence {role} {name!r} is a pre-canonical name "
                f"(canonicalized to {cm.canonical_class(name)!r})",
                "Author equivalences in the canonical vocabulary.",
            )


def _check_properties(veq: VertexEquivalence, cm: CanonicalMap) -> None:
    # Property stale/retarget checks are keyed by the composed class name
    # (``into`` / left after canonicalization).
    for anchor in {veq.left, veq.into}:
        stale_attrs = cm.stale_property_names(anchor)
        canonical_attrs = cm.canonical_property_names(anchor)
        for pe in veq.properties:
            for role, name in (("left", pe.left), ("into", pe.into)):
                if name is not None and name in stale_attrs:
                    raise _conflict(
                        "stale property name",
                        f"property equivalence {role} {name!r} on {anchor!r} is a "
                        "pre-canonical attribute name",
                        "Author property equivalences in the canonical vocabulary.",
                    )
            if (
                pe.left is not None
                and pe.left in canonical_attrs
                and pe.into != pe.left
            ):
                raise _conflict(
                    "property re-target",
                    f"the canonical map routes an attribute of {anchor!r} onto "
                    f"{pe.left!r}, but the equivalence renames it to {pe.into!r}",
                    "Keep canonical attributes stable on the composed class: "
                    "align the right attribute onto the canonical name, or fix "
                    "the canonical map.",
                )


def _warn_on_composed_self_relations(
    op: ComposeManifestsOp, *, left: GraphManifest, right: GraphManifest
) -> None:
    left_rename = {veq.left: veq.into for veq in op.vertices}
    right_rename = {veq.right: veq.into for veq in op.vertices}
    for manifest, rename, side in (
        (left, left_rename, "left"),
        (right, right_rename, "right"),
    ):
        schema = manifest.graph_schema
        if schema is None:
            continue
        for edge in schema.core_schema.edge_config.edges:
            source = rename.get(edge.source, edge.source)
            target = rename.get(edge.target, edge.target)
            if source == target and edge.source != edge.target:
                logger.warning(
                    "compose will turn %s edge %s -> %s (%s) into a "
                    "self-relation on %r; the unary self-relation guard is "
                    "bypassed during compose",
                    side,
                    edge.source,
                    edge.target,
                    edge.relation,
                    source,
                )
