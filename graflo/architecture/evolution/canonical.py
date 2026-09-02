"""Canonical vocabulary maps: derive rename ops and validate compose against them.

A :class:`CanonicalMap` is the author-declared translation of one manifest's
vocabulary (classes and properties) into a canonical target vocabulary. It is a
single source of truth serving two moments of a union build:

1. :func:`canonical_map_to_ops` turns the map into unary evolution ops that
   canonicalize the source manifest standalone (property renames first, then
   class merges/renames), ready for
   :func:`~graflo.architecture.evolution.apply.apply_evolution`.
2. :func:`validate_compose_against_canonical_map` cross-checks a
   :class:`~graflo.architecture.evolution.ops.ComposeManifestsOp` against the
   same map *before* :func:`~graflo.architecture.evolution.compose.compose_manifests`
   runs, failing loudly on conflicts compose itself cannot see — most notably a
   stale pre-canonical name that would otherwise compose silently into the
   wrong union.

Like compose itself, this module applies declared maps deterministically; it
never infers semantic matches.
"""

from __future__ import annotations

import logging

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

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


def validate_compose_against_canonical_map(
    cm: CanonicalMap,
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    allow_implicit_merge: bool = False,
) -> None:
    """Fail loudly when *op* contradicts the canonical map *left* was built with.

    *left* is the already-canonicalized manifest (the output of applying
    :func:`canonical_map_to_ops`); *right* is the other side of the compose.
    Raises :class:`ComposeCanonicalConflictError` on the first conflict; emits
    a warning for compose merges that will create self-relations (compose
    bypasses the unary self-relation guard).

    Deliberately not re-checked here: everything compose already raises on —
    missing equivalence endpoints, name collisions under
    ``name_conflict="error"``, incompatible property types, divergent funnels.
    That now includes **undeclared canonical near-collisions** between the two
    vocabularies, which ``compose_manifests`` raises on directly with
    ``ComposeNameConflictError``. The two are disjoint by construction: this
    function only ever compares an op against a map the author *declared*,
    while compose's check fires on the residue no equivalence covers.
    """
    stale_classes = cm.stale_class_names

    into_by_left: dict[str, set[str]] = {}
    into_by_right: dict[str, set[str]] = {}
    for veq in op.vertices:
        into_by_left.setdefault(veq.into, set()).add(veq.left)
        into_by_right.setdefault(veq.into, set()).add(veq.right)

        _check_stale_class(veq, stale_classes, cm)
        _check_into_is_left(veq)
        _check_properties(veq, cm)

    if not allow_implicit_merge:
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
                    f"equivalences collapse right classes {sorted(rights)} into {into!r}",
                    "Compose treats a shared `into` as a merge with the unary "
                    "guards bypassed; pass allow_implicit_merge=True to state "
                    "the intent.",
                )

    _warn_on_composed_self_relations(op, left=left, right=right)


def _check_stale_class(
    veq: VertexEquivalence, stale_classes: set[str], cm: CanonicalMap
) -> None:
    for role, name in (("left", veq.left), ("into", veq.into)):
        if name in stale_classes:
            raise _conflict(
                "stale class name",
                f"equivalence {role} {name!r} is a pre-canonical name "
                f"(canonicalized to {cm.canonical_class(name)!r})",
                "Author equivalences in the canonical vocabulary.",
            )


def _check_into_is_left(veq: VertexEquivalence) -> None:
    if veq.into != veq.left:
        raise _conflict(
            "into != left",
            f"equivalence maps {veq.left!r} ≡ {veq.right!r} into {veq.into!r}, "
            f"but the left side already speaks the canonical vocabulary",
            "The union is expressed in canonical names: set "
            "`into` to the (canonical) left class name, or fix the canonical "
            "map if the target class is wrong.",
        )


def _check_properties(veq: VertexEquivalence, cm: CanonicalMap) -> None:
    stale_attrs = cm.stale_property_names(veq.left)
    canonical_attrs = cm.canonical_property_names(veq.left)
    for pe in veq.properties:
        for role, name in (("left", pe.left), ("into", pe.into)):
            if name is not None and name in stale_attrs:
                raise _conflict(
                    "stale property name",
                    f"property equivalence {role} {name!r} on {veq.left!r} is a "
                    "pre-canonical attribute name",
                    "Author property equivalences in the canonical vocabulary.",
                )
        if pe.left is not None and pe.left in canonical_attrs and pe.into != pe.left:
            raise _conflict(
                "property re-target",
                f"the canonical map routes an attribute of {veq.left!r} onto "
                f"{pe.left!r}, but the equivalence renames it to {pe.into!r}",
                "Keep canonical attributes stable on the left side: align the "
                "right attribute onto the canonical name, or fix the "
                "canonical map.",
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
