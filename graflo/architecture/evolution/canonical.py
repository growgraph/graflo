"""Canonical vocabulary maps, and the shared pieces of merge resolution.

A :class:`~graflo.architecture.evolution.ops.CanonicalMap` translates one side's
vocabulary into canonical names — a partial function on names, identity where
unmapped, and *idempotent*: a canonical name is a fixed point nothing maps away
from. On its own it lowers to one
:class:`~graflo.architecture.evolution.ops.CanonicalizeOp`
(:func:`canonical_map_to_ops`); :func:`dangling_entries` and
:func:`trim_canonical_map` check it against one manifest before any merge.

In a merge it is the **vocabulary** layer: a class's default name in the union.
:mod:`~graflo.architecture.evolution.naming_graph` resolves it together with
the op's equivalences and ``renames``, in one pass over the sides' own names;
:func:`resolve_clusters` is that resolution, raising every problem at once.

Vocabulary
----------

- **declared map** — a ``CanonicalMap`` the author wrote:
  ``op.canonical_maps[scope]`` and the ``canonical_maps=`` pairs handed to
  merge. Scoped ``left`` / ``right`` (that side's own names) or ``both``
  (either side's names). Folded per side by :func:`compose_canonical_maps`
  into a :class:`DeclaredMaps`.
- **composite map** (:class:`SideMaps`, one ``CanonicalizeOp`` per side) —
  what merge applies to that side before the union by name: every group
  member onto its merged name, every other class onto its rename or
  vocabulary name, simultaneously.
- **satisfied entry** — a declared entry whose source is absent from a side and
  whose target is present: taken as already applied by the caller. A heuristic
  — it cannot tell that from a target that never had that source — so it is
  logged.
- **dangling entry** — a declared entry that matches nothing on any side it
  could apply to. A typo, refused, each with a near-miss candidate where
  another spelling denotes the same concept. ``allow_dangling_entries`` drops
  them instead, for a shared vocabulary deliberately broader than the
  manifest it is applied to.
- **completion** (:class:`Completion`) — a declaration that would settle a
  refusal, carried as a paste-ready payload.

Identity is nominal: a class is the same class across two manifests only by
name (or by declared equivalence) — nothing structural fingerprints it, and
merge never infers a match.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.refusal import Refusal
from graflo.architecture.schema.naming import canonical_slug

from .equivalence import (
    Cluster,
    ClusterIndex,
    Kind,
    Side,
    did_you_mean,
    subject,
)
from .ops import (
    CanonicalizeOp,
    CanonicalMap,
    ManifestOp,
    MergeManifestsOp,
    VertexEquivalence,
)

__all__ = [
    "CanonicalMap",
    "ClusterResolution",
    "Completion",
    "DanglingEntry",
    "DeclaredMaps",
    "MergeCanonicalConflictError",
    "MergeIncompleteError",
    "Scope",
    "SideMaps",
    "SideNames",
    "canonical_map_to_ops",
    "canonical_near_collisions",
    "canonicalize_ops",
    "check_attribute_fixed_points",
    "check_property_fields_exist",
    "check_property_maps_against_manifest",
    "compose_canonical_maps",
    "dangling_entries",
    "dangling_refusal",
    "fold_declared_maps",
    "resolve_clusters",
    "trim_canonical_map",
    "validate_and_complete_canonical_map",
]

logger = logging.getLogger(__name__)

#: Where a declared map applies: one side's own names, or either side's.
Scope = Literal["left", "right", "both"]

_SIDES: tuple[Side, ...] = ("left", "right")


def _other(side: Side) -> Side:
    return "right" if side == "left" else "left"


class MergeCanonicalConflictError(Refusal):
    """A merge op's clusters and its declared maps contradict each other.

    A contradiction (one name, two targets; a fixed point moved), an ambiguity
    (a canonical name denoting two members), or a dangling entry. The
    subclass :class:`MergeIncompleteError` is the one refusal an extension
    resolves.

    ``check`` names the rule that refused — the phrase the message starts
    with — and ``subjects`` the names it is about, as
    :func:`~graflo.architecture.evolution.equivalence.subject` ids; see
    :class:`.Refusal`.
    """


#: What a repair does. ``extend_cluster`` replaces one equivalence by the
#: payload (the same declaration with one more member) and fuses entities;
#: ``declare_equivalences`` adds the payloads (or ``name_conflict="union_right"``
#: declares them itself); ``set_into`` replaces one equivalence by the payload,
#: renamed; ``rename_away`` adds the ``renames`` entry; ``add_key_source``
#: adds the identity entry carried in the payload; ``acknowledge`` replaces one
#: equivalence by the payload with a consequence added to its ``allow``.
#: ``add_key_source`` and ``acknowledge`` are left to the author, because each
#: decides which records fuse.
CompletionKind = Literal[
    "extend_cluster",
    "declare_equivalences",
    "set_into",
    "rename_away",
    "add_key_source",
    "acknowledge",
]


@dataclass(frozen=True)
class Completion:
    """A declaration that would settle a merge refusal, ready to paste.

    Payloads are ``VertexEquivalence`` / ``RelationEquivalence`` documents and
    a ``renames`` fragment (``{side: {vertices: {old: new}}}``), so a CLI can
    print them and an author can paste them. ``label`` says in a few words
    what applying it does.
    """

    kind: CompletionKind
    side: Side | None = None
    vertex_equivalences: tuple[dict[str, Any], ...] = ()
    relation_equivalences: tuple[dict[str, Any], ...] = ()
    renames: dict[str, Any] = field(default_factory=dict)
    label: str = ""
    #: For ``set_into`` / ``extend_cluster``: the position of the equivalence
    #: the payload replaces, in the op's list of its kind.
    replaces: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """The completion as a plain document."""
        out: dict[str, Any] = {"kind": self.kind}
        if self.label:
            out["label"] = self.label
        if self.side is not None:
            out["side"] = self.side
        if self.replaces is not None:
            out["replaces"] = self.replaces
        if self.vertex_equivalences:
            out["vertex_equivalences"] = [dict(p) for p in self.vertex_equivalences]
        if self.relation_equivalences:
            out["relation_equivalences"] = [dict(p) for p in self.relation_equivalences]
        if self.renames:
            out["renames"] = dict(self.renames)
        return out


class MergeIncompleteError(MergeCanonicalConflictError):
    """The declarations are consistent but do not cover a name; an extension would.

    Distinct from a contradiction: nothing has to be retracted, something has
    to be added, and :attr:`completion` says what.
    """

    def __init__(
        self,
        message: str,
        completion: Completion,
        *,
        check: str = "",
        subjects: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message, check=check, subjects=subjects)
        self.completion = completion


@dataclass(frozen=True)
class SideMaps:
    """The composite relabel per side: the one op merge applies to each."""

    left: CanonicalizeOp
    right: CanonicalizeOp

    def __getitem__(self, side: Side) -> CanonicalizeOp:
        return self.left if side == "left" else self.right


@dataclass(frozen=True)
class DeclaredMaps:
    """The declared vocabulary as each side sees it.

    ``left`` / ``right`` are the ``both``-scoped map folded under that side's
    own map; ``both`` is kept apart because its entries may legitimately apply
    to one side only.
    """

    left: CanonicalMap
    right: CanonicalMap
    both: CanonicalMap = field(default_factory=CanonicalMap)

    def __getitem__(self, side: Side) -> CanonicalMap:
        return self.left if side == "left" else self.right


@dataclass(frozen=True)
class DanglingEntry:
    """A canonical-map entry whose source matches nothing on its side.

    The source names no class or relation the side declares, and the entry is
    none of the four ways an absent source is still meaningful: a cluster
    member, an already-applied rename, a merged name as the author spelled
    it, or a ``both``-scoped entry that applies to the other side.

    Carried as data rather than refused one at a time, so that authoring a map
    against a schema of hundreds of classes is not one refusal per mistake.
    """

    side: Side
    kind: Kind | Literal["property"]
    source: str
    target: str | None = None
    suggestion: str = ""

    def describe(self) -> str:
        """The entry as it reads in a refusal, without naming the side.

        The near-miss :attr:`suggestion` is left to the caller to place: it is
        a trailing clause, and only a listing has somewhere to put one.
        """
        if self.kind == "property":
            return f"attribute map for {self.source!r}"
        return f"{self.kind} {self.source!r} -> {self.target!r}"


#: How many dangling entries a refusal lists before summarising the rest. A map
#: authored against the wrong manifest dangles in its entirety, and the first
#: handful of entries already says so.
_DANGLING_LISTED = 10


def _conflict(
    check: str, detail: str, hint: str, *, subjects: tuple[str, ...] = ()
) -> MergeCanonicalConflictError:
    return MergeCanonicalConflictError(
        f"{check}: {detail}. {hint}",
        check=check,
        subjects=subjects,
    )


# ── declared maps ───────────────────────────────────────────────────────────


def _moving(mapping: Mapping[str, str]) -> set[str]:
    return {source for source, target in mapping.items() if source != target}


def _targets(mapping: Mapping[str, str]) -> set[str]:
    return {target for source, target in mapping.items() if source != target}


def _compose_name_maps(
    base: Mapping[str, str], extension: Mapping[str, str], *, noun: str
) -> dict[str, str]:
    """One kind's half of :func:`compose_canonical_maps` — same verb, same operation."""
    out = dict(base)
    for source, target in extension.items():
        existing = out.get(source)
        if existing is not None and existing != target:
            raise _conflict(
                f"canonical {noun} clash",
                f"{source!r} maps to both {existing!r} and {target!r}",
                "Reconcile the canonical maps.",
            )
        out[source] = target
    chained = sorted(
        (_moving(base) & _targets(extension)) | (_moving(extension) & _targets(base))
    )
    if chained:
        raise _conflict(
            f"canonical {noun} re-target",
            f"{chained} are canonical targets of one map and moving sources of "
            "the other",
            "Canonical targets are fixed points: a name one map establishes "
            "may not be mapped away by the other.",
        )
    return out


def compose_canonical_maps(base: CanonicalMap, extension: CanonicalMap) -> CanonicalMap:
    """Partial-function union of two declared maps; a target of either is a fixed point.

    Every source named by *base* or *extension* maps to exactly one target; a
    source the two disagree on raises :class:`MergeCanonicalConflictError`.
    A target of either map is a fixed point the other may not move, checked
    in both directions so the result does not depend on which map is *base*.
    ``properties`` union the same way per source class.
    """
    vertices = _compose_name_maps(base.vertices, extension.vertices, noun="vertex")
    relations = _compose_name_maps(base.relations, extension.relations, noun="relation")
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
        allow_dangling_entries=(
            base.allow_dangling_entries or extension.allow_dangling_entries
        ),
        allow_self_relations=base.allow_self_relations
        or extension.allow_self_relations,
        allow_observation_fusion=base.allow_observation_fusion
        or extension.allow_observation_fusion,
    )


def _effective(
    vertices: Mapping[str, str],
    relations: Mapping[str, str],
    properties: Mapping[str, Mapping[str, str]],
) -> bool:
    return (
        any(source != target for source, target in vertices.items())
        or any(source != target for source, target in relations.items())
        or any(
            old != new
            for attr_map in properties.values()
            for old, new in attr_map.items()
        )
    )


def canonicalize_ops(op: CanonicalizeOp) -> list[ManifestOp]:
    """*op* as the op list to apply: empty when it has no effective entry."""
    if not _effective(op.vertices, op.relations, op.properties):
        return []
    return [op]


def canonical_map_to_ops(
    cm: CanonicalMap,
    *,
    allow_self_relations: bool = False,
    allow_observation_fusion: bool = False,
) -> list[ManifestOp]:
    """Lower a declared map to its single op.

    A canonical map is a function on names, and
    :class:`~graflo.architecture.evolution.ops.CanonicalizeOp` applies exactly
    that function in one step: attribute renames (keyed by the source class)
    first, then classes and relations simultaneously, so no op order can leak
    into the result. A map with no effective entry lowers to no op at all. A
    group of more than one class or relation is a merge and is refused unless
    ``allow_merges`` is set.
    """
    return canonicalize_ops(
        CanonicalizeOp(
            vertices=dict(cm.vertices),
            properties={cls: dict(attrs) for cls, attrs in cm.properties.items()},
            relations=dict(cm.relations),
            allow_merges=cm.allow_merges,
            allow_self_relations=allow_self_relations or cm.allow_self_relations,
            allow_observation_fusion=allow_observation_fusion
            or cm.allow_observation_fusion,
        )
    )


def canonical_near_collisions(
    left_names: Iterable[str],
    right_names: Iterable[str],
    *,
    exempt: Collection[str],
) -> list[tuple[str, str]]:
    """``(left, right)`` pairs that key alike under ``canonical_slug`` but differ.

    Exact matches are excluded: those are the same-name path, so the two
    checks can never report one pair twice.
    """
    by_key: dict[str, list[str]] = {}
    for name in left_names:
        by_key.setdefault(canonical_slug(name), []).append(name)
    pairs: list[tuple[str, str]] = []
    for right_name in right_names:
        if right_name in exempt:
            continue
        for left_name in by_key.get(canonical_slug(right_name), []):
            if left_name != right_name:
                pairs.append((left_name, right_name))
    return sorted(set(pairs))


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

    def of_kind(self, kind: Kind) -> frozenset[str]:
        return self.vertices if kind == "vertex" else self.relations


def _declares(side: Side, names: SideNames) -> str:
    """What the side *does* declare — the context a dangling entry is missing.

    A manifest with no ``schema`` block declares no names at all, which makes
    every entry scoped to it dangle at once. That reads exactly like a page of
    typos unless the refusal says so.
    """
    if not names.vertices and not names.relations:
        return f"the {side} manifest declares no schema block, so no entry can match it"
    return (
        f"{side} declares {_count(len(names.vertices), 'vertex', 'vertices')} "
        f"and {_count(len(names.relations), 'relation', 'relations')}"
    )


def _count(n: int, singular: str, plural: str) -> str:
    return f"{n} {singular if n == 1 else plural}"


def dangling_refusal(
    entries: Sequence[DanglingEntry], *, names: SideNames | None = None
) -> MergeCanonicalConflictError:
    """One refusal naming every dangling entry on a side.

    A canonical map is authored as a whole, so it is corrected as a whole: the
    refusal lists each entry with a near-miss candidate where one exists,
    rather than surfacing the next mistake only after the previous one is
    fixed and the merge re-run.

    *names* is what the side declares, used for context. A caller classifying
    one entry in isolation has no side to describe and omits it.
    """
    side = entries[0].side
    subjects = tuple(subject(e.side, e.source) for e in entries)
    hint = (
        "Check the spelling, drop the entries, or set allow_dangling_entries "
        "if the map is deliberately broader than this manifest."
    )
    if len(entries) == 1:
        entry = entries[0]
        # The degenerate case is worth naming even for a single entry; the
        # count of what the side has is noise when the side has anything.
        context = (
            f" ({_declares(side, names)})"
            if names is not None and not names.vertices and not names.relations
            else ""
        )
        if entry.kind == "property":
            detail = (
                f"the canonical map's {side} attribute map for {entry.source!r} "
                f"matches no class on that side{context}{entry.suggestion}"
            )
        else:
            detail = (
                f"the canonical map's {side} entry {entry.source!r} -> "
                f"{entry.target!r} matches no {entry.kind} on that "
                f"side{context}{entry.suggestion}"
            )
        return _conflict("dangling entry", detail, hint, subjects=subjects)
    listed = "\n".join(
        f"  {entry.describe()}{entry.suggestion}"
        for entry in entries[:_DANGLING_LISTED]
    )
    if len(entries) > _DANGLING_LISTED:
        listed += f"\n  ... and {len(entries) - _DANGLING_LISTED} more"
    context = "" if names is None else f" ({_declares(side, names)})"
    # Built directly rather than through ``_conflict``: the hint belongs with
    # the summary, above the list, not trailing off the last entry.
    return MergeCanonicalConflictError(
        f"dangling entry: {len(entries)} {side} canonical map entries match nothing on that "
        f"side{context}. {hint}\n{listed}",
        check="dangling entry",
        subjects=subjects,
    )


@dataclass(frozen=True)
class ClusterResolution:
    """What merge applies: the resolved groups and one composite relabel per side.

    ``declared`` is the folded vocabulary as seen from each side;
    ``side_maps`` is the composite — every group member onto its merged
    name, every other class onto its rename or vocabulary name, every demoted
    key field onto ``<space>__<field>`` (its ``local_key`` tag, else the
    side's origin) — one
    :class:`~graflo.architecture.evolution.ops.CanonicalizeOp` per side.
    ``index`` includes any group merge synthesized; each cluster's members
    are the closed group. ``findings`` are the notes the resolution made
    (defaults it applied), and ``graph`` the naming graph it was read from.
    """

    index: ClusterIndex
    side_maps: SideMaps
    declared: DeclaredMaps
    findings: tuple[Any, ...] = ()
    graph: Any = None
    #: Each side's origin name, and the sides whose origin the union names
    #: something by: a demoted key, or a ``local_key`` tag left to default.
    origins: Mapping[Side, str] = field(
        default_factory=lambda: {"left": "left", "right": "right"}
    )
    origin_sides: frozenset[Side] = frozenset()
    #: Each re-keyed member whose own key is demoted, ``(side, member)``, to
    #: that key's canonical fields before the origin prefix.
    demoted_keys: Mapping[tuple[Side, str], tuple[str, ...]] = field(
        default_factory=dict
    )
    #: Each of those members to the key space its key is named by: its
    #: ``local_key`` tag, else its side's origin.
    key_spaces: Mapping[tuple[Side, str], str] = field(default_factory=dict)
    #: Each of those members the op tags, to every key space its tags name.
    key_tags: Mapping[tuple[Side, str], tuple[str, ...]] = field(default_factory=dict)


def fold_declared_maps(
    op: MergeManifestsOp, extra: Sequence[tuple[Side, CanonicalMap]]
) -> DeclaredMaps:
    scoped: dict[str, CanonicalMap] = {
        "left": CanonicalMap(),
        "right": CanonicalMap(),
        "both": CanonicalMap(),
    }
    for scope, cm in op.canonical_maps.items():
        scoped[scope] = compose_canonical_maps(scoped[scope], cm)
    for side, cm in extra:
        scoped[side] = compose_canonical_maps(scoped[side], cm)
    return DeclaredMaps(
        left=compose_canonical_maps(scoped["both"], scoped["left"]),
        right=compose_canonical_maps(scoped["both"], scoped["right"]),
        both=scoped["both"],
    )


def check_property_fields_exist(
    manifest: GraphManifest, cluster: Cluster, *, side: Side, declared: CanonicalMap
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
        for member, field_name in per_member.items():
            if member not in vertex_config.vertex_set:
                continue  # the existence check reports the member
            if field_name in vertex_config.property_names(member):
                continue
            sources = sorted(
                old
                for old, new in declared.properties.get(member, {}).items()
                if new == field_name and old != new
            )
            hint = (
                f"{field_name!r} is the canonical name of {member}.{sources[0]!r} — "
                "name the source field"
                if sources
                else "Check the spelling, or declare the property first with "
                "AddVertexPropertiesOp"
            )
            raise _conflict(
                "unknown property",
                f"property equivalence into {pe.into!r} names "
                f"{side}:{member}.{field_name!r}, which is not a declared property",
                hint + ".",
                subjects=(subject(side, member, field_name),),
            )


def check_attribute_fixed_points(
    cluster: Cluster, *, side: Side, declared: CanonicalMap
) -> None:
    """A canonical attribute the map established on a member may not be renamed by the cluster.

    The fixed-point set is keyed by *canonical class*, not by member:
    ``canonical_property_names`` folds every source class the map sends to one
    canonical name, which is the whole point -- one member's rename establishes
    the attribute for every sibling that lands on the same class. Asking it
    with a raw member name returns nothing for exactly the members the map
    renames, which is to say for exactly the cases this check is here for.
    """
    for member, attr_map in cluster.property_maps(side).items():
        canonical = declared.canonical_property_names(declared.canonical_class(member))
        for old, new in attr_map.items():
            if old in canonical and new != old:
                raise _conflict(
                    "property re-target",
                    f"the canonical map established {old!r} on {side}:{member}, "
                    f"but the equivalence renames it to {new!r}",
                    "Canonical attributes are fixed points: align the other "
                    "members onto the canonical name, or fix the canonical map.",
                    subjects=(subject(side, member, old),),
                )


def check_property_maps_against_manifest(
    manifest: GraphManifest, relabel: CanonicalizeOp, *, side: Side
) -> None:
    """Refuse a property rename whose old name is absent or whose new name collides.

    A collision would otherwise drop the losing field at apply time; this
    turns it into a loud merge-time error instead of a quiet data loss.
    """
    schema = manifest.graph_schema
    if schema is None:
        return
    vertex_config = schema.core_schema.vertex_config
    for member, attr_map in relabel.properties.items():
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
                    subjects=(subject(side, member, old),),
                )
            if new in surviving:
                raise _conflict(
                    "property rename collision",
                    f"{side}:{member}.{old!r} -> {new!r} collides with an "
                    "existing property of that name",
                    "A property rename cannot merge fields — align them "
                    "explicitly via PropertyEquivalence on both sides instead.",
                    subjects=(
                        subject(side, member, old),
                        subject(side, member, new),
                    ),
                )


def resolve_clusters(
    op: MergeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
    origins: Mapping[Side, str] | None = None,
) -> ClusterResolution:
    """Resolve *op*'s groups and build the per-side composite relabel.

    *canonical_maps* are extra ``(side, map)`` vocabulary pairs folded into
    ``op.canonical_maps``. *left* / *right* are the manifests about to be
    merged, in their own names: the vocabulary, the equivalences and
    ``op.renames`` are resolved together over those names, in one pass (see
    :mod:`~graflo.architecture.evolution.naming_graph`). *origins* default to
    :func:`~graflo.architecture.evolution.naming_graph.merge_origins`.

    Raises:
        MergeNamingError: Every blocking problem with the declarations, at once
            -- a :class:`MergeCanonicalConflictError`, and a
            :class:`MergeIncompleteError` too when every problem is one an
            addition settles (a shared name under ``name_conflict="error"``,
            a vocabulary sending a class onto a group's name).
    """
    from .naming_graph import resolve_naming

    return resolve_naming(
        op, left=left, right=right, canonical_maps=canonical_maps, origins=origins
    )


def validate_and_complete_canonical_map(
    op: MergeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> SideMaps:
    """Validate *op* against its vocabulary and return the completed per-side relabels.

    Every group's merged name is completed — from ``into``, the vocabulary,
    or the members' shared spelling — and every member maps onto it; the
    remaining vocabulary entries and ``renames`` are carried as written.
    Apply the result to each side with :func:`canonicalize_ops` before the
    schema/resource union, which is what
    :func:`~graflo.architecture.evolution.merge.merge_manifests` does.

    Raises the :class:`MergeNamingError` of :func:`resolve_clusters`.
    """
    return resolve_clusters(
        op, left=left, right=right, canonical_maps=canonical_maps
    ).side_maps


def dangling_entries(
    cm: CanonicalMap, manifest: GraphManifest, *, side: Side = "left"
) -> tuple[DanglingEntry, ...]:
    """The map's entries that match nothing in *manifest*, with near-miss candidates.

    A canonical map is authored against one manifest long before it is merged
    against another, and this is that check on its own: no equivalences, no
    other side, no merge. With no clusters declared there are no merged
    names and no ``both`` scope, so the classification merge uses collapses
    to its two surviving cases — a source the manifest declares is applicable,
    a source it does not but whose target it does is already applied — and
    everything else dangles.

    Args:
        cm: The declared map.
        manifest: The manifest the map is meant to apply to.
        side: Which side the map is scoped to; names the entries in the result.

    Returns:
        One :class:`DanglingEntry` per unmatched entry, vertices first, then
        relations, then attribute maps. Empty means the map applies as written.
    """
    names = SideNames.of(manifest)
    out: list[DanglingEntry] = []
    for kind, mapping, known in (
        ("vertex", cm.vertices, names.vertices),
        ("relation", cm.relations, names.relations),
    ):
        for source, target in mapping.items():
            if source in known or target in known:
                continue
            out.append(
                DanglingEntry(
                    side=side,
                    kind=kind,  # type: ignore[arg-type]
                    source=source,
                    target=target,
                    suggestion=did_you_mean(source, known),
                )
            )
    for cls in cm.properties:
        if cls in names.vertices or cm.canonical_class(cls) in names.vertices:
            continue
        out.append(
            DanglingEntry(
                side=side,
                kind="property",
                source=cls,
                suggestion=did_you_mean(cls, names.vertices),
            )
        )
    return tuple(out)


def trim_canonical_map(
    cm: CanonicalMap, manifest: GraphManifest, *, side: Side = "left"
) -> tuple[CanonicalMap, tuple[DanglingEntry, ...]]:
    """*cm* with its entries for *manifest* only, and the entries dropped.

    Trimming is an authoring step, not something merge does on its own: the
    result is a value to inspect and save, so that a map narrowed to a manifest
    is a change with a diff rather than a silent omission at merge time. A
    map that is deliberately broader than any one manifest is better served by
    ``allow_dangling_entries``, which keeps the map whole.

    Returns:
        The trimmed map and the entries removed, as
        :func:`dangling_entries` reports them.
    """
    dropped = dangling_entries(cm, manifest, side=side)
    if not dropped:
        return cm, ()
    gone = {(entry.kind, entry.source) for entry in dropped}
    return (
        cm.model_copy(
            update={
                "vertices": {
                    s: t for s, t in cm.vertices.items() if ("vertex", s) not in gone
                },
                "relations": {
                    s: t for s, t in cm.relations.items() if ("relation", s) not in gone
                },
                "properties": {
                    c: dict(a)
                    for c, a in cm.properties.items()
                    if ("property", c) not in gone
                },
            }
        ),
        dropped,
    )
