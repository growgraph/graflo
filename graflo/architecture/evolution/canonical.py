"""Canonical vocabulary maps and compose-time cluster resolution.

Two declarations say how two manifests' names relate. A
:class:`~graflo.architecture.evolution.ops.CanonicalMap` translates one side's
vocabulary into canonical names — a partial function on names, identity where
unmapped, and *idempotent*: a canonical name is a fixed point nothing maps away
from. An equivalence cluster on a
:class:`~graflo.architecture.evolution.ops.ComposeManifestsOp` says *which*
classes across the two sides are one, and may leave what they are called to
the map. Renames compose, so "canonicalize, then declare equivalences in
canonical names" and "declare equivalences in raw names, then canonicalize"
are the same function; :func:`resolve_clusters` computes it directly — one
composite relabel per side, applied as a single
:class:`~graflo.architecture.evolution.ops.CanonicalizeOp` — so there is no
intermediate vocabulary an author has to write in.

Vocabulary
----------

- **declared map** — a ``CanonicalMap`` the author wrote:
  ``op.canonical_maps[scope]`` and the ``canonical_maps=`` pairs handed to
  compose. Scoped ``left`` / ``right`` (that side's own names) or ``both``
  (either side's names, and composed names). Folded per side by
  :func:`merge_canonical_maps` into a :class:`DeclaredMaps`.
- **cluster** (:class:`~graflo.architecture.evolution.equivalence.Cluster`,
  resolved from a :class:`~graflo.architecture.evolution.equivalence.ClusterSpec`)
  — one equivalence declaration, resolved: its members per side, in the
  manifests' own spelling, and its **composed name** — ``into`` translated
  through the declared maps, else the canonical name a map gives a member,
  else the one spelling every member shares.
- **cluster map** — per side, every member onto its composed name, the composed
  name itself included as a self entry so the op merges into it rather than
  refusing an occupied target.
- **composite map** (:class:`SideMaps`, one ``CanonicalizeOp`` per side) — the
  cluster map plus every declared entry that applies to a non-member: what
  compose applies to that side before the union by name. A relabel, not a
  vocabulary — two clusters may legitimately chain (one composed name renamed
  away by another declaration), which a ``CanonicalMap`` refuses.
- **fixed point** — a canonical target. No declared map and no cluster may
  move it.
- **opinion** — what the declared maps say a member's canonical name is: the
  target it maps to, or itself when it is a fixed point.
- **satisfied entry** — a declared entry whose source is absent from a side and
  whose target is present: taken as already applied by the caller. A heuristic
  — it cannot tell that from a target that never had that source — so it is
  logged.
- **dangling entry** — a declared entry that matches nothing on any side it
  could apply to. A typo, refused: one refusal names every one of them on a
  side, each with a near-miss candidate where another spelling denotes the
  same concept. ``allow_dangling_entries`` drops them instead, for a shared
  vocabulary deliberately broader than the manifest it is applied to.
- **synthesized cluster** — a cluster compose declares itself under
  ``name_conflict="union_right"`` for a name both sides carry after their
  composite maps, or two spellings of one name, so that a union by name goes
  through the same identity and property reconciliation as a declared one.
- **completion** (:class:`Completion`) — the extension that would make an
  incomplete declaration consistent, carried by
  :class:`ComposeIncompleteError` as declaration payloads.

One rule
--------

**The declared maps and the equivalences must agree on where every name goes,
and a canonical target is a fixed point neither may re-map.** Every refusal is
an instance of it, in one of four classes:

| class | error | a trigger |
|---|---|---|
| contradiction | ``ComposeCanonicalConflictError`` | the map says ``Firm → Company``, the cluster names the composed class ``Party`` |
| ambiguity | ``ComposeCanonicalConflictError`` | one canonical name denotes two members of one cluster |
| incomplete | ``ComposeIncompleteError`` (carries a ``Completion``) | a map entry sends a non-member onto a composed name |
| dangling | ``ComposeCanonicalConflictError`` | an entry matching no name on any side it could apply to |

Plus the cluster-shape checks of
:mod:`~graflo.architecture.evolution.equivalence`, which run before any
rename. Every case a declared entry and a cluster can stand in — agreement,
naming, translation and each refusal — is tabulated in
``docs/concepts/schema/manifest_evolution.md`` under "Canonical maps".

Identity is nominal: a class is the same class across two manifests only by
name (or by declared equivalence) — nothing structural fingerprints it, and
compose never infers a match.
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
    ClusterConflictError,
    ClusterIndex,
    ClusterSpec,
    Kind,
    Side,
    check_member_existence,
    did_you_mean,
    index_clusters,
    subject,
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
    "Completion",
    "ComposeCanonicalConflictError",
    "ComposeIncompleteError",
    "DanglingEntry",
    "DeclaredMaps",
    "Scope",
    "SideMaps",
    "SideNames",
    "canonical_map_to_ops",
    "canonical_near_collisions",
    "canonicalize_ops",
    "clusters_to_side_maps",
    "dangling_entries",
    "fold_declared_maps",
    "merge_canonical_maps",
    "resolve_clusters",
    "same_name_groups",
    "trim_canonical_map",
    "validate_and_complete_canonical_map",
]

logger = logging.getLogger(__name__)

#: Where a declared map applies: one side's own names, or either side's.
Scope = Literal["left", "right", "both"]

_SIDES: tuple[Side, ...] = ("left", "right")


def _other(side: Side) -> Side:
    return "right" if side == "left" else "left"


class ComposeCanonicalConflictError(Refusal):
    """A compose op's clusters and its declared maps contradict each other.

    A contradiction (one name, two targets; a fixed point moved), an ambiguity
    (a canonical name denoting two members), or a dangling entry. The
    subclass :class:`ComposeIncompleteError` is the one refusal an extension
    resolves.

    ``check`` names the rule that refused — the parenthesised phrase in the
    message — and ``subjects`` the names it is about, as
    :func:`~graflo.architecture.evolution.equivalence.subject` ids; see
    :class:`.Refusal`.
    """


@dataclass(frozen=True)
class Completion:
    """The extension that would make an incomplete compose declaration consistent.

    ``kind`` says what to do: ``extend_cluster`` — replace one declared cluster
    by the payload carried here (the same declaration with one more member);
    ``declare_equivalences`` — add the carried declarations to the op (or set
    ``name_conflict="union_right"``, which declares exactly these itself).
    Payloads are ``VertexEquivalence`` / ``RelationEquivalence`` documents, so
    a CLI can print them and an author can paste them.
    """

    kind: Literal["extend_cluster", "declare_equivalences"]
    side: Side | None = None
    vertex_equivalences: tuple[dict[str, Any], ...] = ()
    relation_equivalences: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """The completion as a plain document."""
        out: dict[str, Any] = {"kind": self.kind}
        if self.side is not None:
            out["side"] = self.side
        if self.vertex_equivalences:
            out["vertex_equivalences"] = [dict(p) for p in self.vertex_equivalences]
        if self.relation_equivalences:
            out["relation_equivalences"] = [dict(p) for p in self.relation_equivalences]
        return out


class ComposeIncompleteError(ComposeCanonicalConflictError):
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
    """The composite relabel per side: the one op compose applies to each."""

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
    member, an already-applied rename, a composed name as the author spelled
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
) -> ComposeCanonicalConflictError:
    return ComposeCanonicalConflictError(
        f"compose contradicts the canonical map ({check}): {detail}. {hint}",
        check=check,
        subjects=subjects,
    )


def _incomplete(
    check: str,
    detail: str,
    hint: str,
    completion: Completion,
    *,
    subjects: tuple[str, ...] = (),
) -> ComposeIncompleteError:
    return ComposeIncompleteError(
        f"compose is incomplete ({check}): {detail}. {hint}",
        completion,
        check=check,
        subjects=subjects,
    )


# ── declared maps ───────────────────────────────────────────────────────────


def _moving(mapping: Mapping[str, str]) -> set[str]:
    return {source for source, target in mapping.items() if source != target}


def _targets(mapping: Mapping[str, str]) -> set[str]:
    return {target for source, target in mapping.items() if source != target}


def _merge_name_maps(
    base: Mapping[str, str], extension: Mapping[str, str], *, noun: str
) -> dict[str, str]:
    """One kind's half of :func:`merge_canonical_maps` — same verb, same operation."""
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


def merge_canonical_maps(base: CanonicalMap, extension: CanonicalMap) -> CanonicalMap:
    """Partial-function union of two declared maps; a target of either is a fixed point.

    Every source named by *base* or *extension* maps to exactly one target; a
    source the two disagree on raises :class:`ComposeCanonicalConflictError`.
    A target of either map is a fixed point the other may not move, checked
    in both directions so the result does not depend on which map is *base*.
    ``properties`` union the same way per source class.
    """
    vertices = _merge_name_maps(base.vertices, extension.vertices, noun="vertex")
    relations = _merge_name_maps(base.relations, extension.relations, noun="relation")
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
            allow_self_relations=allow_self_relations,
            allow_observation_fusion=allow_observation_fusion,
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


def _dangling_refusal(
    entries: Sequence[DanglingEntry], *, names: SideNames | None = None
) -> ComposeCanonicalConflictError:
    """One refusal naming every dangling entry on a side.

    A canonical map is authored as a whole, so it is corrected as a whole: the
    refusal lists each entry with a near-miss candidate where one exists,
    rather than surfacing the next mistake only after the previous one is
    fixed and the compose re-run.

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
    return ComposeCanonicalConflictError(
        f"compose contradicts the canonical map (dangling entry): "
        f"{len(entries)} {side} canonical map entries match nothing on that "
        f"side{context}. {hint}\n{listed}",
        check="dangling entry",
        subjects=subjects,
    )


@dataclass(frozen=True)
class ClusterResolution:
    """What compose applies: the resolved clusters and one composite relabel per side.

    ``declared`` is the folded declared vocabulary as seen from each side;
    ``side_maps`` is the composite — every cluster member onto its composed
    name, every applicable declared entry as written — one
    :class:`~graflo.architecture.evolution.ops.CanonicalizeOp` per side.
    ``index`` includes any cluster compose synthesized.
    """

    index: ClusterIndex
    side_maps: SideMaps
    declared: DeclaredMaps


def fold_declared_maps(
    op: ComposeManifestsOp, extra: Sequence[tuple[Side, CanonicalMap]]
) -> DeclaredMaps:
    scoped: dict[str, CanonicalMap] = {
        "left": CanonicalMap(),
        "right": CanonicalMap(),
        "both": CanonicalMap(),
    }
    for scope, cm in op.canonical_maps.items():
        scoped[scope] = merge_canonical_maps(scoped[scope], cm)
    for side, cm in extra:
        scoped[side] = merge_canonical_maps(scoped[side], cm)
    return DeclaredMaps(
        left=merge_canonical_maps(scoped["both"], scoped["left"]),
        right=merge_canonical_maps(scoped["both"], scoped["right"]),
        both=scoped["both"],
    )


def _mapping(cm: CanonicalMap | CanonicalizeOp, kind: Kind) -> dict[str, str]:
    return cm.vertices if kind == "vertex" else cm.relations


def _resolve_member(
    declared: str,
    *,
    names: frozenset[str],
    mapping: Mapping[str, str],
    side: Side,
    kind: Kind,
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
            subjects=tuple(subject(side, source) for source in sources),
        )
    return declared  # reported by the existence check


def _resolve_cluster(
    declaration: VertexEquivalence | RelationEquivalence,
    *,
    kind: Kind,
    declared: DeclaredMaps,
    names: Mapping[Side, SideNames],
    synthesized: bool,
) -> ClusterSpec:
    """Resolve one declaration's members and composed name against the declared maps.

    The composed name is ``into`` (translated through the maps when they map
    it), else the canonical name the maps give a member, else the one
    spelling every member shares. Then the commutation rule: every member the
    maps have an opinion about — a mapped source, or a canonical target, which
    is a fixed point — must agree with that name.
    """
    mapping = {side: _mapping(declared[side], kind) for side in _SIDES}
    side_names = {side: names[side].of_kind(kind) for side in _SIDES}
    targets = {side: _targets(mapping[side]) for side in _SIDES}

    members: dict[Side, list[str]] = {}
    aliases: dict[Side, dict[str, str]] = {}
    for side in _SIDES:
        resolved: list[str] = []
        for spelled in declaration.members(side):
            name = _resolve_member(
                spelled,
                names=side_names[side],
                mapping=mapping[side],
                side=side,
                kind=kind,
            )
            resolved.append(name)
            if name != spelled:
                aliases.setdefault(side, {})[spelled] = name
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
            # `opinions` values are already `side:member` ids.
            subjects=tuple(
                sorted(
                    {subject("composed", name) for name in opinions}
                    | {who for whos in opinions.values() for who in whos}
                )
            ),
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
                subjects=tuple(
                    subject("composed", name) for name in sorted(translated)
                ),
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
                subjects=tuple(
                    subject(side, member) for side in _SIDES for member in members[side]
                ),
            )
        label = spellings.pop()

    if opinions and label not in opinions:
        ((canonical_name, who),) = opinions.items()
        raise _conflict(
            f"{kind} disagreement",
            f"the canonical map says {', '.join(who)} is {canonical_name!r}, but "
            f"the equivalence names the composed {kind} {label!r}",
            "The two declarations must agree on where a name goes; canonical "
            "names are fixed points, so set `into` to the canonical name or "
            "fix the canonical map.",
            subjects=(
                *who,
                subject("composed", canonical_name),
                subject("composed", label),
            ),
        )
    return ClusterSpec(
        left=tuple(members["left"]),
        right=tuple(members["right"]),
        into=label,
        aliases=aliases,
        declared_into=declaration.into,
        synthesized=synthesized,
    )


def _cluster_maps(
    index: ClusterIndex, side: Side
) -> tuple[dict[str, str], dict[str, str], dict[str, dict[str, str]]]:
    """Every cluster member onto its composed name, with the declared attribute maps.

    A member that already equals the composed name gets a self entry: it
    declares the name a member of its own group, so the op merges into it
    rather than refusing an occupied target.
    """
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
    return vertices, relations, properties


def clusters_to_side_maps(index: ClusterIndex, *, allow_merges: bool) -> SideMaps:
    """Lower every cluster of *index* into a pair of per-side relabels, and nothing else."""
    ops: dict[Side, CanonicalizeOp] = {}
    for side in _SIDES:
        vertices, relations, properties = _cluster_maps(index, side)
        ops[side] = CanonicalizeOp(
            vertices=vertices,
            relations=relations,
            properties=properties,
            allow_merges=allow_merges,
        )
    return SideMaps(left=ops["left"], right=ops["right"])


def _extended_cluster_completion(
    index: ClusterIndex, *, kind: Kind, side: Side, composed: str, member: str
) -> Completion:
    clusters: Sequence[Cluster] = (
        index.vertices if kind == "vertex" else index.relations
    )
    cluster = next(c for c in clusters if c.into == composed)
    payload = cluster.declaration.to_dict(skip_defaults=True)
    payload[side] = [*cluster.members(side), member]
    payload["into"] = cluster.into
    if kind == "vertex":
        return Completion(
            kind="extend_cluster", side=side, vertex_equivalences=(payload,)
        )
    return Completion(
        kind="extend_cluster", side=side, relation_equivalences=(payload,)
    )


def _carry_declared_entries(
    out: dict[str, str],
    mapping: Mapping[str, str],
    *,
    names: frozenset[str],
    other_names: frozenset[str],
    shared_sources: Collection[str],
    index: ClusterIndex,
    side: Side,
    kind: Kind,
    dangling: list[DanglingEntry] | None = None,
) -> None:
    """Carry the declared map's entries for non-members into a side's composite.

    *dangling* selects how a dangling entry is reported. Left at ``None`` the
    entry is refused on the spot, which is what a caller classifying one entry
    at a time wants. Given a list, the entry is appended to it and skipped, and
    the caller refuses once for every kind it collected.

    *out* is that side's cluster map; every declared entry lands in one of
    five outcomes:

    * **member** — the source is in *out*: the commutation rule already holds
      for it, nothing to carry;
    * **satisfied** — the source is absent and the target present: assumed
      already applied by the caller, logged, not carried;
    * **translated** — the source is a composed name as the author spelled
      it: the cluster's ``into`` translation already used it;
    * **inapplicable** — a ``both``-scoped entry that matches the other side
      only: carried there, skipped here;
    * **dangling** — matches nothing anywhere it could apply: refused;
    * **carried** — otherwise, as declared — unless the target is a composed
      name, which makes the declaration incomplete: a class joining a composed
      class is governed by the cluster's identity and property maps, so it
      must be a member, and the refusal carries that extended cluster.
    """
    composed_names = index.labels if kind == "vertex" else index.relation_labels
    for source, target in mapping.items():
        if source in out:
            continue
        if source not in names:
            if target in names:
                logger.info(
                    "compose: %s canonical %s entry %r -> %r is already applied "
                    "(source absent, target present); carried as satisfied",
                    side,
                    kind,
                    source,
                    target,
                )
                continue
            if source in index.declared_intos or source in composed_names:
                continue
            if source in shared_sources and (
                source in other_names or target in other_names
            ):
                continue
            entry = DanglingEntry(
                side=side,
                kind=kind,
                source=source,
                target=target,
                suggestion=did_you_mean(source, names),
            )
            if dangling is None:
                raise _dangling_refusal((entry,))
            dangling.append(entry)
            continue
        if target in composed_names and target != source:
            raise _incomplete(
                f"{kind} joining a composed class",
                f"the canonical map sends {side}:{source!r} onto {target!r}, "
                "the composed name of a declared cluster, without declaring it "
                "a member",
                f"A {kind} joining a composed class is governed by the "
                "cluster's identity and property maps: declare it a member "
                "(the completion carries the extended cluster).",
                _extended_cluster_completion(
                    index, kind=kind, side=side, composed=target, member=source
                ),
                subjects=(subject(side, source), subject("composed", target)),
            )
        out[source] = target


def _side_map(
    op: ComposeManifestsOp,
    index: ClusterIndex,
    declared: DeclaredMaps,
    names: Mapping[Side, SideNames],
    *,
    side: Side,
    dangling: list[DanglingEntry],
) -> CanonicalizeOp:
    """One side's composite relabel; its dangling entries land in *dangling*."""
    cm = declared[side]
    other = _other(side)
    vertices, relations, properties = _cluster_maps(index, side)
    _carry_declared_entries(
        vertices,
        cm.vertices,
        names=names[side].vertices,
        other_names=names[other].vertices,
        shared_sources=declared.both.vertices,
        index=index,
        side=side,
        kind="vertex",
        dangling=dangling,
    )
    _carry_declared_entries(
        relations,
        cm.relations,
        names=names[side].relations,
        other_names=names[other].relations,
        shared_sources=declared.both.relations,
        index=index,
        side=side,
        kind="relation",
        dangling=dangling,
    )
    for cls, attrs in cm.properties.items():
        if cls not in names[side].vertices:
            if cm.canonical_class(cls) in names[side].vertices:
                continue  # satisfied: the class was already renamed away
            if cls in declared.both.properties and (
                cls in names[other].vertices
                or cm.canonical_class(cls) in names[other].vertices
            ):
                continue  # a `both` entry that applies to the other side
            if cls in index.labels or cls in index.declared_intos:
                # Not a missing name but a misplaced one, with its own hint:
                # refused on the spot rather than listed with the typos.
                raise _conflict(
                    "dangling entry",
                    f"the canonical map's {side} attribute map is keyed by "
                    f"{cls!r}, a composed name",
                    "`properties` is keyed by the source class: key the "
                    "attribute map by the member it applies to.",
                    subjects=(subject("composed", cls),),
                )
            dangling.append(
                DanglingEntry(
                    side=side,
                    kind="property",
                    source=cls,
                    suggestion=did_you_mean(cls, names[side].vertices),
                )
            )
            continue
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
                    subjects=(subject(side, cls, old),),
                )
            bucket[old] = new
    return CanonicalizeOp(
        vertices=vertices,
        relations=relations,
        properties=properties,
        allow_merges=op.allow_merges or cm.allow_merges,
        allow_self_relations=op.allow_self_relations,
        allow_observation_fusion=op.allow_observation_fusion,
    )


def _composite_side_maps(
    op: ComposeManifestsOp,
    index: ClusterIndex,
    declared: DeclaredMaps,
    names: Mapping[Side, SideNames],
) -> SideMaps:
    ops: dict[Side, CanonicalizeOp] = {}
    for side in _SIDES:
        cm = declared[side]
        dangling: list[DanglingEntry] = []
        try:
            built = _side_map(op, index, declared, names, side=side, dangling=dangling)
        except ComposeIncompleteError as exc:
            # A name that is not on the side at all is the more basic mistake,
            # and which of the two surfaced first used to be mapping order.
            if not dangling:
                raise
            raise _dangling_refusal(dangling, names=names[side]) from exc
        if dangling:
            if not (op.allow_dangling_entries or cm.allow_dangling_entries):
                raise _dangling_refusal(dangling, names=names[side])
            for entry in dangling:
                logger.info(
                    "compose: dropping the %s canonical %s, which matches "
                    "nothing on that side (allow_dangling_entries)%s",
                    entry.side,
                    entry.describe(),
                    entry.suggestion,
                )
        ops[side] = built
    return SideMaps(left=ops["left"], right=ops["right"])


def _check_property_fields_exist(
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


def _check_attribute_fixed_points(
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


def _check_property_maps_against_manifest(
    manifest: GraphManifest, relabel: CanonicalizeOp, *, side: Side
) -> None:
    """Refuse a property rename whose old name is absent or whose new name collides.

    A collision would otherwise drop the losing field at apply time; this
    turns it into a loud compose-time error instead of a quiet data loss.
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


def _occupancy(names: frozenset[str], mapping: Mapping[str, str]) -> frozenset[str]:
    """Names a composed name could collide with: those the declared map does not move away."""
    return names - _moving(mapping)


def _resolve(
    op: ComposeManifestsOp,
    *,
    declared: DeclaredMaps,
    names: Mapping[Side, SideNames],
    manifests: Mapping[Side, GraphManifest],
    synthesized_from: tuple[int, int],
) -> ClusterResolution:
    """Resolve *op*'s clusters as declared; *synthesized_from* marks the tail compose added."""
    vertex_specs = [
        _resolve_cluster(
            v,
            kind="vertex",
            declared=declared,
            names=names,
            synthesized=i >= synthesized_from[0],
        )
        for i, v in enumerate(op.vertex_equivalences)
    ]
    relation_specs = [
        _resolve_cluster(
            r,
            kind="relation",
            declared=declared,
            names=names,
            synthesized=i >= synthesized_from[1],
        )
        for i, r in enumerate(op.relation_equivalences)
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
        left_vertices=_occupancy(names["left"].vertices, declared.left.vertices),
        right_vertices=_occupancy(names["right"].vertices, declared.right.vertices),
        left_relations=_occupancy(names["left"].relations, declared.left.relations),
        right_relations=_occupancy(names["right"].relations, declared.right.relations),
        vertex_specs=vertex_specs,
        relation_specs=relation_specs,
    )
    for side in _SIDES:
        for cluster in index.vertices:
            _check_property_fields_exist(
                manifests[side], cluster, side=side, declared=declared[side]
            )
            _check_attribute_fixed_points(cluster, side=side, declared=declared[side])
    side_maps = _composite_side_maps(op, index, declared, names)
    for side in _SIDES:
        _check_property_maps_against_manifest(
            manifests[side], side_maps[side], side=side
        )
    return ClusterResolution(index=index, side_maps=side_maps, declared=declared)


def _preimages(
    mapping: Mapping[str, str], names: frozenset[str]
) -> dict[str, list[str]]:
    """``{post-map name: [own names landing on it]}`` over one side."""
    groups: dict[str, list[str]] = {}
    for name in sorted(names):
        groups.setdefault(mapping.get(name, name), []).append(name)
    return groups


def _members_field(members: Sequence[str]) -> str | list[str]:
    return members[0] if len(members) == 1 else list(members)


def same_name_groups(
    resolution: ClusterResolution,
    names: Mapping[Side, SideNames],
    *,
    kind: Kind,
    near: bool,
) -> list[tuple[str, list[str], list[str]]]:
    """``(composed name, left members, right members)`` for every name no cluster covers.

    Names are compared after each side's composite map, since that is what
    the union sees. With *near*, two spellings of one name (``canonical_slug``
    alike) form one group too, composed under the left spelling.
    """
    index = resolution.index
    composed = index.labels if kind == "vertex" else index.relation_labels
    pre = {
        side: _preimages(
            _mapping(resolution.side_maps[side], kind), names[side].of_kind(kind)
        )
        for side in _SIDES
    }
    groups: list[tuple[str, list[str], list[str]]] = []
    if near:
        by_key: dict[str, dict[Side, list[str]]] = {}
        for side in _SIDES:
            for post_name in pre[side]:
                if post_name in composed:
                    continue
                by_key.setdefault(canonical_slug(post_name), {}).setdefault(
                    side, []
                ).append(post_name)
        for _key, sides in sorted(by_key.items()):
            left_post = sorted(sides.get("left", []))
            right_post = sorted(sides.get("right", []))
            if not left_post or not right_post:
                continue
            exact = sorted(set(left_post) & set(right_post))
            into = exact[0] if exact else left_post[0]
            groups.append(
                (
                    into,
                    [m for n in left_post for m in pre["left"][n]],
                    [m for n in right_post for m in pre["right"][n]],
                )
            )
        return groups
    for post_name in sorted(set(pre["left"]) & set(pre["right"])):
        if post_name in composed:
            continue
        groups.append((post_name, pre["left"][post_name], pre["right"][post_name]))
    return groups


def _same_name_payloads(
    groups: Sequence[tuple[str, list[str], list[str]]],
) -> tuple[dict[str, Any], ...]:
    return tuple(
        {"left": _members_field(left), "right": _members_field(right), "into": into}
        for into, left, right in groups
    )


def resolve_clusters(
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> ClusterResolution:
    """Resolve *op*'s clusters against its declared maps and build the per-side composite.

    *canonical_maps* are extra ``(side, map)`` pairs folded into
    ``op.canonical_maps``. *left* / *right* are the manifests about to be
    composed, in whatever vocabulary they are in: a declared entry whose
    source is absent on its side but whose target is present is satisfied and
    is a no-op; one matching nothing is refused as a typo.

    A name both sides carry after their composite maps, and no cluster
    composes, is what ``op.name_conflict`` decides: ``error`` refuses it as
    incomplete, naming the equivalences to declare; ``union_right`` declares
    them itself (a **synthesized** cluster, so the union goes through the
    same identity and property reconciliation as a declared one — two
    spellings of one name, ``canonical_slug`` alike, are one such cluster
    under the left spelling); ``prefix_right`` leaves them to compose to keep
    apart.

    Raises :class:`~graflo.architecture.evolution.equivalence.ClusterConflictError`
    when the declared clusters themselves conflict,
    :class:`ComposeCanonicalConflictError` when a map and the equivalences
    disagree — a member the map sends elsewhere than the composed name, a
    canonical class or attribute re-targeted by a cluster, a cluster with no
    name, a dangling entry, a property equivalence naming an absent or
    colliding field — and :class:`ComposeIncompleteError` when an extension
    would resolve it: a map entry sending a non-member onto a composed name,
    or a shared name under ``name_conflict="error"``.
    """
    declared = fold_declared_maps(op, canonical_maps)
    names: dict[Side, SideNames] = {
        "left": SideNames.of(left),
        "right": SideNames.of(right),
    }
    manifests: dict[Side, GraphManifest] = {"left": left, "right": right}
    resolution = _resolve(
        op,
        declared=declared,
        names=names,
        manifests=manifests,
        synthesized_from=(len(op.vertex_equivalences), len(op.relation_equivalences)),
    )
    if op.name_conflict == "prefix_right":
        return resolution

    near = op.name_conflict == "union_right"
    vertex_groups = same_name_groups(resolution, names, kind="vertex", near=near)
    relation_groups = same_name_groups(resolution, names, kind="relation", near=near)
    if not vertex_groups and not relation_groups:
        return resolution

    if op.name_conflict == "error":
        kind: Kind = "vertex" if vertex_groups else "relation"
        shared = [into for into, _l, _r in (vertex_groups or relation_groups)]
        raise _incomplete(
            f"{kind} name collision",
            f"{shared} exist on both sides and no equivalence composes them",
            "Declare the equivalences the completion carries, set "
            "name_conflict='union_right' to union by name, or "
            "name_conflict='prefix_right' to keep them apart.",
            Completion(
                kind="declare_equivalences",
                vertex_equivalences=_same_name_payloads(vertex_groups),
                relation_equivalences=_same_name_payloads(relation_groups),
            ),
            subjects=tuple(subject("composed", name) for name in shared),
        )

    nary = any(
        len(l_members) > 1 or len(r_members) > 1
        for _into, l_members, r_members in (*vertex_groups, *relation_groups)
    )
    extended = op.model_copy(
        update={
            "vertex_equivalences": [
                *op.vertex_equivalences,
                *(
                    VertexEquivalence.model_validate(payload)
                    for payload in _same_name_payloads(vertex_groups)
                ),
            ],
            "relation_equivalences": [
                *op.relation_equivalences,
                *(
                    RelationEquivalence.model_validate(payload)
                    for payload in _same_name_payloads(relation_groups)
                ),
            ],
            "allow_merges": op.allow_merges or nary,
        }
    )
    return _resolve(
        extended,
        declared=declared,
        names=names,
        manifests=manifests,
        synthesized_from=(len(op.vertex_equivalences), len(op.relation_equivalences)),
    )


def validate_and_complete_canonical_map(
    op: ComposeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> SideMaps:
    """Validate *op* against its declared maps and return the completed per-side relabels.

    The composed name of every cluster is completed — from ``into``, the
    declared map, or the members' shared spelling — and every member maps
    onto it; the declared map's remaining entries are carried as written.
    Apply the result to each side with :func:`canonicalize_ops` before the
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
            f"compose contradicts the canonical map (cluster conflict): {exc}",
            check=exc.check or "cluster conflict",
            subjects=exc.subjects,
        ) from exc


def dangling_entries(
    cm: CanonicalMap, manifest: GraphManifest, *, side: Side = "left"
) -> tuple[DanglingEntry, ...]:
    """The map's entries that match nothing in *manifest*, with near-miss candidates.

    A canonical map is authored against one manifest long before it is composed
    against another, and this is that check on its own: no equivalences, no
    other side, no compose. With no clusters declared there are no composed
    names and no ``both`` scope, so the classification compose uses collapses
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

    Trimming is an authoring step, not something compose does on its own: the
    result is a value to inspect and save, so that a map narrowed to a manifest
    is a change with a diff rather than a silent omission at compose time. A
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
