"""One naming graph over the original names of both sides of a merge.

The nodes are the classes and relations each manifest declares, under their
own names, never erased. Three declarations add edges:

* the **vocabulary** (``canonical_maps``) — a class's default name in the
  union; several classes a side's vocabulary sends to one name are merged by
  it, which its ``allow_merges`` acknowledges;
* an **equivalence** — its members are one class, across the two sides;
* ``renames`` — a class or relation no group holds, called something else.

A **group** is a connected component over the merging edges (vocabulary
merges, equivalence membership, and the pairs ``union_right`` unions), so an
equivalence naming one class a vocabulary merges with others takes all of
them. A group is named by its equivalences' ``into`` (a name in the union,
never translated), else by the vocabulary, else by the one spelling its
members share. Everything not in a group keeps its rename, else its
vocabulary name, else its own.

The merge is valid when every union name is reached by exactly one group or
one ungrouped class, every declared name exists, and every group has one name
and one identity. :func:`build_naming` reports every violation as a
:class:`NamingFinding` instead of raising, so a caller sees all of them at
once; :func:`resolve_naming` raises them together as one
:class:`MergeNamingError`.

Per-member maps stay keyed by the members' own names, and
:func:`~graflo.architecture.evolution.merge.merge_manifests` reads each side
as handed in, before the relabel. So class-specific identity sources, property
maps and the ``when`` guards derived from how a resource produces a member
survive the merge. A member a vocabulary joins whose producing resources the
group's identity does not key is given its own key behind a tag
(``side:Class``): it belongs to the class, but its records never fuse with
another member's.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.refusal import Refusal

from .canonical import (
    ClusterResolution,
    Completion,
    DanglingEntry,
    DeclaredMaps,
    MergeCanonicalConflictError,
    MergeIncompleteError,
    SideMaps,
    SideNames,
    canonical_near_collisions,
    check_attribute_fixed_points,
    check_property_fields_exist,
    check_property_maps_against_manifest,
    dangling_refusal,
    fold_declared_maps,
)
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
    DerivedBranch,
    IdentityBranchDecl,
    LocalKeyBranch,
    LocalKeySource,
    MergeManifestsOp,
    PropertyEquivalence,
    RelationEquivalence,
    VertexEquivalence,
    branch_fields,
)

logger = logging.getLogger(__name__)

_SIDES: tuple[Side, ...] = ("left", "right")
_KINDS: tuple[Kind, ...] = ("vertex", "relation")

#: How much a finding matters: ``refusal`` blocks the merge, ``incomplete``
#: blocks it until something is added, ``note`` changes nothing.
Severity = Literal["refusal", "incomplete", "note"]

#: Where a class's union name came from.
Via = Literal["equivalence", "vocabulary", "rename", "union_right", "own name"]

#: A node: which kind of name, which side, the name as that side spells it.
NodeKey = tuple[Kind, Side, str]

#: The finding kind of each refusal
#: :func:`~graflo.architecture.evolution.canonical.check_property_maps_against_manifest`
#: raises, by its ``check``.
_PROPERTY_MAP_KINDS: dict[str, str] = {
    "unknown property": "unknown_property",
    "property rename collision": "property_collision",
}


@dataclass(frozen=True)
class NamingFinding:
    """One violation of the naming rules, or one acknowledged default.

    ``kind`` is the rule, ``subjects`` the names it is about as
    :func:`~graflo.architecture.evolution.equivalence.subject` ids, and
    ``repairs`` the declarations that would settle it, safest first.
    """

    kind: str
    severity: Severity
    message: str
    subjects: tuple[str, ...] = ()
    repairs: tuple[Completion, ...] = ()
    check: str = ""

    @property
    def blocking(self) -> bool:
        """Whether this finding stops the merge."""
        return self.severity != "note"


def _describe_findings(findings: Sequence[NamingFinding]) -> str:
    if len(findings) == 1:
        return findings[0].message
    listed = "\n".join(
        f"  {position}. {finding.message}"
        for position, finding in enumerate(findings, start=1)
    )
    return f"{len(findings)} problems with the declarations:\n{listed}"


class MergeNamingError(MergeCanonicalConflictError):
    """Every naming problem of one merge op, raised together.

    ``findings`` lists them; ``check`` and ``completion`` are the first one's,
    and ``subjects`` names every node any of them is about.
    """

    def __init__(self, findings: Sequence[NamingFinding]) -> None:
        blocking = tuple(f for f in findings if f.blocking) or tuple(findings)
        Refusal.__init__(
            self,
            _describe_findings(blocking),
            check=blocking[0].check,
            subjects=tuple(dict.fromkeys(s for f in blocking for s in f.subjects)),
        )
        self.findings = blocking
        self.completion: Completion | None = (
            blocking[0].repairs[0] if blocking[0].repairs else None
        )


class MergeNamingIncompleteError(MergeNamingError, MergeIncompleteError):
    """Every naming problem is one an addition settles; ``completion`` says which."""


@dataclass(frozen=True)
class NamingEdge:
    """One thing a declaration says about where a name goes."""

    kind: Literal["member", "vocabulary", "rename", "union_right"]
    category: Kind
    side: Side
    source: str
    target: str | None
    declared_by: str


@dataclass(frozen=True)
class NamingGroup:
    """One group: its union name, and each member with where its name came from."""

    kind: Kind
    name: str | None
    members: tuple[tuple[Side, str, Via], ...]
    equivalences: tuple[int, ...] = ()
    synthesized: bool = False

    def side_members(self, side: Side) -> tuple[str, ...]:
        return tuple(name for s, name, _via in self.members if s == side)


@dataclass(frozen=True)
class NamingGraph:
    """Every node's union name and provenance, the groups, and the edges that put them there."""

    targets: Mapping[NodeKey, str | None]
    via: Mapping[NodeKey, Via]
    groups: tuple[NamingGroup, ...]
    edges: tuple[NamingEdge, ...]

    def rows(
        self, kind: Kind = "vertex"
    ) -> list[tuple[str, list[str], list[str], str]]:
        """``(union name, left names, right names, via)`` per union name of *kind*, sorted.

        One row per provenance, so a group joined partly through a vocabulary
        shows the vocabulary-joined members on their own row.
        """
        out: dict[tuple[str, str], tuple[list[str], list[str]]] = {}
        for (node_kind, side, name), target in self.targets.items():
            if node_kind != kind:
                continue
            union = target if target is not None else "?"
            via = self.via.get((node_kind, side, name), "own name")
            left, right = out.setdefault((union, via), ([], []))
            (left if side == "left" else right).append(name)
        order = {"equivalence": 0, "union_right": 1, "vocabulary": 2, "rename": 3}
        return [
            (union, sorted(left), sorted(right), via)
            for (union, via), (left, right) in sorted(
                out.items(), key=lambda item: (item[0][0], order.get(item[0][1], 4))
            )
        ]


@dataclass(frozen=True)
class NamingResult:
    """What a merge would apply, and everything wrong with its declarations."""

    resolution: ClusterResolution
    graph: NamingGraph
    findings: tuple[NamingFinding, ...]

    @property
    def blocking(self) -> tuple[NamingFinding, ...]:
        return tuple(f for f in self.findings if f.blocking)

    @property
    def notes(self) -> tuple[NamingFinding, ...]:
        return tuple(f for f in self.findings if not f.blocking)

    def raise_if_blocking(self) -> None:
        """Raise every blocking finding as one :class:`MergeNamingError`."""
        blocking = self.blocking
        if not blocking:
            return
        if all(f.severity == "incomplete" for f in blocking):
            raise MergeNamingIncompleteError(blocking)
        raise MergeNamingError(blocking)


# ── union-find ──────────────────────────────────────────────────────────────


class _Components:
    """Disjoint sets over nodes, in insertion order so results are stable."""

    def __init__(self, nodes: Iterable[NodeKey]) -> None:
        self._parent: dict[NodeKey, NodeKey] = {node: node for node in nodes}

    def find(self, node: NodeKey) -> NodeKey:
        root = node
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[node] != root:
            self._parent[node], node = root, self._parent[node]
        return root

    def union(self, nodes: Sequence[NodeKey]) -> None:
        if not nodes:
            return
        head = self.find(nodes[0])
        for node in nodes[1:]:
            root = self.find(node)
            if root != head:
                self._parent[root] = head

    def groups(self) -> list[list[NodeKey]]:
        by_root: dict[NodeKey, list[NodeKey]] = {}
        for node in self._parent:
            by_root.setdefault(self.find(node), []).append(node)
        return list(by_root.values())


# ── declarations as resolved ────────────────────────────────────────────────


@dataclass
class _Declaration:
    """One equivalence, its members resolved to names the sides declare."""

    position: int
    kind: Kind
    declaration: VertexEquivalence | RelationEquivalence
    members: dict[Side, list[str]]
    aliases: dict[Side, dict[str, str]]
    synthesized: bool = False

    @property
    def nodes(self) -> list[NodeKey]:
        return [
            (self.kind, side, name) for side in _SIDES for name in self.members[side]
        ]

    @property
    def label(self) -> str:
        return f"{self.kind}-equivalence-{self.position}"


@dataclass
class _Component:
    """One component of the naming graph, named or not."""

    kind: Kind
    nodes: list[NodeKey]
    declarations: list[_Declaration]
    name: str | None = None
    #: The one ``into`` the group's equivalences spell, whichever spells it.
    into: str | None = None

    @property
    def grouped(self) -> bool:
        return bool(self.declarations)

    def sides(self) -> set[Side]:
        return {side for _kind, side, _name in self.nodes}

    def on(self, side: Side) -> list[str]:
        return [name for _kind, s, name in self.nodes if s == side]


def _free_name(base: str, taken: Collection[str], suffix: str) -> str:
    candidate = f"{base}_{suffix}"
    ordinal = 2
    while candidate in taken:
        candidate = f"{base}_{suffix}_{ordinal}"
        ordinal += 1
    return candidate


def _members_field(members: Sequence[str]) -> str | list[str]:
    return members[0] if len(members) == 1 else list(members)


@dataclass
class _Naming:
    """The tolerant pass: builds the graph, collects findings, lowers what it can."""

    op: MergeManifestsOp
    manifests: dict[Side, GraphManifest]
    extra: Sequence[tuple[Side, CanonicalMap]]
    names: dict[Side, SideNames] = field(default_factory=dict)
    declared: DeclaredMaps = field(
        default_factory=lambda: DeclaredMaps(left=CanonicalMap(), right=CanonicalMap())
    )
    findings: list[NamingFinding] = field(default_factory=list)
    #: ``{kind: {side: {source: target}}}`` for vocabulary entries whose source
    #: the side declares, self entries included.
    vocab: dict[Kind, dict[Side, dict[str, str]]] = field(default_factory=dict)
    #: The same shape for ``renames`` entries whose source the side declares.
    renames: dict[Kind, dict[Side, dict[str, str]]] = field(default_factory=dict)
    edges: list[NamingEdge] = field(default_factory=list)

    # ── findings ────────────────────────────────────────────────────────────

    def finding(
        self,
        kind: str,
        message: str,
        *,
        severity: Severity = "refusal",
        subjects: Iterable[str] = (),
        repairs: Iterable[Completion] = (),
        check: str = "",
    ) -> None:
        self.findings.append(
            NamingFinding(
                kind=kind,
                severity=severity,
                message=message,
                subjects=tuple(dict.fromkeys(subjects)),
                repairs=tuple(repairs),
                check=check or kind.replace("_", " "),
            )
        )

    def note(self, kind: str, message: str, *, subjects: Iterable[str] = ()) -> None:
        self.finding(kind, message, severity="note", subjects=subjects)

    def from_refusal(self, exc: ValueError, *, kind: str) -> None:
        """Record a refusal one of the shared checks raised, as a finding of *kind*."""
        refusal = exc if isinstance(exc, Refusal) else None
        self.finding(
            kind,
            str(exc),
            subjects=refusal.subjects if refusal is not None else (),
            repairs=(exc.completion,) if isinstance(exc, MergeIncompleteError) else (),
            check=(refusal.check if refusal is not None else "")
            or kind.replace("_", " "),
        )

    # ── the pass ────────────────────────────────────────────────────────────

    def run(self) -> NamingResult:
        self.names = {side: SideNames.of(self.manifests[side]) for side in _SIDES}
        try:
            self.declared = fold_declared_maps(self.op, self.extra)
        except MergeCanonicalConflictError as exc:
            self.from_refusal(exc, kind="disagreement")
        declarations = self._declarations()
        self._vocabulary(declarations)
        self._renames()

        components = self._components(declarations)
        mark = len(self.findings)
        self._name_all(components)
        synthesized = self._name_policy(components, declarations)
        if synthesized:
            # The names are found again with the unions in place; the first
            # pass's findings would be reported twice.
            del self.findings[mark:]
            declarations = {
                kind: [*declarations[kind], *synthesized.get(kind, [])]
                for kind in _KINDS
            }
            components = self._components(declarations)
            self._name_all(components)
        self._near_collisions(components)
        self._fibers(components)
        self._double_homes(components)
        self._property_vocabulary(components)

        clusters = self._clusters(components)
        index = ClusterIndex(
            vertices=tuple(
                c for c in clusters if isinstance(c.declaration, VertexEquivalence)
            ),
            relations=tuple(
                cast(Cluster[RelationEquivalence], c)
                for c in clusters
                if isinstance(c.declaration, RelationEquivalence)
            ),
        )
        side_maps = self._side_maps(components, index)
        self._consequences(components)
        self._property_checks(index, side_maps)
        graph = self._graph(components)
        findings = tuple(self.findings)
        return NamingResult(
            resolution=ClusterResolution(
                index=index,
                side_maps=side_maps,
                declared=self.declared,
                findings=tuple(f for f in findings if not f.blocking),
                graph=graph,
            ),
            graph=graph,
            findings=findings,
        )

    # ── declarations ────────────────────────────────────────────────────────

    def _active_vocab(self, kind: Kind, side: Side) -> dict[str, str]:
        cm = self.declared[side]
        mapping = cm.vertices if kind == "vertex" else cm.relations
        known = self.names[side].of_kind(kind)
        return {s: t for s, t in mapping.items() if s in known}

    def _declarations(self) -> dict[Kind, list[_Declaration]]:
        out: dict[Kind, list[_Declaration]] = {"vertex": [], "relation": []}
        sources: tuple[
            tuple[Kind, Sequence[VertexEquivalence | RelationEquivalence]], ...
        ] = (
            ("vertex", self.op.vertex_equivalences),
            ("relation", self.op.relation_equivalences),
        )
        for kind, declared in sources:
            claimed: dict[tuple[Side, str], int] = {}
            for position, declaration in enumerate(declared):
                resolved = self._resolve_members(position, kind, declaration)
                for side in _SIDES:
                    for name in resolved.members[side]:
                        prior = claimed.get((side, name))
                        if prior is not None and prior != position:
                            self.finding(
                                "cluster_overlap",
                                f"cluster overlap: {kind} {side}:{name} is declared by two "
                                f"equivalences (#{prior} and #{position}); one class "
                                "is declared in one equivalence — fold them into one",
                                subjects=(subject(side, name),),
                                check="cluster overlap",
                            )
                        claimed[(side, name)] = position
                if all(resolved.members[side] for side in _SIDES):
                    out[kind].append(resolved)
        return out

    def _resolve_members(
        self,
        position: int,
        kind: Kind,
        declaration: VertexEquivalence | RelationEquivalence,
    ) -> _Declaration:
        """Members as the sides declare them; a canonical spelling stands for every class it names."""
        members: dict[Side, list[str]] = {"left": [], "right": []}
        aliases: dict[Side, dict[str, str]] = {"left": {}, "right": {}}
        for side in _SIDES:
            known = self.names[side].of_kind(kind)
            vocab = self._active_vocab(kind, side)
            for spelled in declaration.members(side):
                if spelled in known:
                    resolved = [spelled]
                else:
                    resolved = sorted(
                        s for s, t in vocab.items() if t == spelled and s != t
                    )
                    if len(resolved) == 1:
                        aliases[side][spelled] = resolved[0]
                if not resolved:
                    self.finding(
                        "unknown_member",
                        f"unknown {kind} member: {spelled!r} is not in the {side} "
                        f"manifest{did_you_mean(spelled, known)}",
                        subjects=(subject(side, spelled),),
                        check=f"unknown {kind} member",
                    )
                for name in resolved:
                    if name not in members[side]:
                        members[side].append(name)
        return _Declaration(
            position=position,
            kind=kind,
            declaration=declaration,
            members=members,
            aliases=aliases,
        )

    # ── the vocabulary and renames ──────────────────────────────────────────

    def _vocabulary(self, declarations: Mapping[Kind, Sequence[_Declaration]]) -> None:
        """Classify every vocabulary entry: active, already applied, other side's, or dangling."""
        intos = {
            d.declaration.into
            for kind in _KINDS
            for d in declarations[kind]
            if d.declaration.into is not None
        }
        for kind in _KINDS:
            self.vocab[kind] = {}
            for side in _SIDES:
                other: Side = "right" if side == "left" else "left"
                cm = self.declared[side]
                mapping = cm.vertices if kind == "vertex" else cm.relations
                shared = (
                    self.declared.both.vertices
                    if kind == "vertex"
                    else self.declared.both.relations
                )
                known = self.names[side].of_kind(kind)
                other_known = self.names[other].of_kind(kind)
                active: dict[str, str] = {}
                dangling: list[DanglingEntry] = []
                for source, target in mapping.items():
                    if source in known:
                        active[source] = target
                        self.edges.append(
                            NamingEdge(
                                "vocabulary",
                                kind,
                                side,
                                source,
                                target,
                                f"vocabulary:{side}",
                            )
                        )
                        continue
                    if target in known:
                        logger.info(
                            "merge: %s canonical %s entry %r -> %r is already applied "
                            "(source absent, target present); carried as satisfied",
                            side,
                            kind,
                            source,
                            target,
                        )
                        self.note(
                            "satisfied",
                            f"the {side} canonical {kind} entry {source!r} -> {target!r} "
                            "is taken as already applied (source absent, target present)",
                            subjects=(subject(side, target),),
                        )
                        continue
                    if source in shared and (
                        source in other_known or target in other_known
                    ):
                        continue
                    suggestion = did_you_mean(source, known)
                    if not suggestion and source in intos:
                        suggestion = (
                            f"; {source!r} is an `into`, and a vocabulary renames "
                            "the sides' names, never merged ones — set `into` to "
                            f"{target!r} instead"
                        )
                    dangling.append(
                        DanglingEntry(
                            side=side,
                            kind=kind,
                            source=source,
                            target=target,
                            suggestion=suggestion,
                        )
                    )
                self.vocab[kind][side] = active
                self._report_dangling(side, dangling)

    def _report_dangling(self, side: Side, entries: Sequence[DanglingEntry]) -> None:
        if not entries:
            return
        if self.op.allow_dangling_entries or self.declared[side].allow_dangling_entries:
            for entry in entries:
                logger.info(
                    "merge: dropping the %s canonical %s, which matches "
                    "nothing on that side (allow_dangling_entries)%s",
                    entry.side,
                    entry.describe(),
                    entry.suggestion,
                )
            return
        # One finding per entry, so the count is the number of mistakes; the
        # merge error still names every one of them at once.
        for entry in entries:
            self.from_refusal(
                dangling_refusal((entry,), names=self.names[side]), kind="dangling"
            )

    def _renames(self) -> None:
        for kind in _KINDS:
            self.renames[kind] = {}
            for side in _SIDES:
                declared = self.op.renames[side]
                mapping = declared.vertices if kind == "vertex" else declared.relations
                known = self.names[side].of_kind(kind)
                active: dict[str, str] = {}
                for source, target in mapping.items():
                    if source in known:
                        active[source] = target
                        self.edges.append(
                            NamingEdge(
                                "rename", kind, side, source, target, f"renames:{side}"
                            )
                        )
                        continue
                    self.finding(
                        "dangling",
                        f"dangling entry: renames.{side}.{'vertices' if kind == 'vertex' else 'relations'} "
                        f"renames {source!r}, which the {side} manifest does not "
                        f"declare{did_you_mean(source, known)}",
                        subjects=(subject(side, source),),
                        check="dangling entry",
                    )
                self.renames[kind][side] = active
        for side in _SIDES:
            declared = self.op.renames[side]
            for cls in declared.properties:
                if cls not in self.names[side].vertices:
                    self.finding(
                        "dangling",
                        f"dangling entry: renames.{side}.properties is keyed by {cls!r}, which the "
                        f"{side} manifest does not declare"
                        f"{did_you_mean(cls, self.names[side].vertices)}",
                        subjects=(subject(side, cls),),
                        check="dangling entry",
                    )
            ingestion = self.manifests[side].ingestion_model
            resources = {r.name for r in ingestion.resources} if ingestion else set()
            for resource in declared.resources:
                if resource not in resources:
                    self.finding(
                        "dangling",
                        f"dangling entry: renames.{side}.resources renames {resource!r}, which the "
                        f"{side} manifest does not declare"
                        f"{did_you_mean(resource, resources)}",
                        check="dangling entry",
                    )

    # ── groups ──────────────────────────────────────────────────────────────

    def _components(
        self, declarations: Mapping[Kind, Sequence[_Declaration]]
    ) -> list[_Component]:
        out: list[_Component] = []
        for kind in _KINDS:
            nodes: list[NodeKey] = [
                (kind, side, name)
                for side in _SIDES
                for name in sorted(self.names[side].of_kind(kind))
            ]
            components = _Components(nodes)
            for side in _SIDES:
                by_target: dict[str, list[str]] = {}
                for source, target in self.vocab[kind][side].items():
                    by_target.setdefault(target, []).append(source)
                for sources in by_target.values():
                    if len(sources) > 1:
                        components.union([(kind, side, s) for s in sources])
            for declaration in declarations[kind]:
                components.union(declaration.nodes)
            for group in components.groups():
                members = set(group)
                out.append(
                    _Component(
                        kind=kind,
                        nodes=group,
                        declarations=[
                            d for d in declarations[kind] if members & set(d.nodes)
                        ],
                    )
                )
        return out

    def _vocab_target(self, node: NodeKey) -> str | None:
        kind, side, name = node
        return self.vocab[kind][side].get(name)

    def _name_all(self, components: Sequence[_Component]) -> None:
        for component in components:
            component.name = self._name(component)

    def _name(self, component: _Component) -> str | None:
        kind = component.kind
        targets = list(
            dict.fromkeys(
                t
                for node in component.nodes
                if (t := self._vocab_target(node)) is not None
            )
        )
        if not component.grouped:
            if len(component.nodes) > 1:
                return targets[0]
            ((_kind, side, name),) = component.nodes
            renamed = self.renames[kind][side].get(name)
            vocab = self._vocab_target(component.nodes[0])
            if renamed is not None and vocab is not None and vocab != renamed:
                self.note(
                    "vocabulary_override",
                    f"renames.{side} calls {side}:{name} {renamed!r}; the vocabulary "
                    f"called it {vocab!r}",
                    subjects=(subject(side, name), subject("merged", renamed)),
                )
            return renamed or vocab or name

        left, right = component.on("left"), component.on("right")
        intos = list(
            dict.fromkeys(
                d.declaration.into for d in component.declarations if d.declaration.into
            )
        )
        members = tuple(subject(side, name) for _k, side, name in component.nodes)
        if len(intos) > 1:
            self.finding(
                "disagreement",
                f"{kind} into disagreement: the equivalences of one "
                f"group {left} ~ {right} name it {intos}; one group has one name",
                subjects=(*members, *(subject("merged", n) for n in intos)),
                repairs=tuple(self._set_into(component, name) for name in intos),
                check=f"{kind} into disagreement",
            )
            return None
        if intos:
            (name,) = intos
            component.into = name
            overridden = [t for t in targets if t != name]
            if overridden:
                self.note(
                    "vocabulary_override",
                    f"`into` names the group {left} ~ {right} {name!r}; the vocabulary "
                    f"called it {overridden[0]!r}"
                    + (f" (and {overridden[1:]})" if len(overridden) > 1 else ""),
                    subjects=(*members, subject("merged", name)),
                )
            return name
        if len(targets) == 1:
            return targets[0]
        if len(targets) > 1:
            self.finding(
                "unnamed_cluster",
                f"unnamed {kind} cluster: the vocabularies name the "
                f"members of {left} ~ {right} differently ({targets}); give the "
                "equivalence `into`",
                subjects=members,
                repairs=tuple(self._set_into(component, name) for name in targets),
                check=f"unnamed {kind} cluster",
            )
            return None
        spellings = list(dict.fromkeys(name for _k, _s, name in component.nodes))
        if len(spellings) == 1:
            return spellings[0]
        self.finding(
            "unnamed_cluster",
            f"unnamed {kind} cluster: {left} ~ {right} has no merged "
            "name — its members are spelled differently and no vocabulary names "
            "them. Give the equivalence `into`.",
            subjects=members,
            repairs=tuple(self._set_into(component, name) for name in spellings),
            check=f"unnamed {kind} cluster",
        )
        return None

    def _payload(self, declaration: _Declaration) -> dict[str, Any]:
        return declaration.declaration.to_dict(skip_defaults=True)

    def _set_into(self, component: _Component, name: str) -> Completion:
        declaration = component.declarations[0]
        payload = self._payload(declaration)
        payload["into"] = name
        return self._completion(
            "set_into",
            component.kind,
            payload,
            label=f"name it {name!r}",
            replaces=None if declaration.synthesized else declaration.position,
        )

    def _completion(
        self,
        kind: Literal[
            "extend_cluster",
            "set_into",
            "add_key_source",
            "declare_equivalences",
            "acknowledge",
        ],
        category: Kind,
        payload: dict[str, Any],
        *,
        side: Side | None = None,
        label: str = "",
        replaces: int | None = None,
    ) -> Completion:
        if category == "vertex":
            return Completion(
                kind=kind,
                side=side,
                vertex_equivalences=(payload,),
                label=label,
                replaces=replaces,
            )
        return Completion(
            kind=kind,
            side=side,
            relation_equivalences=(payload,),
            label=label,
            replaces=replaces,
        )

    # ── names both sides arrive at ──────────────────────────────────────────

    def _name_policy(
        self,
        components: Sequence[_Component],
        declarations: Mapping[Kind, Sequence[_Declaration]],
    ) -> dict[Kind, list[_Declaration]]:
        """Ungrouped names both sides arrive at: refused, unioned or left to prefix."""
        policy = self.op.name_conflict
        shared: dict[Kind, list[tuple[str, list[str], list[str]]]] = {}
        for kind in _KINDS:
            by_name: dict[str, dict[Side, list[_Component]]] = {}
            for component in components:
                if (
                    component.kind != kind
                    or component.grouped
                    or component.name is None
                ):
                    continue
                (side,) = component.sides()
                by_name.setdefault(component.name, {}).setdefault(side, []).append(
                    component
                )
            for name, sides in sorted(by_name.items()):
                if len(sides) < 2 or any(len(c) > 1 for c in sides.values()):
                    continue  # one side only, or a same-side collision: _fibers
                shared.setdefault(kind, []).append(
                    (name, sides["left"][0].on("left"), sides["right"][0].on("right"))
                )
        if not shared:
            return {}
        if policy == "prefix_right":
            for kind, groups in shared.items():
                names = [name for name, _l, _r in groups]
                self.note(
                    "prefixed",
                    f"{names} exist on both sides; prefix_right keeps them apart under "
                    "r_ names",
                    subjects=tuple(subject("merged", n) for n in names),
                )
            return {}
        if policy == "error":
            for kind, groups in shared.items():
                names = [name for name, _l, _r in groups]
                payloads = tuple(
                    {
                        "left": _members_field(left),
                        "right": _members_field(right),
                        "into": name,
                    }
                    for name, left, right in groups
                )
                self.finding(
                    "name_collision",
                    f"{kind} name collision: {names} exist on both sides and no "
                    "equivalence merges them. Declare the equivalences the "
                    "completion carries, set name_conflict='union_right' to union "
                    "by name, or name_conflict='prefix_right' to keep them apart.",
                    severity="incomplete",
                    subjects=tuple(subject("merged", n) for n in names),
                    repairs=(
                        Completion(
                            kind="declare_equivalences",
                            vertex_equivalences=payloads if kind == "vertex" else (),
                            relation_equivalences=(
                                payloads if kind == "relation" else ()
                            ),
                        ),
                    ),
                    check=f"{kind} name collision",
                )
            return {}
        out: dict[Kind, list[_Declaration]] = {}
        for kind, groups in shared.items():
            start = len(declarations[kind])
            for offset, (name, left, right) in enumerate(groups):
                declaration: VertexEquivalence | RelationEquivalence = (
                    VertexEquivalence(left=list(left), right=list(right), into=name)
                    if kind == "vertex"
                    else RelationEquivalence(
                        left=list(left), right=list(right), into=name
                    )
                )
                out.setdefault(kind, []).append(
                    _Declaration(
                        position=start + offset,
                        kind=kind,
                        declaration=declaration,
                        members={"left": list(left), "right": list(right)},
                        aliases={"left": {}, "right": {}},
                        synthesized=True,
                    )
                )
                for side, members in (("left", left), ("right", right)):
                    for member in members:
                        self.edges.append(
                            NamingEdge(
                                "union_right", kind, side, member, name, "union_right"
                            )  # type: ignore[arg-type]
                        )
        return out

    def _near_collisions(self, components: Sequence[_Component]) -> None:
        """Two spellings of one concept on the two sides, which no policy unions."""
        if self.op.name_conflict == "prefix_right":
            return
        for kind in _KINDS:
            after: dict[Side, list[str]] = {"left": [], "right": []}
            grouped: set[str] = set()
            for component in components:
                if component.kind != kind or component.name is None:
                    continue
                if component.grouped:
                    grouped.add(component.name)
                    continue
                for side in component.sides():
                    after[side].append(component.name)
            pairs = canonical_near_collisions(
                after["left"], after["right"], exempt=grouped
            )
            if not pairs:
                continue
            listed = ", ".join(f"{left!r} / {right!r}" for left, right in pairs)
            payloads = tuple(
                {"left": left, "right": right, "into": left} for left, right in pairs
            )
            hint = "VertexEquivalence" if kind == "vertex" else "RelationEquivalence"
            self.finding(
                "near_collision",
                f"{kind} near collision: {listed} denote the same concept under different "
                f"naming conventions, so they would merge into two unrelated {kind} "
                f"types with the source data split between them. Declare a {hint} "
                "to combine them, or name_conflict='prefix_right' to keep them apart.",
                subjects=tuple(
                    s
                    for left, right in pairs
                    for s in (subject("left", left), subject("right", right))
                ),
                repairs=(
                    Completion(
                        kind="declare_equivalences",
                        vertex_equivalences=payloads if kind == "vertex" else (),
                        relation_equivalences=payloads if kind == "relation" else (),
                    ),
                ),
                check=f"{kind} near collision",
            )

    # ── one union name, one owner ───────────────────────────────────────────

    def _fibers(self, components: Sequence[_Component]) -> None:
        """Refuse a union name reached by two groups, or by a group and an ungrouped class."""
        for kind in _KINDS:
            by_name: dict[str, list[_Component]] = {}
            for component in components:
                if component.kind == kind and component.name is not None:
                    by_name.setdefault(component.name, []).append(component)
            taken = (
                {c.name for c in components if c.kind == kind and c.name}
                | set(self.names["left"].of_kind(kind))
                | set(self.names["right"].of_kind(kind))
            )
            for name, reaching in sorted(by_name.items()):
                if len(reaching) < 2:
                    continue
                grouped = [c for c in reaching if c.grouped]
                ungrouped = [c for c in reaching if not c.grouped]
                if len(grouped) >= 2:
                    self._shared_into(kind, name, grouped, taken)
                    continue
                if grouped:
                    (group,) = grouped
                    strays = [c for c in ungrouped if c.sides() & group.sides()]
                    if strays:
                        self._occupied(kind, name, group, strays, taken)
                    continue
                for side in _SIDES:
                    on_side = [c for c in ungrouped if side in c.sides()]
                    if len(on_side) > 1:
                        self._same_side(kind, name, side, on_side, taken, set(by_name))

    def _shared_into(
        self,
        kind: Kind,
        name: str,
        groups: Sequence[_Component],
        taken: Collection[str],
    ) -> None:
        described = " and ".join(f"{g.on('left')} ~ {g.on('right')}" for g in groups)
        self.finding(
            "shared_into",
            f"shared into: two {kind} groups, {described}, both arrive at {name!r}; "
            "nothing links them — name one differently, or declare them as one "
            "equivalence",
            subjects=(
                subject("merged", name),
                *(subject(side, n) for g in groups for _k, side, n in g.nodes),
            ),
            repairs=(self._set_into(groups[1], _free_name(name, taken, "2")),),
            check="shared into",
        )

    def _occupied(
        self,
        kind: Kind,
        name: str,
        group: _Component,
        strays: Sequence[_Component],
        taken: Collection[str],
    ) -> None:
        stray_nodes = [node for c in strays for node in c.nodes]
        by_vocabulary = any(self._vocab_target(node) == name for node in stray_nodes)
        described = ", ".join(f"{side}:{n}" for _k, side, n in stray_nodes)
        fiber = (
            subject("merged", name),
            *(subject(side, n) for _k, side, n in group.nodes),
            *(subject(side, n) for _k, side, n in stray_nodes),
        )
        rename_away = [
            Completion(
                kind="rename_away",
                side=side,
                renames={
                    side: {
                        "vertices" if kind == "vertex" else "relations": {
                            n: _free_name(n, taken, side)
                        }
                    }
                },
                label=f"rename {side}:{n} away",
            )
            for c in strays
            if len(c.nodes) == 1
            for _k, side, n in c.nodes
        ]
        set_into = [self._set_into(group, _free_name(name, taken, "merged"))]
        extend = []
        for _k, side, n in stray_nodes:
            payload = self._payload(group.declarations[0])
            payload[side] = [*group.declarations[0].members[side], n]
            payload["into"] = name
            first = group.declarations[0]
            extend.append(
                self._completion(
                    "extend_cluster",
                    kind,
                    payload,
                    side=side,
                    label="fuses entities",
                    replaces=None if first.synthesized else first.position,
                )
            )
        if by_vocabulary:
            self.finding(
                "incomplete",
                f"{kind} joining a merged class: the canonical "
                f"map sends {described} onto {name!r}, the name of the group "
                f"{group.on('left')} ~ {group.on('right')}, without declaring it a "
                f"member. A {kind} joining a merged class is governed by the group's "
                "identity and property maps: declare it a member, or map it elsewhere.",
                severity="incomplete",
                subjects=fiber,
                repairs=(*extend, *set_into),
                check=f"{kind} joining a merged class",
            )
            return
        self.finding(
            "occupied_into",
            f"occupied into: {kind} {name!r} would receive the group "
            f"{group.on('left')} ~ {group.on('right')} and {described}, which nothing "
            f"links — rename {described} away, pick a different `into`, or add it to "
            "the group",
            subjects=fiber,
            repairs=(*rename_away, *set_into, *extend),
            check="occupied into",
        )

    def _same_side(
        self,
        kind: Kind,
        name: str,
        side: Side,
        components: Sequence[_Component],
        taken: Collection[str],
        union_names: Collection[str],
    ) -> None:
        """Refuse ungrouped classes of one side that all arrive at *name*.

        A class that arrived through a rename or the vocabulary can keep its own
        name, unless another class already arrives there; a class already called
        *name* is renamed away.
        """
        nodes = [n for c in components for _k, s, n in c.nodes if s == side]
        field_name = "vertices" if kind == "vertex" else "relations"

        def rename(n: str, to: str, label: str) -> Completion:
            return Completion(
                kind="rename_away",
                side=side,
                renames={side: {field_name: {n: to}}},
                label=label,
            )

        keep = [
            rename(n, n, f"keep {side}:{n} as {n!r}")
            for n in nodes
            if n != name and n not in union_names
        ]
        away = [
            rename(n, _free_name(n, taken, side), f"rename {side}:{n} away")
            for n in nodes
            if n == name
        ]
        self.finding(
            "occupied_into",
            f"occupied into: {side} {kind}s {nodes} would all be called {name!r}, "
            "and nothing merges them; keep or rename one apart, or declare them "
            "one group",
            subjects=(subject("merged", name), *(subject(side, n) for n in nodes)),
            repairs=(*keep, *away),
            check="occupied into",
        )

    def _double_homes(self, components: Sequence[_Component]) -> None:
        """A ``renames`` entry for a class a group already names."""
        owner = {
            node: component
            for component in components
            if component.grouped or len(component.nodes) > 1
            for node in component.nodes
        }
        for kind in _KINDS:
            for side in _SIDES:
                for source, target in self.renames[kind][side].items():
                    component = owner.get((kind, side, source))
                    if component is None:
                        continue
                    by = (
                        "its equivalence's `into`"
                        if component.grouped
                        else "the vocabulary that merges it"
                    )
                    self.finding(
                        "double_home",
                        f"double home: renames.{side} renames {side}:{source} to {target!r}, but it "
                        f"belongs to the group named {component.name!r}, which {by} "
                        "names — a name is declared in one place",
                        subjects=(subject(side, source), subject("merged", target)),
                        check="double home",
                    )
        for side in _SIDES:
            for cls in self.op.renames[side].properties:
                component = owner.get(("vertex", side, cls))
                if component is not None and component.grouped:
                    self.finding(
                        "double_home",
                        f"double home: renames.{side}.properties renames attributes of {side}:{cls}, "
                        f"a member of the group {component.name!r}; align a member's "
                        "attributes with the equivalence's `properties`",
                        subjects=(subject(side, cls),),
                        check="double home",
                    )

    def _property_vocabulary(self, components: Sequence[_Component]) -> None:
        """Every vocabulary attribute map is keyed by a class its side declares."""
        group_names = {c.name for c in components if c.grouped and c.name}
        for side in _SIDES:
            other: Side = "right" if side == "left" else "left"
            cm = self.declared[side]
            known = self.names[side].vertices
            dangling: list[DanglingEntry] = []
            for cls in cm.properties:
                if cls in known:
                    continue
                if cm.canonical_class(cls) in known:
                    continue  # satisfied: the class was already renamed away
                if cls in self.declared.both.properties and (
                    cls in self.names[other].vertices
                    or cm.canonical_class(cls) in self.names[other].vertices
                ):
                    continue
                if cls in group_names:
                    self.finding(
                        "dangling",
                        f"dangling entry: the "
                        f"canonical map's {side} attribute map is keyed by {cls!r}, a "
                        "merged name. `properties` is keyed by the source class: key "
                        "the attribute map by the member it applies to.",
                        subjects=(subject("merged", cls),),
                        check="dangling entry",
                    )
                    continue
                dangling.append(
                    DanglingEntry(
                        side=side,
                        kind="property",
                        source=cls,
                        suggestion=did_you_mean(cls, known),
                    )
                )
            self._report_dangling(side, dangling)

    # ── lowering ────────────────────────────────────────────────────────────

    def _clusters(self, components: Sequence[_Component]) -> list[Cluster[Any]]:
        """The groups as clusters, in the order their equivalences are declared.

        The union lists merged types in this order, ahead of the sides' other
        types.
        """
        out: list[Cluster[Any]] = []
        grouped = sorted(
            (c for c in components if c.grouped and c.name is not None),
            key=lambda c: min(d.position for d in c.declarations),
        )
        for component in grouped:
            ordered = self._ordered_members(component)
            aliases = self._aliases(component, ordered)
            effective = self._effective(component, ordered, aliases)
            if effective is None:
                continue
            declared = {
                side: tuple(
                    dict.fromkeys(
                        m for d in component.declarations for m in d.members[side]
                    )
                )
                for side in _SIDES
            }
            fields: dict[str, Any] = {
                "left": tuple(ordered["left"]),
                "right": tuple(ordered["right"]),
                "into": component.name,
                "aliases": aliases,
                "declared_into": component.into,
                "synthesized": all(d.synthesized for d in component.declarations),
                "declared_left": declared["left"],
                "declared_right": declared["right"],
            }
            if isinstance(effective, VertexEquivalence):
                out.append(Cluster[VertexEquivalence](declaration=effective, **fields))
            else:
                out.append(
                    Cluster[RelationEquivalence](declaration=effective, **fields)
                )
        return out

    def _ordered_members(self, component: _Component) -> dict[Side, list[str]]:
        """Declared members first, in declaration order, then the vocabulary-joined ones."""
        out: dict[Side, list[str]] = {}
        for side in _SIDES:
            declared = list(
                dict.fromkeys(
                    m for d in component.declarations for m in d.members[side]
                )
            )
            joined = sorted(set(component.on(side)) - set(declared))
            out[side] = [*declared, *joined]
        return out

    def _aliases(
        self, component: _Component, ordered: Mapping[Side, Sequence[str]]
    ) -> dict[Side, dict[str, str]]:
        """Every other spelling a member answers to: an unambiguous canonical name."""
        aliases: dict[Side, dict[str, str]] = {}
        for side in _SIDES:
            known = self.names[side].of_kind(component.kind)
            vocab = self.vocab[component.kind][side]
            for declaration in component.declarations:
                for spelled, member in declaration.aliases[side].items():
                    aliases.setdefault(side, {})[spelled] = member
            targets = [vocab.get(m, m) for m in ordered[side]]
            for member, target in zip(ordered[side], targets, strict=True):
                if target == member or target in known or targets.count(target) > 1:
                    continue
                aliases.setdefault(side, {}).setdefault(target, member)
        return aliases

    def _effective(
        self,
        component: _Component,
        ordered: Mapping[Side, Sequence[str]],
        aliases: Mapping[Side, Mapping[str, str]],
    ) -> VertexEquivalence | RelationEquivalence | None:
        """One declaration for the whole group, over its closed member sets."""
        assert component.name is not None
        if component.kind == "relation":
            return RelationEquivalence(
                left=list(ordered["left"]),
                right=list(ordered["right"]),
                into=component.name,
            )
        declarations = [
            d
            for d in component.declarations
            if isinstance(d.declaration, VertexEquivalence)
        ]
        properties = self._group_properties(component, declarations, ordered, aliases)
        keyed = [
            d
            for d in declarations
            if cast(VertexEquivalence, d.declaration).identity is not None
        ]
        if len(keyed) > 1:
            self.finding(
                "identity_disagreement",
                f"identity disagreement: merged vertex {component.name!r} takes its key from one "
                f"equivalence, but #{keyed[0].position} and #{keyed[1].position} "
                "both declare `identity`; keep it on one",
                subjects=(
                    subject("merged", component.name),
                    *(subject(side, n) for _k, side, n in component.nodes),
                ),
                check="identity disagreement",
            )
        source = cast(VertexEquivalence, (keyed or declarations)[0].declaration)
        derive_at: dict[str, list[int]] = {}
        for declaration in declarations:
            for resource, path in cast(
                VertexEquivalence, declaration.declaration
            ).derive_at.items():
                prior = derive_at.get(resource)
                if prior is not None and prior != path:
                    self.finding(
                        "disagreement",
                        f"derive_at disagreement: merged vertex {component.name!r}: derive_at for resource "
                        f"{resource!r} is {prior} in one equivalence and {path} in another",
                        subjects=(subject("merged", component.name),),
                        check="derive_at disagreement",
                    )
                derive_at[resource] = list(path)
        allow = sorted(
            {
                value
                for declaration in declarations
                for value in cast(VertexEquivalence, declaration.declaration).allow
            }
        )
        identity = source.identity
        if identity is not None and any(
            isinstance(b, DerivedBranch | LocalKeyBranch) for b in identity
        ):
            identity = self._with_own_keys(component, identity, ordered, aliases)
        elif identity is not None:
            self._property_key_coverage(component, identity, ordered, properties)
        try:
            return VertexEquivalence(
                left=list(ordered["left"]),
                right=list(ordered["right"]),
                into=component.name,
                properties=properties,
                identity=identity,
                digest_field=source.digest_field,
                derive_at=derive_at if identity is not None else {},
                retire=source.retire,
                allow=allow,  # type: ignore[arg-type]
            )
        except ValueError as exc:
            self.from_refusal(exc, kind="disagreement")
            return None

    def _group_properties(
        self,
        component: _Component,
        declarations: Sequence[_Declaration],
        ordered: Mapping[Side, Sequence[str]],
        aliases: Mapping[Side, Mapping[str, str]],
    ) -> list[PropertyEquivalence]:
        """Each property equivalence keyed per member: a bare name covers its own equivalence's members."""
        out: list[PropertyEquivalence] = []
        for declaration in declarations:
            vertex = cast(VertexEquivalence, declaration.declaration)
            for pe in vertex.properties:
                specs: dict[Side, dict[str, str] | None] = {}
                for side in _SIDES:
                    spec = pe.left if side == "left" else pe.right
                    if spec is None:
                        specs[side] = None
                        continue
                    if isinstance(spec, str):
                        specs[side] = dict.fromkeys(declaration.members[side], spec)
                        continue
                    keyed: dict[str, str] = {}
                    for key, attr in spec.items():
                        member = aliases.get(side, {}).get(key, key)
                        if member not in ordered[side]:
                            self.finding(
                                "unknown_member",
                                f"unknown key member: property equivalence into {pe.into!r} names "
                                f"{side}:{key}, which is not a member of the group "
                                f"{component.name!r} ({list(ordered[side])})",
                                subjects=(subject(side, key),),
                                check="unknown key member",
                            )
                            continue
                        keyed[member] = attr
                    specs[side] = keyed or None
                if specs["left"] is None and specs["right"] is None:
                    continue
                out.append(
                    PropertyEquivalence(
                        left=specs["left"], right=specs["right"], into=pe.into
                    )
                )
        return out

    # ── own keys for members no derivation covers ───────────────────────────

    def _resource_names(self) -> dict[Side, dict[str, str]]:
        """Each side's resources under the names the union gives them."""
        from .merge import _resolve_name_collisions

        out: dict[Side, dict[str, str]] = {}
        listed: dict[Side, list[str]] = {}
        for side in _SIDES:
            ingestion = self.manifests[side].ingestion_model
            names = [r.name for r in ingestion.resources] if ingestion else []
            renames = self.op.renames[side].resources
            out[side] = {name: renames.get(name, name) for name in names}
            listed[side] = [out[side][name] for name in names]
        if self.op.name_conflict == "prefix_right":
            collisions = _resolve_name_collisions(
                set(listed["left"]),
                listed["right"],
                name_conflict="prefix_right",
                kind="resource",
            )
            out["right"] = {
                old: collisions.get(new, new) for old, new in out["right"].items()
            }
        return out

    def _producing(
        self, side: Side, member: str, resource_names: Mapping[Side, Mapping[str, str]]
    ) -> list[str]:
        """The resources of *side* that produce *member*, under their union names."""
        from .merge import _steps_producing

        manifest = self.manifests[side]
        ingestion, schema = manifest.ingestion_model, manifest.graph_schema
        if ingestion is None or schema is None:
            return []
        known = schema.core_schema.vertex_config.vertex_set
        return [
            resource_names[side].get(resource.name, resource.name)
            for resource in ingestion.resources
            if _steps_producing(resource.pipeline, member, known_vertices=known)
        ]

    def _property_key_coverage(
        self,
        component: _Component,
        identity: Sequence[IdentityBranchDecl],
        ordered: Mapping[Side, Sequence[str]],
        properties: Sequence[PropertyEquivalence],
    ) -> None:
        """Refuse a vocabulary-joined member that completes no branch of a property-only key.

        Such a member keeps its own key only under a funnel, so the repair
        carries the ``local_key`` branch to append to ``identity``. A declared
        member is left to the schema union, which names the property to map.
        """
        assert component.name is not None
        branches = [set(branch_fields(branch)) for branch in identity]
        declared = {
            side: {m for d in component.declarations for m in d.members[side]}
            for side in _SIDES
        }
        resource_names = self._resource_names()
        for side in _SIDES:
            schema = self.manifests[side].graph_schema
            if schema is None:
                continue
            vertex_config = schema.core_schema.vertex_config
            for member in ordered[side]:
                if member in declared[side] or member not in vertex_config.vertex_set:
                    continue
                renames = dict(self.declared[side].properties.get(member, {}))
                for pe in properties:
                    spec = pe.left if side == "left" else pe.right
                    if isinstance(spec, dict) and member in spec:
                        renames[spec[member]] = pe.into
                carried = {
                    renames.get(name, name)
                    for name in vertex_config.property_names(member)
                }
                if any(branch <= carried for branch in branches):
                    continue
                own = list(vertex_config[member].identity)
                tag = f"{side}:{member}"
                entry = {
                    "local_key": {
                        resource: {member: {"field": "|".join(own) or "?", "tag": tag}}
                        for resource in self._producing(side, member, resource_names)
                    }
                }
                self.finding(
                    "identity_coverage",
                    f"identity coverage: merged vertex {component.name!r} is keyed "
                    f"on {[sorted(b) for b in branches]}, which {side}:{member} "
                    "cannot complete; the vocabulary joins it to the group, so its "
                    "records would have no key. A member the vocabulary joins keeps "
                    "its own key only when the identity is a funnel: append the "
                    "local_key branch the repair carries to `identity`",
                    subjects=(
                        subject("merged", component.name),
                        subject(side, member),
                    ),
                    repairs=(
                        Completion(
                            kind="add_key_source",
                            side=side,
                            vertex_equivalences=(entry,),
                            label=f"key {side}:{member} on its own key",
                        ),
                    ),
                    check="identity coverage",
                )

    def _with_own_keys(
        self,
        component: _Component,
        identity: Sequence[IdentityBranchDecl],
        ordered: Mapping[Side, Sequence[str]],
        aliases: Mapping[Side, Mapping[str, str]],
    ) -> list[IdentityBranchDecl]:
        """*identity* plus a tagged own key for each member a producing resource leaves unkeyed.

        A member's records from a resource complete the funnel when some
        stepped branch has an entry for the resource that applies to it: one
        unkeyed spec, or one keyed by that member. A resource keying other
        members only would drop its records; a vocabulary-joined member's
        resource with no entry at all would otherwise be turned into a lookup.
        Both get the member's own key behind ``side:Member``. A declared member
        whose resource has no entry is left to merge, which turns that
        resource into a lookup of the merged class.
        """
        assert component.name is not None
        resource_names = self._resource_names()
        stepped = [b for b in identity if isinstance(b, DerivedBranch | LocalKeyBranch)]
        declared = {
            side: {m for d in component.declarations for m in d.members[side]}
            for side in _SIDES
        }
        additions: dict[str, dict[str, LocalKeySource]] = {}
        for side in _SIDES:
            schema = self.manifests[side].graph_schema
            if schema is None:
                continue
            vertex_config = schema.core_schema.vertex_config
            for member in ordered[side]:
                for union_name in self._producing(side, member, resource_names):

                    def applies(
                        entry: Any, side: Side = side, member: str = member
                    ) -> bool:
                        return entry is not None and (
                            not isinstance(entry, dict)
                            or member
                            in {aliases.get(side, {}).get(k, k) for k in entry}
                        )

                    entries = [b.sources.get(union_name) for b in stepped]
                    local = next(
                        (
                            b.sources.get(union_name)
                            for b in stepped
                            if isinstance(b, LocalKeyBranch)
                        ),
                        None,
                    )
                    # The local key is the fallback every record completes; a
                    # member-keyed one that skips this member drops its records.
                    if any(applies(entry) for entry in entries) and not (
                        isinstance(local, dict) and not applies(local)
                    ):
                        continue
                    if (
                        all(entry is None for entry in entries)
                        and member in declared[side]
                    ):
                        continue
                    vertex = vertex_config[member]
                    plain = not (
                        vertex.blank
                        or vertex.assigned
                        or vertex.hash_identity_properties
                        or vertex.identity_funnel is not None
                    )
                    tag = f"{side}:{member}"
                    if plain and len(vertex.identity) == 1:
                        additions.setdefault(union_name, {})[member] = LocalKeySource(
                            field=vertex.identity[0], tag=tag
                        )
                        self.note(
                            "auto_local_key",
                            f"{side}:{member} joins {component.name!r} without a key "
                            f"source for resource {union_name!r}; its records key on "
                            f"their own {vertex.identity[0]!r} as '{tag}:<value>' "
                            "and never fuse with another member's",
                            subjects=(
                                subject(side, member),
                                subject("merged", component.name),
                            ),
                        )
                        continue
                    entry = {
                        "local_key": {
                            union_name: {
                                member: {
                                    "field": "|".join(vertex.identity) or "?",
                                    "tag": tag,
                                }
                            }
                        }
                    }
                    self.finding(
                        "identity_coverage",
                        f"identity coverage: merged vertex {component.name!r} is keyed by a derived "
                        f"identity, but resource {union_name!r} derives nothing for "
                        f"{side}:{member}, whose own key {list(vertex.identity)} is "
                        "not one field, so it cannot be keyed on it automatically; add "
                        "a key source for it (the repair carries a local_key entry to "
                        "fill in)",
                        subjects=(
                            subject("merged", component.name),
                            subject(side, member),
                        ),
                        repairs=(
                            Completion(
                                kind="add_key_source",
                                side=side,
                                vertex_equivalences=(entry,),
                                label=f"key {side}:{member} from {union_name!r}",
                            ),
                        ),
                        check="identity coverage",
                    )
        if not additions:
            return list(identity)
        out = list(identity)
        position = next(
            (i for i, b in enumerate(out) if isinstance(b, LocalKeyBranch)), None
        )
        if position is None:
            out.append(LocalKeyBranch(local_key=dict(additions)))  # type: ignore[arg-type]
            return out
        branch = out[position]
        assert isinstance(branch, LocalKeyBranch)
        local_key: dict[str, LocalKeySource | dict[str, LocalKeySource]] = dict(
            branch.local_key
        )
        for resource, members in additions.items():
            existing = local_key.get(resource)
            local_key[resource] = (
                {**existing, **members} if isinstance(existing, dict) else dict(members)
            )
        out[position] = branch.model_copy(update={"local_key": local_key})
        return out

    def _side_maps(
        self, components: Sequence[_Component], index: ClusterIndex
    ) -> SideMaps:
        """One relabel per side: every node onto its union name, simultaneously."""
        maps: dict[Side, CanonicalizeOp] = {}
        for side in _SIDES:
            mappings: dict[Kind, dict[str, str]] = {"vertex": {}, "relation": {}}
            for component in components:
                if component.name is None:
                    continue
                multi = component.grouped or len(component.nodes) > 1
                for kind, s, name in component.nodes:
                    if s != side or (not multi and name == component.name):
                        continue
                    mappings[kind][name] = component.name
            # Whatever collapses here was declared as a group or is already a
            # finding; the relabel only has to express it.
            merges = any(
                len(sources) > len(set(sources))
                for mapping in mappings.values()
                for sources in [list(mapping.values())]
            )
            properties = self._side_properties(side, index)
            try:
                # Each group's self-relations and fused observations were
                # judged against its own acknowledgement in _consequences.
                maps[side] = CanonicalizeOp(
                    vertices=mappings["vertex"],
                    relations=mappings["relation"],
                    properties=properties,
                    allow_merges=merges,
                    allow_self_relations=True,
                    allow_observation_fusion=True,
                )
            except ValueError as exc:
                self.from_refusal(exc, kind="disagreement")
                maps[side] = CanonicalizeOp(allow_merges=True)
        return SideMaps(left=maps["left"], right=maps["right"])

    def _consequences(self, components: Sequence[_Component]) -> None:
        """Refuse a self-relation or fused observation a group makes unacknowledged.

        Judged per group and per side, on that side's manifest before the
        relabel. A group accepts what one of its equivalences lists in
        ``allow``; a side's vocabulary accepts, through its own flags, what a
        merge it makes causes.
        """
        from .apply import merge_fused_slots, merge_self_relations

        for component in components:
            if component.kind != "vertex" or component.name is None:
                continue
            accepted = {
                value
                for d in component.declarations
                if isinstance(d.declaration, VertexEquivalence)
                for value in d.declaration.allow
            }
            for side in _SIDES:
                members = component.on(side)
                if len(members) < 2:
                    continue
                manifest = self.manifests[side]
                schema = manifest.graph_schema
                if schema is None:
                    continue
                vocabulary = self.declared[side]
                targets = [self._vocab_target(("vertex", side, m)) for m in members]
                by_vocabulary = any(
                    t is not None and targets.count(t) > 1 for t in targets
                )
                mapping = dict.fromkeys(members, component.name)
                resources = (
                    manifest.ingestion_model.resources
                    if manifest.ingestion_model is not None
                    else []
                )
                checks: tuple[
                    tuple[
                        Literal["self_relations", "observation_fusion"], list[str], bool
                    ],
                    ...,
                ] = (
                    (
                        "self_relations",
                        merge_self_relations(
                            schema.core_schema.edge_config.edges, mapping
                        ),
                        vocabulary.allow_self_relations,
                    ),
                    (
                        "observation_fusion",
                        merge_fused_slots(
                            resources, merged=component.name, mapping=mapping
                        ),
                        vocabulary.allow_observation_fusion,
                    ),
                )
                for value, found, flag in checks:
                    if not found or value in accepted or (by_vocabulary and flag):
                        continue
                    self._unacknowledged(component, side, members, value, found)

    def _unacknowledged(
        self,
        component: _Component,
        side: Side,
        members: Sequence[str],
        value: Literal["self_relations", "observation_fusion"],
        found: Sequence[str],
    ) -> None:
        assert component.name is not None
        name = component.name
        if value == "self_relations":
            kind, check = "self_relation", "self-relation"
            detail = (
                f"turns edges into self-relations: {list(found)}. Both endpoints "
                "then land on one vertex"
            )
            flag = "allow_self_relations"
        else:
            kind, check = "observation_fusion", "observation fusion"
            detail = (
                f"leaves pipeline slots producing {name!r} more than once: "
                f"{list(found)}. One document that yields both members then fuses "
                "them into a single node; give each step its own `role`, or accept it"
            )
            flag = "allow_observation_fusion"
        repairs: tuple[Completion, ...] = ()
        if component.grouped:
            first = component.declarations[0]
            payload = self._payload(first)
            payload["allow"] = sorted({*payload.get("allow", []), value})
            repairs = (
                self._completion(
                    "acknowledge",
                    "vertex",
                    payload,
                    side=side,
                    label=f"accept {value.replace('_', ' ')}",
                    replaces=None if first.synthesized else first.position,
                ),
            )
            how = f"add `allow: [{value}]` to the equivalence"
        else:
            how = f"set `{flag}: true` on the {side} canonical map"
        self.finding(
            kind,
            f"{check}: {side} {list(members)} become {name!r}, which {detail}. "
            f"To accept it, {how}.",
            subjects=(
                subject("merged", name),
                *(subject(side, m) for m in members),
            ),
            repairs=repairs,
            check=check,
        )

    def _side_properties(
        self, side: Side, index: ClusterIndex
    ) -> dict[str, dict[str, str]]:
        """Attribute renames keyed by source class: the groups', the vocabulary's, then ``renames``."""
        properties: dict[str, dict[str, str]] = {}
        for cluster in index.vertices:
            for member, attr_map in cluster.property_maps(side).items():
                properties.setdefault(member, {}).update(attr_map)
        cm = self.declared[side]
        for cls, attrs in cm.properties.items():
            if cls not in self.names[side].vertices:
                continue
            bucket = properties.setdefault(cls, {})
            for old, new in attrs.items():
                existing = bucket.get(old)
                if existing is not None and existing != new:
                    self.finding(
                        "property_disagreement",
                        f"property disagreement: "
                        f"the canonical map says {side}:{cls}.{old} -> {new!r}, but the "
                        f"equivalence maps it to {existing!r}. The two declarations "
                        "must agree on where an attribute goes; fix one of them.",
                        subjects=(subject(side, cls, old),),
                        check="property disagreement",
                    )
                    continue
                bucket[old] = new
        members = {m for cluster in index.vertices for m in cluster.members(side)}
        for cls, attrs in self.op.renames[side].properties.items():
            if cls not in self.names[side].vertices or cls in members:
                continue
            bucket = properties.setdefault(cls, {})
            for old, new in attrs.items():
                existing = bucket.get(old)
                if existing is not None and existing != new:
                    self.note(
                        "vocabulary_override",
                        f"renames.{side}.properties calls {side}:{cls}.{old} {new!r}; "
                        f"the vocabulary called it {existing!r}",
                        subjects=(subject(side, cls, old),),
                    )
                bucket[old] = new
        return {cls: attrs for cls, attrs in properties.items() if attrs}

    def _property_checks(self, index: ClusterIndex, side_maps: SideMaps) -> None:
        for cluster in index.vertices:
            for side in _SIDES:
                try:
                    check_property_fields_exist(
                        self.manifests[side],
                        cluster,
                        side=side,
                        declared=self.declared[side],
                    )
                except MergeCanonicalConflictError as exc:
                    self.from_refusal(exc, kind="unknown_property")
                try:
                    check_attribute_fixed_points(
                        cluster, side=side, declared=self.declared[side]
                    )
                except MergeCanonicalConflictError as exc:
                    self.from_refusal(exc, kind="property_retarget")
        for side in _SIDES:
            for member, attr_map in side_maps[side].properties.items():
                one = CanonicalizeOp(properties={member: attr_map}, allow_merges=True)
                try:
                    check_property_maps_against_manifest(
                        self.manifests[side], one, side=side
                    )
                except MergeCanonicalConflictError as exc:
                    self.from_refusal(
                        exc, kind=_PROPERTY_MAP_KINDS.get(exc.check, "unknown_property")
                    )

    # ── the graph ───────────────────────────────────────────────────────────

    def _graph(self, components: Sequence[_Component]) -> NamingGraph:
        targets: dict[NodeKey, str | None] = {}
        via: dict[NodeKey, Via] = {}
        groups: list[NamingGroup] = []
        for component in components:
            declared = {node for d in component.declarations for node in d.nodes}
            synthesized = bool(component.declarations) and all(
                d.synthesized for d in component.declarations
            )
            members: list[tuple[Side, str, Via]] = []
            for node in component.nodes:
                kind, side, name = node
                targets[node] = component.name
                how: Via
                if node in declared:
                    how = "union_right" if synthesized else "equivalence"
                elif component.grouped or len(component.nodes) > 1:
                    how = "vocabulary"
                elif self.renames[kind][side].get(name) is not None:
                    how = "rename"
                elif self._vocab_target(node) not in (None, name):
                    how = "vocabulary"
                else:
                    how = "own name"
                via[node] = how
                members.append((side, name, how))
                if node in declared:
                    self.edges.append(
                        NamingEdge(
                            "member",
                            kind,
                            side,
                            name,
                            component.name,
                            next(
                                d.label
                                for d in component.declarations
                                if node in d.nodes
                            ),
                        )
                    )
            if component.grouped or len(component.nodes) > 1:
                groups.append(
                    NamingGroup(
                        kind=component.kind,
                        name=component.name,
                        members=tuple(members),
                        equivalences=tuple(d.position for d in component.declarations),
                        synthesized=synthesized,
                    )
                )
        return NamingGraph(
            targets=targets, via=via, groups=tuple(groups), edges=tuple(self.edges)
        )


def build_naming(
    op: MergeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> NamingResult:
    """Resolve *op* against *left* and *right* in one pass, refusing nothing.

    Args:
        op: The merge op: equivalences, vocabulary, renames, policy.
        left: The left manifest, as handed to merge.
        right: The right manifest.
        canonical_maps: Extra ``(side, map)`` vocabulary pairs, folded into
            ``op.canonical_maps``.

    Returns:
        The resolution merge would apply (groups as clusters over their closed
        member sets, one relabel per side), the naming graph, and every
        finding. Lowered best-effort when a finding blocks it.
    """
    return _Naming(
        op=op, manifests={"left": left, "right": right}, extra=canonical_maps
    ).run()


def resolve_naming(
    op: MergeManifestsOp,
    *,
    left: GraphManifest,
    right: GraphManifest,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> ClusterResolution:
    """:func:`build_naming`, raising every blocking finding as one :class:`MergeNamingError`."""
    result = build_naming(op, left=left, right=right, canonical_maps=canonical_maps)
    result.raise_if_blocking()
    return result.resolution


def naming_table(
    graph: NamingGraph,
    kind: Kind = "vertex",
    *,
    findings: Sequence[NamingFinding] = (),
    quiet: bool = False,
) -> list[str]:
    """The naming graph as text: one row per union name and provenance.

    A row whose union name a blocking finding is about ends in
    ``<- conflict``. With *quiet*, a table where every class keeps its own
    name and nothing conflicts is left out.
    """
    rows = graph.rows(kind)
    marked = {
        s.removeprefix("merged:")
        for f in findings
        if f.blocking
        for s in f.subjects
        if s.startswith("merged:")
    }
    if not rows or (
        quiet
        and all(via == "own name" for *_r, via in rows)
        and not {union for union, *_r in rows} & marked
    ):
        return []
    header = ("merged", "left", "right", "via")
    cells = [
        (union, ", ".join(left) or "-", ", ".join(right) or "-", via)
        for union, left, right, via in rows
    ]
    widths = [max(len(row[i]) for row in (header, *cells)) for i in range(4)]
    lines = []
    for position, row in enumerate((header, *cells)):
        line = "  ".join(
            cell.ljust(width) for cell, width in zip(row, widths, strict=True)
        ).rstrip()
        if position and row[0] in marked:
            line = f"{line.ljust(sum(widths) + 6)}  <- conflict"
        lines.append(line)
    return lines


def apply_repair(op: MergeManifestsOp, repair: Completion) -> MergeManifestsOp | None:
    """*op* with *repair* applied, or ``None`` when an edit of the op cannot express it.

    ``add_key_source`` carries an identity entry to fill in by hand, and a
    repair of a synthesized group has no declaration to replace.
    """
    payload = op.to_dict(skip_defaults=True)
    if repair.kind == "declare_equivalences":
        for key, added in (
            ("vertex_equivalences", repair.vertex_equivalences),
            ("relation_equivalences", repair.relation_equivalences),
        ):
            if added:
                payload[key] = [*payload.get(key, []), *(dict(p) for p in added)]
    elif repair.kind in ("set_into", "extend_cluster"):
        if repair.replaces is None:
            return None
        key = (
            "vertex_equivalences"
            if repair.vertex_equivalences
            else "relation_equivalences"
        )
        (replacement,) = repair.vertex_equivalences or repair.relation_equivalences
        declared = list(payload.get(key, []))
        if repair.replaces >= len(declared):
            return None
        declared[repair.replaces] = dict(replacement)
        payload[key] = declared
    elif repair.kind == "rename_away":
        renames = payload.setdefault("renames", {})
        for side, fragment in repair.renames.items():
            side_renames = renames.setdefault(side, {})
            for field_name, mapping in fragment.items():
                side_renames[field_name] = {
                    **side_renames.get(field_name, {}),
                    **mapping,
                }
    else:
        return None
    return MergeManifestsOp.model_validate(payload)


def _with_explicit_keys(op: MergeManifestsOp, result: NamingResult) -> MergeManifestsOp:
    """*op* with each automatic own key written into the equivalence that carries the identity."""
    named = {
        subject_id
        for finding in result.findings
        if finding.kind == "auto_local_key"
        for subject_id in finding.subjects
        if subject_id.startswith("merged:")
    }
    if not named:
        return op
    equivalences = list(op.vertex_equivalences)
    for cluster in result.resolution.index.vertices:
        if subject("merged", cluster.into) not in named:
            continue
        effective = cluster.declaration
        members = {*cluster.left, *cluster.right}
        for position, equivalence in enumerate(equivalences):
            spelled = {*equivalence.left_members, *equivalence.right_members}
            resolved = {
                cluster.resolved(side, name)
                for side in _SIDES
                for name in equivalence.members(side)
            }
            if equivalence.identity is not None and (spelled | resolved) & members:
                equivalences[position] = equivalence.model_copy(
                    update={"identity": effective.identity}
                )
                break
    return op.model_copy(update={"vertex_equivalences": equivalences})


def suggest_merge_op(
    left: GraphManifest,
    right: GraphManifest,
    op: MergeManifestsOp | None = None,
    *,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
    max_steps: int = 32,
) -> MergeManifestsOp:
    """A merge op that settles what can be settled without guessing, for review.

    Without *op*, a scaffold: an equivalence for every name both sides share.
    With one, *op* with the first repair of each blocking finding applied —
    a rename away before a new ``into``, and either before fusing entities —
    until no finding has one left, and each automatic own key written out
    where the identity is declared. Two spellings of one concept are never
    unioned: that is a guess, left to the author. Nothing is applied to a
    manifest; the result is a value to read, edit and save.
    """
    current = op if op is not None else MergeManifestsOp()
    tried: set[str] = set()
    for _ in range(max_steps):
        result = build_naming(
            current, left=left, right=right, canonical_maps=canonical_maps
        )
        progressed = False
        for finding in result.blocking:
            if finding.kind == "near_collision":
                continue
            for repair in finding.repairs:
                key = repr(repair.to_dict())
                if key in tried:
                    continue
                tried.add(key)
                repaired = apply_repair(current, repair)
                if repaired is None:
                    continue
                current, progressed = repaired, True
                break
            if progressed:
                break
        if not progressed:
            break
    result = build_naming(
        current, left=left, right=right, canonical_maps=canonical_maps
    )
    return _with_explicit_keys(current, result)


__all__ = [
    "MergeNamingError",
    "MergeNamingIncompleteError",
    "NamingEdge",
    "NamingFinding",
    "NamingGraph",
    "NamingGroup",
    "NamingResult",
    "apply_repair",
    "build_naming",
    "naming_table",
    "resolve_naming",
    "suggest_merge_op",
]
