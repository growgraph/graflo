"""What a compose would do, and every way it could refuse — without refusing.

:func:`~graflo.architecture.evolution.compose.compose_manifests` raises at the
first refusal. That is right for a function that returns a manifest — a
half-composed model is worse than none — but it makes authoring a compose a
game of whack-a-mole: fix the contradiction the message names, run again, learn
about the next one. Three declarations that each refuse take three runs to
discover.

This module is the other view. :func:`preview_compose` walks the same
declarations and reports **every** problem it finds, as data:

* the **declaration graph** — classes and their attributes on each side, the
  clusters that collapse them, the canonical names the maps establish, and the
  edges between them. This is the ``class_A - attr_a - attr_b - class_B``
  picture the authoring model is actually about;
* **findings**, each naming the nodes it is about, at one of three severities —
  ``possible`` (found structurally, by this module), ``refusal`` (what compose
  actually raised, if it was asked to try) and ``note`` (an acknowledged
  heuristic, such as an entry taken as already applied);
* an **outcome**, from a real compose attempt.

Nothing here is a second implementation of the resolution rules. Every check
calls the function in :mod:`~graflo.architecture.evolution.canonical` that
compose itself calls, in units small enough — one declaration, one map entry,
one member — that a refusal on one unit does not hide the others. A refusal
carries its ``check`` and its ``subjects``, so the finding it becomes is
pinned to the same nodes the message names, and there is no parallel copy of
the rules to fall out of date.

The consistency invariant, asserted in the tests: **whatever compose refuses,
the structural pass has a finding of a matching kind for**, and a compose that
succeeds leaves no ``refusal`` finding behind.

Merging two branches of one lineage needs none of this.
:func:`~graflo.architecture.evolution.merge3.merge_three_way` already *returns*
its conflicts rather than raising them, one record per contested slot, so
:func:`build_merge_preview` only has to put them in the shape they already
have: slots form a tree — ``vertex/person`` contains ``vertex/person/field/age``
— and the tree is what shows *where in the model* two branches collided.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Vertex

from .apply import relabel_vertex_fields
from .canonical import (
    ClusterResolution as _ClusterResolution,
)
from .canonical import (
    ComposeCanonicalConflictError,
    ComposeIncompleteError,
    SideNames,
    _carry_declared_entries,
    _check_attribute_fixed_points,
    _check_property_fields_exist,
    _check_property_maps_against_manifest,
    _cluster_maps,
    _moving,
    _preimages,
    _resolve_cluster,
    _same_name_payloads,
    canonical_near_collisions,
    fold_declared_maps,
    same_name_groups,
)
from .canonical import (
    SideMaps as _SideMaps,
)
from .compose import _resolve_schema_collisions
from .equivalence import (
    Cluster,
    ClusterConflictError,
    ClusterIndex,
    ClusterSpec,
    Kind,
    Side,
    UnknownMemberError,
    did_you_mean,
    subject,
)
from .merge_core import (
    merge_edge_pair,
    merge_vertex_models,
)
from .ops import (
    CanonicalizeOp,
    CanonicalMap,
    ComposeManifestsOp,
    RelationEquivalence,
    VertexEquivalence,
)

logger = logging.getLogger(__name__)

_SIDES: tuple[Side, ...] = ("left", "right")

#: What a preview node stands for. ``ghost`` is a name a declaration mentions
#: that the manifest on that side does not declare -- drawn, because the
#: absence is the point.
NodeKind = Literal["class", "relation", "attribute", "composed", "canonical", "ghost"]

#: What one declaration says about a pair of names. ``suggested`` comes from a
#: refusal's completion: the declaration that would settle it.
EdgeKind = Literal["member", "map", "property_map", "property_equivalence", "suggested"]

#: ``refusal`` is what compose raised; ``possible`` what this module found on
#: its own; ``note`` an acknowledged heuristic that changes nothing.
Severity = Literal["refusal", "possible", "note"]

FindingKind = Literal[
    "cluster_overlap",
    "shared_into",
    "occupied_into",
    "unknown_member",
    "disagreement",
    "ambiguity",
    "unnamed_cluster",
    "incomplete",
    "dangling",
    "satisfied",
    "name_collision",
    "near_collision",
    "prefixed",
    "property_disagreement",
    "property_collision",
    "property_retarget",
    "unknown_property",
    "identity_disagreement",
    "type_conflict",
    "unit_conflict",
    "identity_mode_conflict",
    "identity_funnel_conflict",
    "secondary_identity_conflict",
    "edge_conflict",
]

#: A refusal's ``check`` phrase to the finding kind it is an instance of. The
#: keys are matched as substrings of ``check``, longest first, so the
#: ``vertex`` / ``relation`` prefix the messages carry needs no entry of its
#: own.
_KIND_BY_CHECK: dict[str, FindingKind] = {
    "cluster overlap": "cluster_overlap",
    "shared into": "shared_into",
    "occupied into": "occupied_into",
    "unknown vertex member": "unknown_member",
    "unknown relation member": "unknown_member",
    "joining a composed class": "incomplete",
    "name collision": "name_collision",
    "near collision": "near_collision",
    "canonical split": "name_collision",
    "property disagreement": "property_disagreement",
    "property rename collision": "property_collision",
    "property re-target": "property_retarget",
    "unknown property": "unknown_property",
    "canonical property clash": "property_disagreement",
    "ambiguous": "ambiguity",
    "unnamed": "unnamed_cluster",
    "dangling entry": "dangling",
    "disagreement": "disagreement",
    "re-target": "disagreement",
    "clash": "disagreement",
    "identity disagreement": "identity_disagreement",
    "cluster conflict": "cluster_overlap",
    # What the schema union refuses. ``edge type disagreement`` has to outrank
    # the bare ``disagreement`` above, which longest-match already guarantees;
    # a bare ``conflict`` key would swallow all five and must never be added.
    "field type conflict": "type_conflict",
    "field units conflict": "unit_conflict",
    "identity mode conflict": "identity_mode_conflict",
    "identity funnel conflict": "identity_funnel_conflict",
    "secondary identity conflict": "secondary_identity_conflict",
    "edge type disagreement": "edge_conflict",
}

#: Refusals that carry no ``check`` are classified by type alone. A value of
#: ``None`` means "no expectation" -- the preview is not asked to have seen it.
_KINDS_BY_TYPE: dict[str, frozenset[FindingKind]] = {
    "ClusterConflictError": frozenset(
        {"cluster_overlap", "shared_into", "occupied_into"}
    ),
    "UnknownMemberError": frozenset({"unknown_member"}),
    "ComposeIncompleteError": frozenset({"incomplete", "name_collision"}),
    "ComposeIdentityError": frozenset({"identity_disagreement"}),
    "ComposeNameConflictError": frozenset({"near_collision", "name_collision"}),
    # Inert while the union's refusals set ``check`` -- which they all do. Kept
    # so a raise site added later without one still classifies rather than
    # silently becoming exempt.
    "FieldMergeError": frozenset({"type_conflict", "unit_conflict"}),
    "VertexMergeError": frozenset(
        {
            "identity_mode_conflict",
            "identity_funnel_conflict",
            "secondary_identity_conflict",
        }
    ),
    "EdgeMergeError": frozenset({"edge_conflict"}),
}


# ── the model ───────────────────────────────────────────────────────────────


class PreviewNode(ConfigBaseModel):
    """One class, relation or attribute in the declaration graph."""

    id: str = PydanticField(
        ..., description="Stable id, as `subject()` builds it: `left:Firm.firm_id`."
    )
    kind: NodeKind = PydanticField(..., description="What this node stands for.")
    name: str = PydanticField(..., description="The name as its side spells it.")
    side: Side | None = PydanticField(
        default=None,
        description="Which manifest it comes from; composed names have none.",
    )
    owner: str | None = PydanticField(
        default=None, description="For an attribute, the node id of its class."
    )
    identity: bool = PydanticField(
        default=False,
        description="Whether this attribute takes part in its class's identity.",
    )
    exists: bool = PydanticField(
        default=True,
        description="False when a declaration names it but the manifest does not.",
    )
    field_type: str | None = PydanticField(
        default=None, description="Declared type of an attribute, when it has one."
    )
    identity_mode: str | None = PydanticField(
        default=None, description="natural / hash / blank / assigned, for a class."
    )


class PreviewEdge(ConfigBaseModel):
    """One thing a declaration says about a pair of names."""

    id: str = PydanticField(..., description="Stable id: `kind:source->target`.")
    source: str = PydanticField(..., description="Node id this edge leaves.")
    target: str = PydanticField(..., description="Node id this edge enters.")
    kind: EdgeKind = PydanticField(..., description="Which declaration said it.")
    label: str = PydanticField(default="", description="Short caption, when useful.")
    declared_by: str = PydanticField(
        default="",
        description="Where it was declared: a map scope, or a cluster's composed name.",
    )


class PreviewCluster(ConfigBaseModel):
    """One equivalence declaration, resolved as far as it could be."""

    id: str = PydanticField(..., description="Stable id of the cluster.")
    kind: Kind = PydanticField(
        ..., description="Whether it collapses classes or relations."
    )
    into: str | None = PydanticField(
        default=None, description="Composed name; None when it could not be resolved."
    )
    declared_into: str | None = PydanticField(
        default=None, description="`into` as the author spelled it, before translation."
    )
    left: list[str] = PydanticField(default_factory=list, description="Left members.")
    right: list[str] = PydanticField(default_factory=list, description="Right members.")
    synthesized: bool = PydanticField(
        default=False, description="Declared by compose itself under `union_right`."
    )
    declared_identity: bool = PydanticField(
        default=False, description="Whether the declaration states an `identity`."
    )
    aligned: bool = PydanticField(
        default=False, description="Whether an `identity_alignments` entry names it."
    )


class ComposeFinding(ConfigBaseModel):
    """One thing wrong with the declarations, or one acknowledged heuristic."""

    kind: FindingKind = PydanticField(
        ..., description="Which rule it is an instance of."
    )
    severity: Severity = PydanticField(..., description="How much it matters.")
    message: str = PydanticField(..., description="What to tell the author.")
    source: Literal["compose", "structure"] = PydanticField(
        ..., description="Whether compose raised it, or this module found it."
    )
    nodes: list[str] = PydanticField(
        default_factory=list, description="Node ids this finding is about."
    )
    edges: list[str] = PydanticField(
        default_factory=list, description="Edge ids this finding is about."
    )
    completion: dict[str, Any] | None = PydanticField(
        default=None,
        description="The declaration that would settle it, when there is one.",
    )
    check: str | None = PydanticField(
        default=None, description="The refusal's own name for the rule."
    )
    error_type: str | None = PydanticField(
        default=None, description="Exception type, for a finding compose raised."
    )


class ComposeOutcome(ConfigBaseModel):
    """What a real compose attempt produced, or refused with."""

    status: Literal["composed", "refused", "not_attempted"] = PydanticField(
        ..., description="Whether compose ran, and how it ended."
    )
    error_type: str | None = PydanticField(default=None, description="Exception type.")
    message: str | None = PydanticField(
        default=None, description="The refusal, in full."
    )
    check: str | None = PydanticField(
        default=None, description="The rule that refused."
    )
    completion: dict[str, Any] | None = PydanticField(
        default=None,
        description="The extension that would settle an incomplete refusal.",
    )
    vertices: int | None = PydanticField(
        default=None, description="Vertex count of the composed schema."
    )
    edges: int | None = PydanticField(
        default=None, description="Edge count of the composed schema."
    )
    version: str | None = PydanticField(
        default=None, description="Version of the composed schema."
    )


class ComposePreview(ConfigBaseModel):
    """The declaration graph, everything wrong with it, and what compose did."""

    left_name: str = PydanticField(
        default="left", description="Name of the left manifest."
    )
    right_name: str = PydanticField(
        default="right", description="Name of the right manifest."
    )
    name_conflict: str = PydanticField(
        default="error", description="The op's policy for names no equivalence covers."
    )
    nodes: list[PreviewNode] = PydanticField(default_factory=list)
    edges: list[PreviewEdge] = PydanticField(default_factory=list)
    clusters: list[PreviewCluster] = PydanticField(default_factory=list)
    findings: list[ComposeFinding] = PydanticField(default_factory=list)
    outcome: ComposeOutcome = PydanticField(
        default_factory=lambda: ComposeOutcome(status="not_attempted")
    )

    @property
    def refused(self) -> bool:
        """Whether the compose attempt refused."""
        return self.outcome.status == "refused"

    @property
    def blocking(self) -> list[ComposeFinding]:
        """Findings that would stop a compose: the refusal and every possible one."""
        return [f for f in self.findings if f.severity != "note"]

    def node(self, node_id: str) -> PreviewNode | None:
        """The node with *node_id*, or ``None``."""
        return next((n for n in self.nodes if n.id == node_id), None)

    def attributes_of(self, node_id: str) -> list[PreviewNode]:
        """Attribute nodes owned by the class node *node_id*, in declared order."""
        return [n for n in self.nodes if n.owner == node_id]

    def with_outcome(
        self, outcome: ComposeOutcome, *, subjects: Sequence[str] = ()
    ) -> ComposePreview:
        """A copy carrying *outcome*, and the finding a refusal becomes.

        The structural pass usually found the refusal too — it calls the same
        check — so the two are folded into one finding marked ``refusal``
        rather than listed twice. Reporting one problem as two would undercut
        the only number this is for: how many things are actually wrong.
        """
        if outcome.status != "refused":
            return self.model_copy(update={"outcome": outcome})

        known = {n.id for n in self.nodes}
        refusal = ComposeFinding(
            kind=kind_for_check(outcome.check, outcome.error_type),
            severity="refusal",
            message=outcome.message or "compose refused",
            source="compose",
            nodes=[s for s in subjects if s in known],
            completion=outcome.completion,
            check=outcome.check,
            error_type=outcome.error_type,
        )
        findings: list[ComposeFinding] = []
        folded = False
        for finding in self.findings:
            if not folded and _is_same_problem(finding, refusal):
                findings.append(
                    refusal.model_copy(
                        update={
                            "nodes": list(
                                dict.fromkeys([*refusal.nodes, *finding.nodes])
                            ),
                            "edges": finding.edges,
                        }
                    )
                )
                folded = True
                continue
            findings.append(finding)
        if not folded:
            findings.append(refusal)
        return self.model_copy(update={"outcome": outcome, "findings": findings})


def _is_same_problem(structural: ComposeFinding, refusal: ComposeFinding) -> bool:
    """Whether a structural finding and the refusal describe one problem.

    Same kind, and either the same message — the usual case, since both come
    from the same check — or the same subjects.
    """
    if structural.severity != "possible" or structural.kind != refusal.kind:
        return False
    if structural.message == refusal.message:
        return True
    if not structural.nodes or not refusal.nodes:
        return False
    # The refusal usually names more -- it knows the composed names the
    # structural pass only reached one of -- so a subset is the same problem
    # seen with less context.
    return set(structural.nodes) <= set(refusal.nodes)


# ── classifying a refusal ───────────────────────────────────────────────────


def kind_for_check(check: str | None, error_type: str | None = None) -> FindingKind:
    """The finding kind a refusal's ``check`` phrase is an instance of.

    Longest match wins, so ``"property rename collision"`` is not read as the
    plain ``"collision"`` of a name clash. Falls back to the exception type,
    and finally to ``disagreement`` -- the most general of the four classes.
    """
    if check:
        for phrase in sorted(_KIND_BY_CHECK, key=len, reverse=True):
            if phrase in check:
                return _KIND_BY_CHECK[phrase]
    if error_type:
        kinds = _KINDS_BY_TYPE.get(error_type)
        if kinds:
            return min(kinds)
    return "disagreement"


def expected_kinds(outcome: ComposeOutcome) -> frozenset[FindingKind]:
    """Structural finding kinds that should accompany *outcome*.

    The invariant this module is tested against: whatever compose refused, the
    structural pass saw something of a matching kind. An empty set means the
    refusal is one the preview is not asked to anticipate -- a structural merge
    error from deep inside the union, say -- and asserts nothing.
    """
    if outcome.status != "refused":
        return frozenset()
    if outcome.check:
        return frozenset({kind_for_check(outcome.check, outcome.error_type)})
    by_type = _KINDS_BY_TYPE.get(outcome.error_type or "")
    return by_type if by_type is not None else frozenset()


def outcome_from_exception(
    exc: BaseException,
) -> tuple[ComposeOutcome, tuple[str, ...]]:
    """The outcome a refusal is, and the node ids it names."""
    completion = getattr(exc, "completion", None)
    return (
        ComposeOutcome(
            status="refused",
            error_type=type(exc).__name__,
            message=str(exc),
            check=getattr(exc, "check", "") or None,
            completion=completion.to_dict() if completion is not None else None,
        ),
        tuple(getattr(exc, "subjects", ()) or ()),
    )


def outcome_from_manifest(manifest: GraphManifest) -> ComposeOutcome:
    """The outcome a composed manifest is."""
    schema = manifest.graph_schema
    if schema is None:
        return ComposeOutcome(status="composed")
    core = schema.core_schema
    return ComposeOutcome(
        status="composed",
        vertices=len(core.vertex_config.vertices),
        edges=len(core.edge_config.edges),
        version=str(schema.metadata.version) if schema.metadata.version else None,
    )


# ── the tolerant pass ───────────────────────────────────────────────────────


@dataclass
class _Builder:
    """Accumulates the declaration graph and its findings, refusing nothing.

    Every check delegates to the canonical-map machinery, one unit at a time,
    so a refusal on one declaration does not hide the next one. What this class
    adds is the bookkeeping: which node a refusal is about, and the graph it is
    a finding on.
    """

    op: ComposeManifestsOp
    manifests: dict[Side, GraphManifest]
    names: dict[Side, SideNames]
    declared: Any  # DeclaredMaps
    nodes: dict[str, PreviewNode] = field(default_factory=dict)
    edges: dict[str, PreviewEdge] = field(default_factory=dict)
    clusters: list[PreviewCluster] = field(default_factory=list)
    findings: list[ComposeFinding] = field(default_factory=list)
    index: ClusterIndex = field(
        default_factory=lambda: ClusterIndex(vertices=(), relations=())
    )
    composite: dict[Side, CanonicalizeOp] = field(default_factory=dict)
    #: Declarations at or past this position per kind were synthesized by
    #: compose rather than written by the author.
    synthesized_from: tuple[int, int] = (1 << 30, 1 << 30)
    #: ``{kind: [(composed name, left members, right members)]}`` for the names
    #: both sides arrive at and no cluster composes.
    groups: dict[str, list[tuple[str, list[str], list[str]]]] = field(
        default_factory=dict
    )

    # ── registries ──────────────────────────────────────────────────────────

    def add_node(self, node: PreviewNode) -> str:
        self.nodes.setdefault(node.id, node)
        return node.id

    def ghost(self, side: Side, name: str, kind: Kind = "vertex") -> str:
        """A node for a name a declaration mentions and the manifest does not have."""
        node_id = subject(side, name)
        if node_id not in self.nodes:
            self.add_node(
                PreviewNode(
                    id=node_id,
                    kind="ghost",
                    name=name,
                    side=side,
                    exists=False,
                    identity_mode=kind,
                )
            )
        return node_id

    def attribute(
        self,
        scope: Literal["composed", "canonical"],
        owner: str,
        name: str,
        *,
        identity: bool = False,
        field_type: str | None = None,
    ) -> str:
        """An attribute row on a composed or canonical class, minting both."""
        owner_id = self.add_node(
            PreviewNode(id=subject(scope, owner), kind=scope, name=owner)
        )
        node_id = subject(scope, owner, name)
        existing = self.nodes.get(node_id)
        if existing is None:
            self.add_node(
                PreviewNode(
                    id=node_id,
                    kind="attribute",
                    name=name,
                    owner=owner_id,
                    identity=identity,
                    field_type=field_type,
                )
            )
        elif identity and not existing.identity:
            self.nodes[node_id] = existing.model_copy(update={"identity": True})
        return node_id

    def add_edge(
        self,
        source: str,
        target: str,
        kind: EdgeKind,
        *,
        label: str = "",
        declared_by: str = "",
    ) -> str:
        edge_id = f"{kind}:{source}->{target}"
        self.edges.setdefault(
            edge_id,
            PreviewEdge(
                id=edge_id,
                source=source,
                target=target,
                kind=kind,
                label=label,
                declared_by=declared_by,
            ),
        )
        return edge_id

    def finding(
        self,
        kind: FindingKind,
        message: str,
        *,
        severity: Severity = "possible",
        nodes: Sequence[str] = (),
        edges: Sequence[str] = (),
        completion: dict[str, Any] | None = None,
        check: str | None = None,
    ) -> None:
        self.findings.append(
            ComposeFinding(
                kind=kind,
                severity=severity,
                message=message,
                source="structure",
                nodes=list(dict.fromkeys(nodes)),
                edges=list(dict.fromkeys(edges)),
                completion=completion,
                check=check,
            )
        )

    def from_refusal(
        self, exc: BaseException, *, fallback: FindingKind, nodes: Sequence[str] = ()
    ) -> None:
        """Record a refusal raised by one unit of the real resolution."""
        check = getattr(exc, "check", "") or None
        completion = getattr(exc, "completion", None)
        subjects = [s for s in getattr(exc, "subjects", ()) or () if s in self.nodes]
        self.finding(
            kind_for_check(check, type(exc).__name__) if check else fallback,
            str(exc),
            nodes=[*subjects, *nodes] or list(nodes),
            completion=completion.to_dict() if completion is not None else None,
            check=check,
        )

    # ── the passes ──────────────────────────────────────────────────────────

    def schema_nodes(self) -> None:
        """Every class and attribute each side declares."""
        for side in _SIDES:
            schema = self.manifests[side].graph_schema
            if schema is None:
                continue
            for vertex in schema.core_schema.vertex_config.vertices:
                class_id = self.add_node(
                    PreviewNode(
                        id=subject(side, vertex.name),
                        kind="class",
                        name=vertex.name,
                        side=side,
                        identity_mode=vertex.identity_mode,
                    )
                )
                keyed = {*vertex.identity, *vertex.digest_source_fields}
                for prop in vertex.properties:
                    self.add_node(
                        PreviewNode(
                            id=subject(side, vertex.name, prop.name),
                            kind="attribute",
                            name=prop.name,
                            side=side,
                            owner=class_id,
                            identity=prop.name in keyed,
                            field_type=_type_label(prop),
                        )
                    )
            for relation in sorted(self.names[side].relations):
                self.add_node(
                    PreviewNode(
                        id=subject(side, relation),
                        kind="relation",
                        name=relation,
                        side=side,
                    )
                )

    def resolve_clusters(self) -> None:
        """Resolve every declaration on its own, so one refusal hides no other."""
        specs: dict[Kind, list[ClusterSpec]] = {"vertex": [], "relation": []}
        declarations: dict[Kind, list[Any]] = {
            "vertex": list(self.op.vertex_equivalences),
            "relation": list(self.op.relation_equivalences),
        }
        kinds: tuple[Kind, Kind] = ("vertex", "relation")
        for position, kind in enumerate(kinds):
            threshold = self.synthesized_from[position]
            for index, declaration in enumerate(declarations[kind]):
                specs[kind].append(
                    self._resolve_one(
                        declaration, kind=kind, synthesized=index >= threshold
                    )
                )
        self._shape_checks(specs["vertex"], kind="vertex")
        self._shape_checks(specs["relation"], kind="relation")
        self._member_existence(specs)
        self.index = _index_of(declarations, specs)
        self._record_clusters()

    def _resolve_one(
        self,
        declaration: VertexEquivalence | RelationEquivalence,
        *,
        kind: Kind,
        synthesized: bool = False,
    ) -> ClusterSpec:
        """One declaration through the real resolver; its refusal becomes a finding."""
        try:
            return _resolve_cluster(
                declaration,
                kind=kind,
                declared=self.declared,
                names=self.names,
                synthesized=synthesized,
            )
        except ComposeCanonicalConflictError as exc:
            self.from_refusal(exc, fallback="disagreement")
        except ValueError as exc:  # a malformed declaration, reported as written
            self.finding("disagreement", str(exc))
        # A best-effort shape, so the cluster still draws: members as spelled,
        # and the composed name only if the author gave one.
        return ClusterSpec(
            left=tuple(declaration.members("left")),
            right=tuple(declaration.members("right")),
            into=declaration.into or "",
            declared_into=declaration.into,
            synthesized=synthesized,
        )

    def _shape_checks(self, specs: Sequence[ClusterSpec], *, kind: Kind) -> None:
        """Overlap, shared composed name, occupied composed name -- all of them.

        The three rules of
        :func:`~graflo.architecture.evolution.equivalence.index_clusters`,
        reported together rather than one per run. Occupancy is computed
        against the declared map, not the raw side names: a composed name whose
        occupant another declaration renames away is not occupied.
        """
        claimed: dict[tuple[Side, str], int] = {}
        into_owner: dict[str, int] = {}
        claimed_by_side: dict[Side, set[str]] = {"left": set(), "right": set()}
        for spec in specs:
            claimed_by_side["left"].update(spec.left)
            claimed_by_side["right"].update(spec.right)

        for position, spec in enumerate(specs):
            for side in _SIDES:
                for name in spec.left if side == "left" else spec.right:
                    prior = claimed.get((side, name))
                    if prior is not None and prior != position:
                        self.finding(
                            "cluster_overlap",
                            f"{kind} equivalence: {side}:{name} is claimed by two "
                            f"equivalence declarations (into {specs[prior].into!r} "
                            f"and into {spec.into!r}); merge them into one "
                            "declaration",
                            nodes=[subject(side, name)],
                        )
                    claimed[(side, name)] = position
            if not spec.into:
                continue
            owner = into_owner.get(spec.into)
            if owner is not None and owner != position:
                self.finding(
                    "shared_into",
                    f"{kind} equivalence: two declarations both target into "
                    f"{spec.into!r}; sharing one `into` collapses them into one "
                    "composed class — spell it as one declaration",
                    nodes=[subject("composed", spec.into)],
                )
            into_owner[spec.into] = position
            for side in _SIDES:
                members = spec.left if side == "left" else spec.right
                occupied = self.names[side].of_kind(kind) - _moving(
                    _side_mapping(self.declared, side, kind)
                )
                if spec.into not in occupied or spec.into in members:
                    continue
                if spec.into in claimed_by_side[side]:
                    continue  # another declaration renames the occupant away
                self.finding(
                    "occupied_into",
                    f"{kind} equivalence: into {spec.into!r} already exists on the "
                    f"{side} side but is not a member of its cluster "
                    f"({side}={list(members)})",
                    nodes=[subject(side, spec.into), subject("composed", spec.into)],
                )

    def _member_existence(self, specs: Mapping[Kind, Sequence[ClusterSpec]]) -> None:
        """Every member a side does not declare, with the near-spelling hint."""
        for kind in ("vertex", "relation"):
            for spec in specs[kind]:  # type: ignore[index]
                for side in _SIDES:
                    known = self.names[side].of_kind(kind)  # type: ignore[arg-type]
                    for member in spec.left if side == "left" else spec.right:
                        if member in known:
                            continue
                        self.finding(
                            "unknown_member",
                            f"compose: {side} {kind} {member!r} is not in the "
                            f"{side} manifest{did_you_mean(member, known)}",
                            nodes=[self.ghost(side, member, kind)],  # type: ignore[arg-type]
                        )

    def _record_clusters(self) -> None:
        """One :class:`PreviewCluster` per declaration, with its member edges."""
        aligned = {a.vertex for a in self.op.identity_alignments}
        for kind, clusters in (
            ("vertex", self.index.vertices),
            ("relation", self.index.relations),
        ):
            for position, cluster in enumerate(clusters):
                into = cluster.into or None
                composed_id = (
                    self.add_node(
                        PreviewNode(
                            id=subject("composed", into),
                            kind="composed",
                            name=into,
                        )
                    )
                    if into
                    else None
                )
                self.clusters.append(
                    PreviewCluster(
                        id=f"{kind}-cluster-{position}",
                        kind=kind,  # type: ignore[arg-type]
                        into=into,
                        declared_into=cluster.declared_into,
                        left=list(cluster.left),
                        right=list(cluster.right),
                        synthesized=cluster.synthesized,
                        declared_identity=getattr(cluster.declaration, "identity", None)
                        is not None,
                        aligned=into in aligned,
                    )
                )
                if composed_id is None:
                    continue
                for side in _SIDES:
                    for member in cluster.members(side):
                        member_id = (
                            subject(side, member)
                            if member in self.names[side].of_kind(kind)  # type: ignore[arg-type]
                            else self.ghost(side, member, kind)  # type: ignore[arg-type]
                        )
                        self.add_edge(
                            member_id,
                            composed_id,
                            "member",
                            declared_by=f"{kind}-cluster-{position}",
                        )
                self._property_equivalence_edges(cluster, composed_id)

    def _property_equivalence_edges(self, cluster: Cluster, composed_id: str) -> None:
        """An attribute-level edge per :class:`PropertyEquivalence` side."""
        declaration = cluster.declaration
        if not isinstance(declaration, VertexEquivalence):
            return
        for side in _SIDES:
            for member, attr_map in cluster.property_maps(side).items():
                for old, new in attr_map.items():
                    self.add_edge(
                        subject(side, member, old),
                        self.attribute("composed", cluster.into, new),
                        "property_equivalence",
                        label=new,
                        declared_by=cluster.into,
                    )

    def composite_maps(self) -> None:
        """Carry each declared entry into its side's composite, one at a time."""
        for side in _SIDES:
            cm: CanonicalMap = self.declared[side]
            other: Side = "right" if side == "left" else "left"
            vertices, relations, properties = _cluster_maps(self.index, side)
            for kind, mapping, out in (
                ("vertex", cm.vertices, vertices),
                ("relation", cm.relations, relations),
            ):
                for source, target in mapping.items():
                    self._carry_one(
                        out,
                        source,
                        target,
                        kind=kind,  # type: ignore[arg-type]
                        side=side,
                        other=other,
                        cm=cm,
                    )
            self._carry_properties(properties, cm, side=side, other=other)
            self.composite[side] = CanonicalizeOp(
                vertices=vertices,
                relations=relations,
                properties=properties,
                allow_merges=True,
                allow_self_relations=True,
                allow_observation_fusion=True,
            )
            self._map_edges(side, kind="vertex", mapping=vertices)
            self._map_edges(side, kind="relation", mapping=relations)

    def _carry_one(
        self,
        out: dict[str, str],
        source: str,
        target: str,
        *,
        kind: Kind,
        side: Side,
        other: Side,
        cm: CanonicalMap,
    ) -> None:
        """One declared entry through the real classification.

        ``_carry_declared_entries`` is a loop over a mapping, so handing it a
        one-entry mapping classifies exactly that entry -- member, satisfied,
        translated, inapplicable, dangling or incomplete -- in the same order
        and by the same rules compose uses.
        """
        before = dict(out)
        shared = (
            self.declared.both.vertices
            if kind == "vertex"
            else self.declared.both.relations
        )
        try:
            _carry_declared_entries(
                out,
                {source: target},
                names=self.names[side].of_kind(kind),
                other_names=self.names[other].of_kind(kind),
                shared_sources=shared,
                index=self.index,
                side=side,
                kind=kind,
            )
        except ComposeIncompleteError as exc:
            self.from_refusal(exc, fallback="incomplete")
            return
        except ComposeCanonicalConflictError as exc:
            self.from_refusal(exc, fallback="dangling")
            return
        if (
            out == before
            and source not in out
            and target in self.names[side].of_kind(kind)
        ):
            self.finding(
                "satisfied",
                f"the {side} canonical {kind} entry {source!r} -> {target!r} is "
                "taken as already applied (source absent, target present)",
                severity="note",
                nodes=[subject(side, target)],
            )

    def _carry_properties(
        self,
        properties: dict[str, dict[str, str]],
        cm: CanonicalMap,
        *,
        side: Side,
        other: Side,
    ) -> None:
        """The declared attribute maps, folded onto the cluster's own."""
        known = self.names[side].vertices
        for cls, attrs in cm.properties.items():
            if cls not in known:
                if cm.canonical_class(cls) in known:
                    continue
                if cls in self.declared.both.properties and (
                    cls in self.names[other].vertices
                    or cm.canonical_class(cls) in self.names[other].vertices
                ):
                    continue
                where = (
                    "a composed name"
                    if cls in self.index.labels or cls in self.index.declared_intos
                    else "no class on that side"
                )
                self.finding(
                    "dangling",
                    f"the {side} attribute map is keyed by {cls!r}, {where}; "
                    "`properties` is keyed by the source class",
                    nodes=[subject(side, cls)],
                )
                continue
            bucket = properties.setdefault(cls, {})
            for old, new in attrs.items():
                existing = bucket.get(old)
                if existing is not None and existing != new:
                    self.finding(
                        "property_disagreement",
                        f"the canonical map says {side}:{cls}.{old} -> {new!r}, but "
                        f"the equivalence maps it to {existing!r}",
                        nodes=[subject(side, cls, old)],
                    )
                    continue
                bucket[old] = new
                owning = next(
                    (c for c in self.index.vertices if cls in c.members(side)), None
                )
                target = (
                    self.attribute("composed", owning.into, new)
                    if owning is not None
                    else self.attribute("canonical", cm.canonical_class(cls), new)
                )
                self.add_edge(
                    subject(side, cls, old),
                    target,
                    "property_map",
                    label=new,
                    declared_by=side,
                )

    def _map_edges(self, side: Side, *, kind: Kind, mapping: Mapping[str, str]) -> None:
        """A ``map`` edge per declared rename that no cluster already explains."""
        members = (
            self.index.vertex_members(side)
            if kind == "vertex"
            else self.index.relation_members(side)
        )
        labels = self.index.labels if kind == "vertex" else self.index.relation_labels
        for source, target in mapping.items():
            if source == target or source in members:
                continue
            scope = "composed" if target in labels else "canonical"
            source_id = (
                subject(side, source)
                if source in self.names[side].of_kind(kind)
                else self.ghost(side, source, kind)
            )
            target_id = self.add_node(
                PreviewNode(id=subject(scope, target), kind=scope, name=target)  # type: ignore[arg-type]
            )
            self.add_edge(source_id, target_id, "map", label=target, declared_by=side)

    def property_checks(self) -> None:
        """Absent fields, re-targeted canonical attributes, rename collisions."""
        for cluster in self.index.vertices:
            for side in _SIDES:
                manifest = self.manifests[side]
                try:
                    _check_property_fields_exist(
                        manifest, cluster, side=side, declared=self.declared[side]
                    )
                except ComposeCanonicalConflictError as exc:
                    self.from_refusal(exc, fallback="unknown_property")
                try:
                    _check_attribute_fixed_points(
                        cluster, side=side, declared=self.declared[side]
                    )
                except ComposeCanonicalConflictError as exc:
                    self.from_refusal(exc, fallback="property_retarget")
        for side in _SIDES:
            relabel = self.composite.get(side)
            if relabel is None:
                continue
            for member, attr_map in relabel.properties.items():
                one = CanonicalizeOp(properties={member: attr_map}, allow_merges=True)
                try:
                    _check_property_maps_against_manifest(
                        self.manifests[side], one, side=side
                    )
                except ComposeCanonicalConflictError as exc:
                    self.from_refusal(exc, fallback="property_collision")

    def same_names(self) -> None:
        """Names both sides arrive at that no cluster composes."""
        policy = self.op.name_conflict
        resolution = _ClusterResolution(
            index=self.index,
            side_maps=_SideMaps(
                left=self.composite["left"], right=self.composite["right"]
            ),
            declared=self.declared,
        )
        near = policy == "union_right"
        for kind in ("vertex", "relation"):
            groups = same_name_groups(
                resolution,
                self.names,
                kind=kind,  # type: ignore[arg-type]
                near=near,
            )
            if not groups:
                continue
            self.groups[kind] = groups
            shared = [into for into, _left, _right in groups]
            if policy == "error":
                self.finding(
                    "name_collision",
                    f"{shared} exist on both sides after the declared maps and no "
                    "equivalence composes them",
                    nodes=[subject("composed", name) for name in shared],
                    completion={
                        "kind": "declare_equivalences",
                        f"{kind}_equivalences": [
                            dict(p) for p in _same_name_payloads(groups)
                        ],
                    },
                )
            elif policy == "prefix_right":
                self.finding(
                    "prefixed",
                    f"{shared} exist on both sides; prefix_right keeps them apart "
                    "under r_ names",
                    severity="note",
                    nodes=[subject("composed", name) for name in shared],
                )
            self._suggest(groups, kind=kind)  # type: ignore[arg-type]
        if policy == "error":
            self._near_collisions()

    def _near_collisions(self) -> None:
        """Two spellings of one name, which compose refuses under ``error``."""
        for kind in ("vertex", "relation"):
            post = {
                side: sorted(
                    _preimages(
                        _kind_mapping(self.composite[side], kind),  # type: ignore[arg-type]
                        self.names[side].of_kind(kind),  # type: ignore[arg-type]
                    )
                )
                for side in _SIDES
                if side in self.composite
            }
            if len(post) != 2:
                continue
            labels = (
                self.index.labels if kind == "vertex" else self.index.relation_labels
            )
            for left, right in canonical_near_collisions(
                post["left"], post["right"], exempt=labels
            ):
                self.finding(
                    "near_collision",
                    f"{left!r} and {right!r} denote the same concept under different "
                    "naming conventions, so they would compose into two unrelated "
                    f"{kind} types with the source data split between them",
                    nodes=[subject("composed", left), subject("composed", right)],
                )

    def _suggest(
        self,
        groups: Sequence[tuple[str, list[str], list[str]]],
        *,
        kind: Kind,
    ) -> None:
        """Draw the equivalence a shared name is asking for."""
        for into, left, right in groups:
            target = self.add_node(
                PreviewNode(id=subject("composed", into), kind="composed", name=into)
            )
            for side, members in (("left", left), ("right", right)):
                for member in members:
                    self.add_edge(
                        subject(side, member),  # type: ignore[arg-type]
                        target,
                        "suggested",
                        label="declare",
                        declared_by="completion",
                    )

    def composed_attributes(self) -> None:
        """Fill in every composed class's attribute rows.

        The union of its members' properties under that side's composite
        rename -- which is what the composed vertex actually carries. Marked as
        identity when the declaration says so, or when a member keys on it.
        """
        for cluster in self.index.vertices:
            if not cluster.into:
                continue
            declaration = cluster.declaration
            declared_identity = (
                set(declaration.identity)
                if isinstance(declaration, VertexEquivalence)
                and isinstance(declaration.identity, list)
                else set()
            )
            flagged = {
                pe.into
                for pe in getattr(declaration, "properties", [])
                if getattr(pe, "identity", False)
            }
            for side in _SIDES:
                schema = self.manifests[side].graph_schema
                if schema is None:
                    continue
                vertex_config = schema.core_schema.vertex_config
                renames = (
                    self.composite[side].properties if side in self.composite else {}
                )
                for member in cluster.members(side):
                    if member not in vertex_config.vertex_set:
                        continue
                    vertex = vertex_config[member]
                    rename = renames.get(member, {})
                    keyed = {*vertex.identity, *vertex.digest_source_fields}
                    for prop in vertex.properties:
                        composed_name = rename.get(prop.name, prop.name)
                        self.attribute(
                            "composed",
                            cluster.into,
                            composed_name,
                            identity=(
                                composed_name in declared_identity
                                or composed_name in flagged
                                or (not declared_identity and prop.name in keyed)
                            ),
                            field_type=_type_label(prop),
                        )

    def identity_checks(self) -> None:
        """Members of one cluster whose natural keys disagree, unresolved.

        The rule of
        :func:`~graflo.architecture.evolution.compose._composed_identity`: only
        plain natural keys take part -- blank, assigned, hash and funnel
        identities are reconciled (or refused) by the vertex merge itself.
        """
        aligned = {a.vertex for a in self.op.identity_alignments}
        for cluster in self.index.vertices:
            declaration = cluster.declaration
            if not isinstance(declaration, VertexEquivalence):
                continue
            if declaration.identity is not None or cluster.into in aligned:
                continue
            if any(pe.identity for pe in declaration.properties):
                continue
            keys: list[tuple[Side, str, tuple[str, ...]]] = []
            for side in _SIDES:
                schema = self.manifests[side].graph_schema
                if schema is None:
                    continue
                vertex_config = schema.core_schema.vertex_config
                renames = (
                    self.composite[side].properties if side in self.composite else {}
                )
                for member in cluster.members(side):
                    if member not in vertex_config.vertex_set:
                        continue
                    vertex = vertex_config[member]
                    if (
                        vertex.blank
                        or vertex.assigned
                        or vertex.hash_identity_properties
                        or vertex.identity_funnel is not None
                    ):
                        continue
                    rename = renames.get(member, {})
                    keys.append(
                        (side, member, tuple(rename.get(f, f) for f in vertex.identity))
                    )
            if len({frozenset(k) for _s, _m, k in keys}) > 1:
                detail = "; ".join(f"{s}:{m}={list(k)}" for s, m, k in keys)
                self.finding(
                    "identity_disagreement",
                    f"composed vertex {cluster.into!r} has members that disagree on "
                    f"identity ({detail}) and nothing resolves it",
                    nodes=[
                        subject("composed", cluster.into),
                        *(subject(s, m) for s, m, _k in keys),
                    ],
                )

    def merge_checks(self) -> None:
        """What the schema union itself refuses, one cluster at a time.

        Not a second implementation of the rules: this calls
        :func:`~graflo.architecture.evolution.merge_core.merge_vertex_models`,
        the same kernel ``_union_schema`` calls, so a type clash, a unit clash,
        two identity modes that exclude each other, a divergent funnel and a
        secondary identity claimed twice are all found by the code that decides
        them. One call per cluster, so a refusal on one does not hide the next.

        The kernel takes the composed name as an argument and never reads
        ``Vertex.name``, so only the *attribute* renames have to be applied
        first -- the class relabel is irrelevant here. Applying them through
        the whole-side ``apply_canonicalize`` would be wrong twice over: it is
        all-or-nothing per side, and it runs this very kernel internally, so
        one bad cluster would abort every other cluster's finding.

        Compose reaches the kernel once more after the union, through
        ``_apply_identity_alignments``; no case is known that refuses only
        there, and this pass would not see it if one appeared.
        """
        for cluster in self.index.vertices:
            members: list[tuple[Side, str, Vertex]] = []
            for side in _SIDES:
                schema = self.manifests[side].graph_schema
                if schema is None:
                    continue
                vertex_config = schema.core_schema.vertex_config
                renames = (
                    self.composite[side].properties if side in self.composite else {}
                )
                for member in cluster.members(side):
                    if member not in vertex_config.vertex_set:
                        continue  # reported by the existence check
                    try:
                        relabelled = relabel_vertex_fields(
                            vertex_config[member], renames.get(member, {})
                        )
                    except ValueError as exc:
                        # A rename the member cannot carry. Its own rule, not a
                        # merge one -- and pydantic has already eaten any
                        # `check` it might have carried.
                        self.from_refusal(
                            exc,
                            fallback="property_collision",
                            nodes=[subject(side, member)],
                        )
                        continue
                    members.append((side, member, relabelled))
            if len(members) < 2:
                continue
            try:
                merge_vertex_models(
                    [vertex for _s, _m, vertex in members], cluster.into
                )
            except ValueError as exc:
                # Deliberately wider than the two typed errors: this module
                # describes a compose, it never raises one. An untyped refusal
                # from deeper in the kernel still earns a finding -- a general
                # one, since nothing says which rule it is -- rather than
                # taking the whole preview down with it.
                self.from_refusal(
                    exc,
                    fallback="disagreement",
                    nodes=self._merge_nodes(cluster, members, exc),
                )

    def _effective_map(self, side: Side, kind: Kind) -> dict[str, str]:
        """Where a name on *side* ends up, as the union sees it.

        Compose applies the composite relabel first and the right side's
        name-conflict policy second (``compose_manifests``, in that order), so
        the map the union sees is the policy composed onto the relabel. The
        policy is obtained from the very function compose calls, over the
        *post-relabel* names, so ``prefix_right`` cannot drift out of sync
        here and start reporting conflicts on composes that succeed.

        Under ``error`` and ``union_right`` the policy is empty by
        construction, and when it refuses, ``same_names`` has already said so.
        """
        composite = (
            _kind_mapping(self.composite[side], kind) if side in self.composite else {}
        )
        if side != "right":
            return dict(composite)

        left_after = {
            _kind_mapping(self.composite["left"], kind).get(name, name)
            if "left" in self.composite
            else name
            for name in self.names["left"].of_kind(kind)
        }
        right_after = sorted(
            {composite.get(name, name) for name in self.names[side].of_kind(kind)}
        )
        try:
            policy = _resolve_schema_collisions(
                left_names=left_after,
                right_names=right_after,
                exempt=(
                    self.index.labels
                    if kind == "vertex"
                    else self.index.relation_labels
                ),
                name_conflict=self.op.name_conflict,
                kind=kind,
                equivalence_hint=(
                    "VertexEquivalence" if kind == "vertex" else "RelationEquivalence"
                ),
            )
        except ValueError:
            policy = {}  # the refusal is `same_names`' to report, not this pass's
        if not policy:
            return dict(composite)
        out = {name: policy.get(target, target) for name, target in composite.items()}
        for name in self.names[side].of_kind(kind):
            if name not in out and name in policy:
                out[name] = policy[name]
        return out

    def edge_merge_checks(self) -> None:
        """Two declarations of one logical edge that the union cannot fold.

        Not scopeable to the relation clusters: ``_union_schema`` folds *every*
        edge of both sides by ``edge_id``, and two edges collide precisely
        because a **vertex** cluster renamed their endpoints onto one name. So
        this remaps both sides whole, groups by the resulting ``edge_id``, and
        folds each group on its own -- one unit at a time, so a group that
        refuses does not hide the next.
        """
        by_id: dict[Any, list[tuple[Side, Edge]]] = {}
        for side in _SIDES:
            schema = self.manifests[side].graph_schema
            if schema is None:
                continue
            vertices = self._effective_map(side, "vertex")
            relations = self._effective_map(side, "relation")
            for edge in schema.core_schema.edge_config.edges:
                remapped = edge.model_copy(
                    update={
                        "source": vertices.get(edge.source, edge.source),
                        "target": vertices.get(edge.target, edge.target),
                        **(
                            {"relation": relations.get(edge.relation, edge.relation)}
                            if edge.relation is not None
                            else {}
                        ),
                    }
                )
                by_id.setdefault(remapped.edge_id, []).append((side, remapped))

        for edge_id, group in by_id.items():
            if len(group) < 2:
                continue
            folded = group[0][1]
            for _side, edge in group[1:]:
                try:
                    folded = merge_edge_pair(folded, edge)
                except ValueError as exc:  # never raise out of a preview
                    self.from_refusal(
                        exc,
                        fallback="edge_conflict",
                        nodes=[
                            subject("composed", str(name))
                            for name in edge_id
                            if name is not None
                        ],
                    )
                    break

    def _merge_nodes(
        self,
        cluster: Cluster,
        members: list[tuple[Side, str, Vertex]],
        exc: ValueError,
    ) -> list[str]:
        """The nodes a union refusal is about, most specific first.

        A field conflict names the composed attribute and every member that
        declares it -- *including* members that left it untyped, which is the
        side an author most needs to see. Anything else names the classes.
        """
        nodes: list[str] = [subject("composed", cluster.into)]
        fields: tuple[str, ...] = getattr(exc, "fields", ())
        if not fields:
            return [*nodes, *(subject(s, m) for s, m, _v in members)]
        for name in fields:
            nodes.append(subject("composed", cluster.into, name))
            for side, member, vertex in members:
                if any(prop.name == name for prop in vertex.properties):
                    nodes.append(subject(side, member, name))
        return nodes

    def build(self) -> ComposePreview:
        self.schema_nodes()
        self.resolve_clusters()
        self.composite_maps()
        self.composed_attributes()
        self.property_checks()
        self.same_names()
        self.identity_checks()
        self.merge_checks()
        self.edge_merge_checks()
        return ComposePreview(
            left_name=_manifest_name(self.manifests["left"], "left"),
            right_name=_manifest_name(self.manifests["right"], "right"),
            name_conflict=self.op.name_conflict,
            nodes=list(self.nodes.values()),
            edges=list(self.edges.values()),
            clusters=self.clusters,
            findings=self.findings,
        )


def _kind_mapping(relabel: CanonicalizeOp, kind: Kind) -> dict[str, str]:
    return relabel.vertices if kind == "vertex" else relabel.relations


def _side_mapping(declared: Any, side: Side, kind: Kind) -> Mapping[str, str]:
    cm: CanonicalMap = declared[side]
    return cm.vertices if kind == "vertex" else cm.relations


def _type_label(prop: Any) -> str | None:
    """``"LIST[STRING]"`` / ``"INT"`` / ``None``, as a reader wants to see it."""
    if prop.type is None:
        return None
    base = str(prop.type)
    if prop.item_type is not None:
        return f"{base}[{prop.item_type}]"
    return base


def _manifest_name(manifest: GraphManifest, fallback: str) -> str:
    schema = manifest.graph_schema
    if schema is not None and schema.metadata.name:
        return str(schema.metadata.name)
    if manifest.metadata is not None and getattr(manifest.metadata, "name", None):
        return str(manifest.metadata.name)
    return fallback


def _index_of(
    declarations: Mapping[Kind, Sequence[Any]],
    specs: Mapping[Kind, Sequence[ClusterSpec]],
) -> ClusterIndex:
    """A :class:`ClusterIndex` over resolved specs, skipping the unnamed ones.

    Built directly rather than through ``index_clusters``: the shape checks
    have already run tolerantly, and a cluster whose composed name could not be
    resolved has nothing to index under.
    """
    built: dict[Kind, list[Cluster]] = {"vertex": [], "relation": []}
    for kind in ("vertex", "relation"):
        for declaration, spec in zip(
            declarations[kind],  # type: ignore[index]
            specs[kind],  # type: ignore[index]
            strict=True,
        ):
            if not spec.into:
                continue
            built[kind].append(  # type: ignore[index]
                Cluster(
                    left=spec.left,
                    right=spec.right,
                    into=spec.into,
                    declaration=declaration,
                    aliases=spec.aliases,
                    declared_into=spec.declared_into,
                    synthesized=spec.synthesized,
                )
            )
    return ClusterIndex(
        vertices=tuple(built["vertex"]),  # type: ignore[arg-type]
        relations=tuple(built["relation"]),  # type: ignore[arg-type]
    )


# ── merging two branches: a projection, not a preview ───────────────────────


class SlotNode(ConfigBaseModel):
    """One addressable location in the manifest, and what each branch did to it."""

    id: str = PydanticField(
        ..., description="The slot as a path: `vertex/person/field/age`."
    )
    segment: str = PydanticField(..., description="This slot's own last segment.")
    depth: int = PydanticField(..., description="How many segments deep it sits.")
    parent: str | None = PydanticField(
        default=None, description="The containing slot, or None at the root."
    )
    contested: bool = PydanticField(
        default=False, description="Whether both branches changed this slot."
    )
    reason: str | None = PydanticField(
        default=None, description="Why it could not be reconciled automatically."
    )
    left_ops: list[str] = PydanticField(
        default_factory=list, description="What the left branch did here, by op name."
    )
    right_ops: list[str] = PydanticField(
        default_factory=list, description="What the right branch did here."
    )
    clean_ops: list[str] = PydanticField(
        default_factory=list,
        description="Ops the merge applied here without a decision.",
    )
    base_excerpt: dict[str, Any] | None = PydanticField(
        default=None, description="The ancestor's state here, for whoever decides."
    )


class MergePreview(ConfigBaseModel):
    """A three-way merge as a slot tree: what moved, and where the branches met."""

    nodes: list[SlotNode] = PydanticField(default_factory=list)
    conflicts: int = PydanticField(default=0, description="Contested slot count.")
    clean: bool = PydanticField(
        default=True, description="Whether the merge left no decision to make."
    )
    merged_hash: str | None = PydanticField(
        default=None,
        description="Content hash of the merged manifest, if there is one.",
    )
    warnings: list[str] = PydanticField(default_factory=list)

    @property
    def contested(self) -> list[SlotNode]:
        """The contested slots, in tree order."""
        return [node for node in self.nodes if node.contested]

    def children_of(self, node_id: str | None) -> list[SlotNode]:
        """Slots directly under *node_id*; pass ``None`` for the roots."""
        return [node for node in self.nodes if node.parent == node_id]


def build_merge_preview(
    result: Any, *, base: GraphManifest | None = None
) -> MergePreview:
    """Project a :class:`~graflo.architecture.evolution.merge3.MergeResult` onto its slot tree.

    Slots are paths and contain one another, so the set of slots a merge
    touched is already a tree once its prefixes are filled in. Contested slots
    carry what each branch did and the ancestor's state; slots the merge
    settled on its own carry the ops it applied, which is the context that makes
    a conflict legible — a rename colliding with three field edits is one
    conflict about the vertex, with the field edits visible beneath it.

    Args:
        result: What ``merge_three_way`` returned alongside the merged manifest.
        base: The common ancestor. Unused today; accepted so a caller can pass
            it without knowing whether the excerpt came from the result.

    Returns:
        A :class:`MergePreview`. Never raises: a merge that could not complete
        is exactly the case worth drawing.
    """
    from .merge3 import describe_slot, op_slots

    del base  # excerpts travel on the conflicts themselves

    nodes: dict[str, SlotNode] = {}

    def ensure(slot: tuple[str, ...]) -> SlotNode:
        """The node for *slot*, and every slot that contains it."""
        node_id = describe_slot(slot)
        existing = nodes.get(node_id)
        if existing is not None:
            return existing
        parent = describe_slot(slot[:-1]) if len(slot) > 1 else None
        if len(slot) > 1:
            ensure(slot[:-1])
        node = SlotNode(
            id=node_id,
            segment=slot[-1],
            depth=len(slot) - 1,
            parent=parent,
        )
        nodes[node_id] = node
        return node

    for op in result.ops:
        for slot in op_slots(op):
            if not slot:
                continue
            ensure(slot).clean_ops.append(op.op)

    for conflict in result.conflicts:
        slot = tuple(conflict.slot)
        if not slot:
            continue
        node = ensure(slot)
        nodes[node.id] = node.model_copy(
            update={
                "contested": True,
                "reason": conflict.reason,
                "left_ops": [op.op for op in conflict.left_ops],
                "right_ops": [op.op for op in conflict.right_ops],
                "base_excerpt": conflict.base_excerpt or None,
                # A contested slot is held back whole, so nothing here was
                # applied cleanly; showing both would misread the result.
                "clean_ops": [],
            }
        )

    ordered = sorted(nodes.values(), key=lambda n: (n.depth, n.id))
    return MergePreview(
        nodes=ordered,
        conflicts=len(result.conflicts),
        clean=result.clean,
        merged_hash=result.merged_hash,
        warnings=list(result.warnings),
    )


# ── the entry point ─────────────────────────────────────────────────────────


def _extended(op: ComposeManifestsOp, builder: _Builder) -> ComposeManifestsOp | None:
    """*op* with an equivalence per name both sides arrive at, or ``None``.

    What ``resolve_clusters`` does under ``union_right``: every shared or
    alike-spelled name becomes a 1-1 cluster into the left spelling, so the
    union goes through the same identity and property reconciliation a declared
    cluster does.
    """
    vertex_groups = builder.groups.get("vertex", [])
    relation_groups = builder.groups.get("relation", [])
    if not vertex_groups and not relation_groups:
        return None
    nary = any(
        len(left_members) > 1 or len(right_members) > 1
        for _into, left_members, right_members in (*vertex_groups, *relation_groups)
    )
    return op.model_copy(
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


def preview_compose(
    left: GraphManifest,
    right: GraphManifest,
    op: ComposeManifestsOp,
    *,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
    attempt: bool = True,
) -> ComposePreview:
    """The declaration graph of a compose, and everything wrong with it.

    Walks *op*'s equivalences and canonical maps against *left* and *right*
    without refusing: each declaration, each map entry and each member is put
    through the same check
    :func:`~graflo.architecture.evolution.compose.compose_manifests` uses, one
    at a time, so a problem with one does not hide the rest. Every refusal
    becomes a ``possible`` finding naming the nodes it is about.

    With *attempt*, compose is then run for real and its result -- the composed
    schema's shape, or the one refusal it raised, with the completion that
    would settle it -- is recorded as the outcome and as a single ``refusal``
    finding. Set it to ``False`` to describe the declarations without composing.

    Args:
        left: The left manifest, in whatever vocabulary it is in.
        right: The right manifest.
        op: The compose op: equivalences, canonical maps, identity alignments.
        canonical_maps: Extra ``(side, map)`` pairs, folded into ``op``'s.
        attempt: Whether to run a real compose for the authoritative outcome.

    Returns:
        A :class:`ComposePreview`. It never raises for a problem with the
        declarations -- that is the point -- so an empty
        :attr:`~ComposePreview.blocking` is what "this would compose" looks
        like.
    """
    declared = fold_declared_maps(op, canonical_maps)
    manifests: dict[Side, GraphManifest] = {"left": left, "right": right}
    names: dict[Side, SideNames] = {
        "left": SideNames.of(left),
        "right": SideNames.of(right),
    }

    builder = _Builder(op=op, manifests=manifests, names=names, declared=declared)
    preview = builder.build()

    if op.name_conflict == "union_right":
        # The names both sides arrive at are only known once the first pass has
        # applied the declared maps, so the clusters compose would synthesize
        # for them are resolved on a second pass over the extended op.
        extended = _extended(op, builder)
        if extended is not None:
            preview = _Builder(
                op=extended,
                manifests=manifests,
                names=names,
                declared=declared,
                synthesized_from=(
                    len(op.vertex_equivalences),
                    len(op.relation_equivalences),
                ),
            ).build()
    if not attempt:
        return preview
    outcome, subjects = _attempt(left, right, op, canonical_maps)
    return preview.with_outcome(outcome, subjects=subjects)


def _attempt(
    left: GraphManifest,
    right: GraphManifest,
    op: ComposeManifestsOp,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]],
) -> tuple[ComposeOutcome, tuple[str, ...]]:
    """Compose for real, and turn whichever way it went into an outcome."""
    from .alignment import AlignmentConflictError
    from .compose import (
        ComposeIdentityError,
        ComposeNameConflictError,
        compose_manifests,
    )

    try:
        composed = compose_manifests(
            left, right, op, canonical_maps=canonical_maps, finish_init=False
        )
    except (
        ClusterConflictError,
        ComposeCanonicalConflictError,
        ComposeIdentityError,
        ComposeNameConflictError,
        AlignmentConflictError,
        UnknownMemberError,
        ValueError,
    ) as exc:
        return outcome_from_exception(exc)
    return outcome_from_manifest(composed), ()


__all__ = [
    "ComposeFinding",
    "ComposeOutcome",
    "ComposePreview",
    "EdgeKind",
    "FindingKind",
    "MergePreview",
    "NodeKind",
    "PreviewCluster",
    "PreviewEdge",
    "PreviewNode",
    "Severity",
    "SlotNode",
    "build_merge_preview",
    "expected_kinds",
    "kind_for_check",
    "outcome_from_exception",
    "outcome_from_manifest",
    "preview_compose",
]
