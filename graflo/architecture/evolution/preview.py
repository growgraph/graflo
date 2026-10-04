"""What a merge would do, and every way it could refuse — without refusing.

:func:`~graflo.architecture.evolution.merge.merge_manifests` raises at the
first refusal. That is right for a function that returns a manifest — a
half-merged model is worse than none — but it makes authoring a merge a
game of whack-a-mole: fix the contradiction the message names, run again, learn
about the next one. Three declarations that each refuse take three runs to
discover.

This module is the other view. :func:`preview_merge` walks the same
declarations and reports **every** problem it finds, as data:

* the **declaration graph** — classes and their attributes on each side, the
  clusters that collapse them, the canonical names the maps establish, and the
  edges between them. This is the ``class_A - attr_a - attr_b - class_B``
  picture the authoring model is actually about;
* **findings**, each naming the nodes it is about, at one of three severities —
  ``possible`` (found structurally, by this module), ``refusal`` (what merge
  actually raised, if it was asked to try) and ``note`` (an acknowledged
  heuristic, such as an entry taken as already applied);
* an **outcome**, from a real merge attempt.

Nothing here is a second implementation of the resolution rules. Every check
calls the function in :mod:`~graflo.architecture.evolution.canonical` that
merge itself calls, in units small enough — one declaration, one map entry,
one member — that a refusal on one unit does not hide the others. A refusal
carries its ``check`` and its ``subjects``, so the finding it becomes is
pinned to the same nodes the message names, and there is no parallel copy of
the rules to fall out of date.

The consistency invariant, asserted in the tests: **whatever merge refuses,
the structural pass has a finding of a matching kind for**, and a merge that
succeeds leaves no ``refusal`` finding behind.

Merging two branches of one lineage needs none of this.
:func:`~graflo.architecture.evolution.merge3.merge_three_way` already *returns*
its conflicts rather than raising them, one record per contested slot, so
:func:`build_merge3_preview` only has to put them in the shape they already
have: slots form a tree — ``vertex/person`` contains ``vertex/person/field/age``
— and the tree is what shows *where in the model* two branches collided.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.refusal import Refusal
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.identity_funnel import IdentityFunnel
from graflo.architecture.schema.vertex import FieldMergeError, Vertex

from .apply import relabel_vertex_fields
from .canonical import (
    Completion,
    MergeCanonicalConflictError,
    MergeIncompleteError,
    SideNames,
)
from .equivalence import (
    Cluster,
    ClusterIndex,
    Kind,
    Side,
    subject,
)
from .merge import (
    DemotedKey,
    MergeIdentityError,
    MergeNameConflictError,
)
from .merge_core import (
    EdgeMergeError,
    VertexMergeError,
    merge_edge_pair,
    merge_vertex_models,
)
from .merge_types import (
    MERGE_RETYPE_REMEDY,
    FieldTypeDeclarationError,
    UnionNames,
    field_type_ops,
    union_name_map,
    with_declared_types,
)
from .naming_graph import MergeNamingError, NamingGraph, build_naming
from .ops import (
    CanonicalizeOp,
    CanonicalMap,
    MergeManifestsOp,
    VertexEquivalence,
    identity_branches_funnel,
)

logger = logging.getLogger(__name__)

_SIDES: tuple[Side, ...] = ("left", "right")

#: What a preview node stands for. ``ghost`` is a name a declaration mentions
#: that the manifest on that side does not declare -- drawn, because the
#: absence is the point.
NodeKind = Literal["class", "relation", "attribute", "merged", "canonical", "ghost"]

#: What one declaration says about a pair of names. ``suggested`` comes from a
#: refusal's completion: the declaration that would settle it.
EdgeKind = Literal["member", "map", "property_map", "property_equivalence", "suggested"]

#: ``refusal`` is what merge raised; ``possible`` what this module found on
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
    "identity_coverage",
    "identity_collision",
    "type_conflict",
    "unit_conflict",
    "identity_mode_conflict",
    "identity_funnel_conflict",
    "secondary_identity_conflict",
    "edge_conflict",
    "reference_conversion",
    "lookup_demotion",
    "double_home",
    "vocabulary_override",
    "auto_local_key",
    "self_relation",
    "observation_fusion",
]

#: The ``check`` phrase of a refusal raised after naming, to the finding kind
#: it is an instance of. A naming refusal carries its findings' kinds itself
#: (:attr:`MergeOutcome.kinds`), so none of its phrases is listed. The keys are
#: matched as substrings of ``check``, longest first, so the ``vertex`` /
#: ``relation`` prefix the messages carry needs no entry of its own.
_KIND_BY_CHECK: dict[str, FindingKind] = {
    "near collision": "near_collision",
    "canonical split": "name_collision",
    "ambiguous reference": "ambiguity",
    "identity disagreement": "identity_disagreement",
    "identity coverage": "identity_coverage",
    "identity collision": "identity_collision",
    # What the schema union refuses. A bare ``conflict`` key would swallow all
    # six and must never be added.
    "field type conflict": "type_conflict",
    "field units conflict": "unit_conflict",
    "field type declaration": "dangling",
    "identity mode conflict": "identity_mode_conflict",
    "identity funnel conflict": "identity_funnel_conflict",
    "secondary identity conflict": "secondary_identity_conflict",
    "edge type disagreement": "edge_conflict",
}

#: The caption a suggested edge carries, per repair kind: what applying it does.
_SUGGESTED_LABEL: dict[str, str] = {
    "declare_equivalences": "declare",
    "set_into": "into",
    "rename_away": "rename",
    "extend_cluster": "fuses entities",
    "add_key_source": "key",
    "acknowledge": "accept",
}

#: Refusals that carry no ``check`` are classified by type alone. A value of
#: ``None`` means "no expectation" -- the preview is not asked to have seen it.
_KINDS_BY_TYPE: dict[type[BaseException], frozenset[FindingKind]] = {
    MergeNamingError: frozenset(
        {
            "cluster_overlap",
            "shared_into",
            "occupied_into",
            "unknown_member",
            "disagreement",
            "unnamed_cluster",
            "incomplete",
            "dangling",
            "name_collision",
            "near_collision",
            "double_home",
            "identity_disagreement",
            "identity_coverage",
            "property_disagreement",
            "property_collision",
            "property_retarget",
            "unknown_property",
            "self_relation",
            "observation_fusion",
        }
    ),
    MergeIncompleteError: frozenset({"incomplete", "name_collision"}),
    MergeIdentityError: frozenset(
        {"identity_disagreement", "identity_coverage", "identity_collision"}
    ),
    MergeNameConflictError: frozenset({"near_collision", "name_collision"}),
    # Inert while the union's refusals set ``check`` -- which they all do. Kept
    # so a raise site added later without one still classifies rather than
    # silently becoming exempt.
    FieldMergeError: frozenset({"type_conflict", "unit_conflict"}),
    VertexMergeError: frozenset(
        {
            "identity_mode_conflict",
            "identity_funnel_conflict",
            "secondary_identity_conflict",
        }
    ),
    EdgeMergeError: frozenset({"edge_conflict"}),
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
        description="Which manifest it comes from; merged names have none.",
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
        description="Where it was declared: a map scope, or a cluster's merged name.",
    )


class PreviewCluster(ConfigBaseModel):
    """One equivalence declaration, resolved as far as it could be."""

    id: str = PydanticField(..., description="Stable id of the cluster.")
    kind: Kind = PydanticField(
        ..., description="Whether it collapses classes or relations."
    )
    into: str | None = PydanticField(
        default=None, description="Merged name; None when it could not be resolved."
    )
    declared_into: str | None = PydanticField(
        default=None, description="`into` as the author wrote it, if any."
    )
    left: list[str] = PydanticField(default_factory=list, description="Left members.")
    right: list[str] = PydanticField(default_factory=list, description="Right members.")
    synthesized: bool = PydanticField(
        default=False, description="Declared by merge itself under `union_right`."
    )
    declared_identity: bool = PydanticField(
        default=False, description="Whether the declaration states an `identity`."
    )
    derived_identity: bool = PydanticField(
        default=False,
        description="Whether its `identity` has a derived or `local_key` branch.",
    )


class MergeFinding(ConfigBaseModel):
    """One thing wrong with the declarations, or one acknowledged heuristic."""

    kind: FindingKind = PydanticField(
        ..., description="Which rule it is an instance of."
    )
    severity: Severity = PydanticField(..., description="How much it matters.")
    message: str = PydanticField(..., description="What to tell the author.")
    source: Literal["merge", "structure"] = PydanticField(
        ..., description="Whether merge raised it, or this module found it."
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
        default=None, description="Exception type, for a finding merge raised."
    )


class MergeOutcome(ConfigBaseModel):
    """What a real merge attempt produced, or refused with."""

    status: Literal["merged", "refused", "not_attempted"] = PydanticField(
        ..., description="Whether merge ran, and how it ended."
    )
    error_type: str | None = PydanticField(default=None, description="Exception type.")
    message: str | None = PydanticField(
        default=None, description="The refusal, in full."
    )
    check: str | None = PydanticField(
        default=None, description="The rule that refused."
    )
    kinds: list[FindingKind] = PydanticField(
        default_factory=list,
        description=(
            "For a naming refusal, the kind of every problem it lists, in order."
        ),
    )
    completion: dict[str, Any] | None = PydanticField(
        default=None,
        description="The extension that would settle an incomplete refusal.",
    )
    vertices: int | None = PydanticField(
        default=None, description="Vertex count of the merged schema."
    )
    edges: int | None = PydanticField(
        default=None, description="Edge count of the merged schema."
    )
    version: str | None = PydanticField(
        default=None, description="Version of the merged schema."
    )


class MergePreview(ConfigBaseModel):
    """The declaration graph, everything wrong with it, and what merge did."""

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
    findings: list[MergeFinding] = PydanticField(default_factory=list)
    outcome: MergeOutcome = PydanticField(
        default_factory=lambda: MergeOutcome(status="not_attempted")
    )

    @property
    def refused(self) -> bool:
        """Whether the merge attempt refused."""
        return self.outcome.status == "refused"

    @property
    def blocking(self) -> list[MergeFinding]:
        """Findings that would stop a merge: the refusal and every possible one."""
        return [f for f in self.findings if f.severity != "note"]

    def node(self, node_id: str) -> PreviewNode | None:
        """The node with *node_id*, or ``None``."""
        return next((n for n in self.nodes if n.id == node_id), None)

    def attributes_of(self, node_id: str) -> list[PreviewNode]:
        """Attribute nodes owned by the class node *node_id*, in declared order."""
        return [n for n in self.nodes if n.owner == node_id]

    def edges_between(self, source: str, target: str) -> list[PreviewEdge]:
        """Every edge from node *source* to node *target*."""
        return [e for e in self.edges if e.source == source and e.target == target]

    def with_outcome(
        self, outcome: MergeOutcome, *, subjects: Sequence[str] = ()
    ) -> MergePreview:
        """A copy carrying *outcome*, and the finding a refusal becomes.

        The structural pass usually found the refusal too — it calls the same
        check — so the two are folded into one finding marked ``refusal``
        rather than listed twice. Reporting one problem as two would undercut
        the only number this is for: how many things are actually wrong.
        """
        if outcome.status != "refused":
            return self.model_copy(update={"outcome": outcome})

        known = {n.id for n in self.nodes}
        refusal = MergeFinding(
            kind=outcome.kinds[0]
            if outcome.kinds
            else kind_for_check(outcome.check, outcome.error_type),
            severity="refusal",
            message=outcome.message or "merge refused",
            source="merge",
            nodes=[s for s in subjects if s in known],
            completion=outcome.completion,
            check=outcome.check,
            error_type=outcome.error_type,
        )
        message = outcome.message or ""
        listed = {
            position
            for position, finding in enumerate(self.findings)
            if finding.severity == "possible"
            and finding.message
            and finding.message != message
            and finding.message in message
        }
        if listed:
            # A naming refusal that lists several problems: each one the
            # structural pass found is part of it, and none is reported twice.
            return self.model_copy(
                update={
                    "outcome": outcome,
                    "findings": [
                        finding.model_copy(
                            update={
                                "severity": "refusal",
                                "source": "merge",
                                "error_type": outcome.error_type,
                            }
                        )
                        if position in listed
                        else finding
                        for position, finding in enumerate(self.findings)
                    ],
                }
            )
        findings: list[MergeFinding] = []
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


def _is_same_problem(structural: MergeFinding, refusal: MergeFinding) -> bool:
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
    # The refusal usually names more -- it knows the merged names the
    # structural pass only reached one of -- so a subset is the same problem
    # seen with less context.
    return set(structural.nodes) <= set(refusal.nodes)


# ── classifying a refusal ───────────────────────────────────────────────────

#: The same table keyed by exception name, for the serialized
#: ``MergeOutcome.error_type``. Derived, never written by hand.
_KINDS_BY_TYPE_NAME: dict[str, frozenset[FindingKind]] = {
    exc.__name__: kinds for exc, kinds in _KINDS_BY_TYPE.items()
}


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
        kinds = _KINDS_BY_TYPE_NAME.get(error_type)
        if kinds:
            return min(kinds)
    return "disagreement"


def expected_kinds(outcome: MergeOutcome) -> frozenset[FindingKind]:
    """Structural finding kinds that should accompany *outcome*.

    The invariant this module is tested against: whatever merge refused, the
    structural pass saw something of a matching kind. An empty set means the
    refusal is one the preview is not asked to anticipate -- a structural merge
    error from deep inside the union, say -- and asserts nothing.
    """
    if outcome.status != "refused":
        return frozenset()
    if outcome.kinds:
        return frozenset(outcome.kinds)
    if outcome.check:
        return frozenset({kind_for_check(outcome.check, outcome.error_type)})
    by_type = _KINDS_BY_TYPE_NAME.get(outcome.error_type or "")
    return by_type if by_type is not None else frozenset()


def outcome_from_exception(
    exc: BaseException,
) -> tuple[MergeOutcome, tuple[str, ...]]:
    """The outcome a refusal is, and the node ids it names."""
    refusal = exc if isinstance(exc, Refusal) else None
    completion = _completion_of(exc)
    kinds: list[FindingKind] = (
        [cast(FindingKind, finding.kind) for finding in exc.findings]
        if isinstance(exc, MergeNamingError)
        else []
    )
    return (
        MergeOutcome(
            status="refused",
            error_type=type(exc).__name__,
            message=str(exc),
            check=(refusal.check if refusal is not None else "") or None,
            kinds=kinds,
            completion=completion.to_dict() if completion is not None else None,
        ),
        refusal.subjects if refusal is not None else (),
    )


def _completion_of(exc: BaseException) -> Completion | None:
    """The declaration that would settle *exc*, when it carries one."""
    if isinstance(exc, MergeNamingError | MergeIncompleteError):
        return exc.completion
    return None


def outcome_from_manifest(manifest: GraphManifest) -> MergeOutcome:
    """The outcome a merged manifest is."""
    schema = manifest.graph_schema
    if schema is None:
        return MergeOutcome(status="merged")
    core = schema.core_schema
    return MergeOutcome(
        status="merged",
        vertices=len(core.vertex_config.vertices),
        edges=len(core.edge_config.edges),
        version=str(schema.metadata.version) if schema.metadata.version else None,
    )


# ── the tolerant pass ───────────────────────────────────────────────────────


@dataclass
class _Builder:
    """Accumulates the declaration graph and its findings, refusing nothing.

    The naming checks are the ones merge runs, in
    :func:`~graflo.architecture.evolution.naming_graph.build_naming`, which
    reports every problem rather than the first. The schema-union checks
    delegate to the merge kernel one unit at a time, so a refusal on one
    group does not hide the next. What this class adds is the bookkeeping:
    which node a finding is about, and the graph it is a finding on.
    """

    op: MergeManifestsOp
    manifests: dict[Side, GraphManifest]
    names: dict[Side, SideNames]
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = ()
    declared: Any = None  # DeclaredMaps, set by the naming pass
    graph: NamingGraph | None = None
    nodes: dict[str, PreviewNode] = field(default_factory=dict)
    edges: dict[str, PreviewEdge] = field(default_factory=dict)
    clusters: list[PreviewCluster] = field(default_factory=list)
    findings: list[MergeFinding] = field(default_factory=list)
    index: ClusterIndex = field(
        default_factory=lambda: ClusterIndex(vertices=(), relations=())
    )
    composite: dict[Side, CanonicalizeOp] = field(default_factory=dict)

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
        scope: Literal["merged", "canonical"],
        owner: str,
        name: str,
        *,
        identity: bool = False,
        field_type: str | None = None,
    ) -> str:
        """An attribute row on a merged or canonical class, minting both."""
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
            MergeFinding(
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
        refusal = exc if isinstance(exc, Refusal) else None
        check = (refusal.check if refusal is not None else "") or None
        completion = _completion_of(exc)
        subjects = [
            s
            for s in (refusal.subjects if refusal is not None else ())
            if s in self.nodes
        ]
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

    def naming(self) -> None:
        """The naming graph, and every finding of the checker merge itself runs.

        :func:`~graflo.architecture.evolution.naming_graph.build_naming`
        never refuses: it reports every naming problem as a finding and
        lowers what it can, so the passes after this one still see groups and
        relabels to check.
        """
        result = build_naming(
            self.op,
            left=self.manifests["left"],
            right=self.manifests["right"],
            canonical_maps=self.canonical_maps,
        )
        resolution = result.resolution
        self.index = resolution.index
        self.declared = resolution.declared
        self.composite = {side: resolution.side_maps[side] for side in _SIDES}
        self.graph = result.graph
        for finding in result.findings:
            self.finding(
                cast(FindingKind, finding.kind),
                finding.message,
                severity="note" if not finding.blocking else "possible",
                nodes=[n for n in (self._node_for(s) for s in finding.subjects) if n],
                completion=finding.repairs[0].to_dict() if finding.repairs else None,
                check=finding.check or None,
            )
            if finding.blocking and finding.repairs:
                self._suggest(finding.repairs[0])
        self._record_clusters()
        self._rename_edges()
        self._property_edges()

    def _node_for(self, subject_id: str) -> str | None:
        """The node a finding's subject names: a ghost for an absent class, a merged name minted."""
        if subject_id in self.nodes:
            return subject_id
        scope, _, rest = subject_id.partition(":")
        name, _, attr = rest.partition(".")
        if attr:
            return None
        if scope in _SIDES:
            return self.ghost(scope, name)
        if scope == "merged":
            return self.add_node(PreviewNode(id=subject_id, kind="merged", name=name))
        return None

    def _record_clusters(self) -> None:
        """One :class:`PreviewCluster` per group, with an edge from each member.

        A declared member's edge is a ``member`` edge; a member the vocabulary
        joins is drawn with a ``map`` edge, so the closure is visible.
        """
        for kind, clusters in (
            ("vertex", self.index.vertices),
            ("relation", self.index.relations),
        ):
            for position, cluster in enumerate(clusters):
                composed_id = self.add_node(
                    PreviewNode(
                        id=subject("merged", cluster.into),
                        kind="merged",
                        name=cluster.into,
                    )
                )
                cluster_id = f"{kind}-cluster-{position}"
                declaration = cluster.declaration
                self.clusters.append(
                    PreviewCluster(
                        id=cluster_id,
                        kind=kind,  # type: ignore[arg-type]
                        into=cluster.into,
                        declared_into=cluster.declared_into,
                        left=list(cluster.left),
                        right=list(cluster.right),
                        synthesized=cluster.synthesized,
                        declared_identity=isinstance(declaration, VertexEquivalence)
                        and declaration.identity is not None,
                        derived_identity=isinstance(declaration, VertexEquivalence)
                        and declaration.has_derivation,
                    )
                )
                for side in _SIDES:
                    declared = set(cluster.declared_members(side))
                    for member in cluster.members(side):
                        member_id = subject(side, member)
                        if member_id not in self.nodes:
                            member_id = self.ghost(side, member, kind)  # type: ignore[arg-type]
                        if member in declared:
                            self.add_edge(
                                member_id, composed_id, "member", declared_by=cluster_id
                            )
                        else:
                            self.add_edge(
                                member_id,
                                composed_id,
                                "map",
                                label="vocabulary",
                                declared_by=side,
                            )

    def _rename_edges(self) -> None:
        """A ``map`` edge per vocabulary entry or rename of a class no group holds."""
        assert self.graph is not None
        for edge in self.graph.edges:
            if edge.kind not in ("vocabulary", "rename") or edge.target is None:
                continue
            if edge.source == edge.target:
                continue
            members = (
                self.index.vertex_members(edge.side)
                if edge.category == "vertex"
                else self.index.relation_members(edge.side)
            )
            if edge.source in members:
                continue
            labels = (
                self.index.labels
                if edge.category == "vertex"
                else self.index.relation_labels
            )
            scope = "merged" if edge.target in labels else "canonical"
            target_id = self.add_node(
                PreviewNode(
                    id=subject(scope, edge.target), kind=scope, name=edge.target
                )  # type: ignore[arg-type]
            )
            self.add_edge(
                subject(edge.side, edge.source),
                target_id,
                "map",
                label=edge.target,
                declared_by=edge.declared_by,
            )

    def _property_edges(self) -> None:
        """An attribute-level edge per property equivalence and per vocabulary attribute rename."""
        for cluster in self.index.vertices:
            for side in _SIDES:
                for member, attr_map in cluster.property_maps(side).items():
                    for old, new in attr_map.items():
                        self.add_edge(
                            subject(side, member, old),
                            self.attribute("merged", cluster.into, new),
                            "property_equivalence",
                            label=new,
                            declared_by=cluster.into,
                        )
        for side in _SIDES:
            cm: CanonicalMap = self.declared[side]
            for cls, attrs in cm.properties.items():
                if cls not in self.names[side].vertices:
                    continue
                owning = next(
                    (c for c in self.index.vertices if cls in c.members(side)), None
                )
                for old, new in attrs.items():
                    target = (
                        self.attribute("merged", owning.into, new)
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

    def _suggest(self, repair: Completion) -> None:
        """Draw the declaration a repair proposes."""
        for payload in (*repair.vertex_equivalences, *repair.relation_equivalences):
            into = payload.get("into")
            if not into:
                continue
            target = self.add_node(
                PreviewNode(id=subject("merged", into), kind="merged", name=into)
            )
            for side in _SIDES:
                members = payload.get(side) or []
                for member in [members] if isinstance(members, str) else members:
                    self.add_edge(
                        subject(side, member),
                        target,
                        "suggested",
                        label=_SUGGESTED_LABEL[repair.kind],
                        declared_by="completion",
                    )
        for side_name, fragment in repair.renames.items():
            side = cast(Side, side_name)
            for mapping in fragment.values():
                for old, new in mapping.items():
                    target = self.add_node(
                        PreviewNode(
                            id=subject("canonical", new), kind="canonical", name=new
                        )
                    )
                    self.add_edge(
                        subject(side, old),
                        target,
                        "suggested",
                        label=_SUGGESTED_LABEL[repair.kind],
                        declared_by="completion",
                    )

    def composed_attributes(self) -> None:
        """Fill in every merged class's attribute rows.

        The union of its members' properties under that side's composite
        rename -- which is what the merged vertex actually carries -- plus the
        attributes its derived identity branches add. Marked as identity when
        a declared branch keys on it, or, with nothing declared, when a member
        keys on it.
        """
        for cluster in self.index.vertices:
            if not cluster.into:
                continue
            declaration = cluster.declaration
            declared_identity: set[str] = set()
            derived: list[str] = []
            if isinstance(declaration, VertexEquivalence):
                declared_identity = {
                    f for fields in declaration.raw_branches() for f in fields
                }
                derived = [b.name for b in declaration.derived_branches()]
                local_key = declaration.local_key_branch()
                if local_key is not None:
                    derived.append(local_key.name)
                declared_identity.update(derived)
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
                            "merged",
                            cluster.into,
                            composed_name,
                            identity=(
                                composed_name in declared_identity
                                or (not declared_identity and prop.name in keyed)
                            ),
                            field_type=_type_label(prop),
                        )
            for name in derived:
                self.attribute("merged", cluster.into, name, identity=True)

    def identity_checks(self) -> None:
        """Members of one cluster no single key serves, unresolved or uncovered.

        The rules of
        :func:`~graflo.architecture.evolution.merge._composed_identity` and
        :func:`~graflo.architecture.evolution.merge._check_identity_coverage`:
        only plain natural keys take part in disagreement -- blank, assigned,
        hash and funnel identities are reconciled (or refused) by the vertex
        merge itself. A declared key of property branches must be one every
        member can complete; one with a derived branch is checked by its own
        lowering. A funnel must not meet a member declaring its
        ``digest_field``.
        """
        for cluster in self.index.vertices:
            declaration = cluster.declaration
            if not isinstance(declaration, VertexEquivalence):
                continue
            keys, properties = self._member_identity_state(cluster)
            if declaration.identity is not None:
                raw = declaration.raw_branches()
                if declaration.has_derivation or len(raw) > 1:
                    self._digest_field_collision(
                        cluster, declaration.digest_field, keys, properties
                    )
                if not declaration.has_derivation:
                    self._identity_coverage(
                        cluster,
                        list(raw[0])
                        if len(raw) == 1
                        else identity_branches_funnel(declaration.identity),
                        properties,
                    )
                continue
            if len({frozenset(k) for _s, _m, k in keys}) <= 1:
                continue
            detail = "; ".join(f"{s}:{m}={list(k)}" for s, m, k in keys)
            self.finding(
                "identity_disagreement",
                f"merged vertex {cluster.into!r} has members that disagree on "
                f"identity ({detail}) and nothing resolves it",
                nodes=[
                    subject("merged", cluster.into),
                    *(subject(s, m) for s, m, _k in keys),
                ],
            )

    def _digest_field_collision(
        self,
        cluster: Cluster,
        digest_field: str,
        keys: Sequence[tuple[Side, str, tuple[str, ...]]],
        properties: Mapping[tuple[Side, str], set[str]],
    ) -> None:
        colliding = [
            (side, member)
            for side, member, _key in keys
            if digest_field in properties.get((side, member), set())
        ]
        if colliding:
            self.finding(
                "identity_collision",
                f"merged vertex {cluster.into!r} is keyed on a funnel whose "
                f"digest is stored in `{digest_field}`, but "
                f"{', '.join(f'{s}:{m}' for s, m in colliding)} declare a "
                f"property `{digest_field}`",
                nodes=[
                    subject("merged", cluster.into),
                    *(subject(s, m) for s, m in colliding),
                ],
            )

    def _member_identity_state(
        self, cluster: Cluster
    ) -> tuple[
        list[tuple[Side, str, tuple[str, ...]]], dict[tuple[Side, str], set[str]]
    ]:
        """Each member's plain natural key and property names, in canonical names."""
        keys: list[tuple[Side, str, tuple[str, ...]]] = []
        properties: dict[tuple[Side, str], set[str]] = {}
        for side in _SIDES:
            schema = self.manifests[side].graph_schema
            if schema is None:
                continue
            vertex_config = schema.core_schema.vertex_config
            renames = self.composite[side].properties if side in self.composite else {}
            for member in cluster.members(side):
                if member not in vertex_config.vertex_set:
                    continue
                vertex = vertex_config[member]
                rename = renames.get(member, {})
                properties[(side, member)] = {
                    rename.get(f, f) for f in vertex.property_names
                }
                if (
                    vertex.blank
                    or vertex.assigned
                    or vertex.hash_identity_properties
                    or vertex.identity_funnel is not None
                ):
                    continue
                keys.append(
                    (side, member, tuple(rename.get(f, f) for f in vertex.identity))
                )
        return keys, properties

    def _identity_coverage(
        self,
        cluster: Cluster,
        identity: list[str] | IdentityFunnel,
        properties: Mapping[tuple[Side, str], set[str]],
    ) -> None:
        branches = (
            [set(branch.required_fields) for branch in identity.branches]
            if isinstance(identity, IdentityFunnel)
            else [set(identity)]
        )
        if isinstance(identity, IdentityFunnel):
            declared_anywhere = (
                set().union(*properties.values()) if properties else set()
            )
            unreachable = sorted(
                {f for branch in branches for f in branch - declared_anywhere}
            )
            if unreachable:
                self.finding(
                    "identity_coverage",
                    f"merged vertex {cluster.into!r} is keyed on a funnel with "
                    f"branches over {unreachable}, which no member declares",
                    nodes=[subject("merged", cluster.into)],
                )
        uncovered = [
            (side, member)
            for (side, member), declared in properties.items()
            if not any(branch <= declared for branch in branches)
        ]
        if uncovered:
            self.finding(
                "identity_coverage",
                f"merged vertex {cluster.into!r} is keyed on a field-set "
                f"{', '.join(f'{s}:{m}' for s, m in uncovered)} cannot complete, "
                "so their records would be dropped",
                nodes=[
                    subject("merged", cluster.into),
                    *(subject(s, m) for s, m in uncovered),
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

        The kernel takes the merged name as an argument and never reads
        ``Vertex.name``, so only the *attribute* renames have to be applied
        first -- the class relabel is irrelevant here. Applying them through
        the whole-side ``apply_canonicalize`` would be wrong twice over: it is
        all-or-nothing per side, and it runs this very kernel internally, so
        one bad cluster would abort every other cluster's finding.

        Merge reaches the kernel once more after the union, through
        ``_apply_derived_identities``; no case is known that refuses only
        there, and this pass would not see it if one appeared.
        """
        declared = self.op.field_types
        try:
            field_type_ops(
                declared,
                self.manifests,
                UnionNames.of(
                    self.manifests,
                    self.composite,
                    index=self.index,
                    name_conflict=self.op.name_conflict,
                ),
            )
        except FieldTypeDeclarationError as exc:
            self.from_refusal(exc, fallback="dangling")
        declared_vertices = declared.vertices if declared is not None else {}
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
                    relabelled = relabelled.model_copy(
                        update={
                            "properties": with_declared_types(
                                relabelled.properties,
                                declared_vertices.get(cluster.into),
                            )
                        }
                    )
                    members.append((side, member, relabelled))
            if len(members) < 2:
                continue
            try:
                merge_vertex_models(
                    [vertex for _s, _m, vertex in members],
                    cluster.into,
                    retype_remedy=MERGE_RETYPE_REMEDY,
                )
            except ValueError as exc:
                # Deliberately wider than the two typed errors: this module
                # describes a merge, it never raises one. An untyped refusal
                # from deeper in the kernel still earns a finding -- a general
                # one, since nothing says which rule it is -- rather than
                # taking the whole preview down with it.
                self.from_refusal(
                    exc,
                    fallback="disagreement",
                    nodes=self._merge_nodes(cluster, members, exc),
                )

    def _effective_map(self, side: Side, kind: Kind) -> dict[str, str]:
        """Where a name on *side* ends up, as the union sees it (see
        :func:`~graflo.architecture.evolution.merge_types.union_name_map`)."""
        return union_name_map(
            self.composite,
            self.names,
            index=self.index,
            name_conflict=self.op.name_conflict,
            side=side,
            kind=kind,
        )

    def edge_merge_checks(self) -> None:
        """Two declarations of one logical edge that the union cannot fold.

        Not scopeable to the relation clusters: ``_union_schema`` folds *every*
        edge of both sides by ``edge_id``, and two edges collide precisely
        because a **vertex** cluster renamed their endpoints onto one name. So
        this remaps both sides whole, groups by the resulting ``edge_id``, and
        folds each group on its own -- one unit at a time, so a group that
        refuses does not hide the next.
        """
        declared = self.op.field_types
        declared_edges = declared.edges if declared is not None else {}
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
                if remapped.relation is not None:
                    remapped = remapped.model_copy(
                        update={
                            "properties": with_declared_types(
                                remapped.properties,
                                declared_edges.get(remapped.relation),
                            )
                        }
                    )
                by_id.setdefault(remapped.edge_id, []).append((side, remapped))

        for edge_id, group in by_id.items():
            if len(group) < 2:
                continue
            folded = group[0][1]
            for _side, edge in group[1:]:
                try:
                    folded = merge_edge_pair(
                        folded, edge, retype_remedy=MERGE_RETYPE_REMEDY
                    )
                except ValueError as exc:  # never raise out of a preview
                    self.from_refusal(
                        exc,
                        fallback="edge_conflict",
                        nodes=[
                            subject("merged", str(name))
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

        A field conflict names the merged attribute and every member that
        declares it -- *including* members that left it untyped, which is the
        side an author most needs to see. Anything else names the classes.
        """
        nodes: list[str] = [subject("merged", cluster.into)]
        fields: tuple[str, ...] = getattr(exc, "fields", ())
        if not fields:
            return [*nodes, *(subject(s, m) for s, m, _v in members)]
        for name in fields:
            nodes.append(subject("merged", cluster.into, name))
            for side, member, vertex in members:
                if any(prop.name == name for prop in vertex.properties):
                    nodes.append(subject(side, member, name))
        return nodes

    def build(self) -> MergePreview:
        self.schema_nodes()
        self.naming()
        self.composed_attributes()
        self.identity_checks()
        self.merge_checks()
        self.edge_merge_checks()
        return MergePreview(
            left_name=_manifest_name(self.manifests["left"], "left"),
            right_name=_manifest_name(self.manifests["right"], "right"),
            name_conflict=self.op.name_conflict,
            nodes=list(self.nodes.values()),
            edges=list(self.edges.values()),
            clusters=self.clusters,
            findings=self.findings,
        )


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


class Merge3Preview(ConfigBaseModel):
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


def build_merge3_preview(
    result: Any, *, base: GraphManifest | None = None
) -> Merge3Preview:
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
        A :class:`Merge3Preview`. Never raises: a merge that could not complete
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
    return Merge3Preview(
        nodes=ordered,
        conflicts=len(result.conflicts),
        clean=result.clean,
        merged_hash=result.merged_hash,
        warnings=list(result.warnings),
    )


# ── the entry point ─────────────────────────────────────────────────────────


def preview_merge(
    left: GraphManifest,
    right: GraphManifest,
    op: MergeManifestsOp,
    *,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
    attempt: bool = True,
) -> MergePreview:
    """The declaration graph of a merge, and everything wrong with it.

    Walks *op*'s equivalences and canonical maps against *left* and *right*
    without refusing: each declaration, each map entry and each member is put
    through the same check
    :func:`~graflo.architecture.evolution.merge.merge_manifests` uses, one
    at a time, so a problem with one does not hide the rest. Every refusal
    becomes a ``possible`` finding naming the nodes it is about.

    With *attempt*, merge is then run for real and its result -- the merged
    schema's shape, or the one refusal it raised, with the completion that
    would settle it -- is recorded as the outcome and as a single ``refusal``
    finding. Set it to ``False`` to describe the declarations without merging.

    Args:
        left: The left manifest, in whatever vocabulary it is in.
        right: The right manifest.
        op: The merge op: equivalences (with their identities) and canonical maps.
        canonical_maps: Extra ``(side, map)`` pairs, folded into ``op``'s.
        attempt: Whether to run a real merge for the authoritative outcome.

    Returns:
        A :class:`MergePreview`. It never raises for a problem with the
        declarations -- that is the point -- so an empty
        :attr:`~MergePreview.blocking` is what "this would merge" looks
        like.
    """
    manifests: dict[Side, GraphManifest] = {"left": left, "right": right}
    names: dict[Side, SideNames] = {
        "left": SideNames.of(left),
        "right": SideNames.of(right),
    }
    preview = _Builder(
        op=op, manifests=manifests, names=names, canonical_maps=canonical_maps
    ).build()
    if not attempt:
        return preview
    outcome, subjects, notes = _attempt(left, right, op, canonical_maps)
    preview = preview.with_outcome(outcome, subjects=subjects)
    known = {n.id for n in preview.nodes}
    return preview.model_copy(
        update={
            "findings": [
                *preview.findings,
                *(
                    note.model_copy(
                        update={"nodes": [n for n in note.nodes if n in known]}
                    )
                    for note in notes
                ),
            ]
        }
    )


def _attempt(
    left: GraphManifest,
    right: GraphManifest,
    op: MergeManifestsOp,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]],
) -> tuple[MergeOutcome, tuple[str, ...], list[MergeFinding]]:
    """Merge for real, and turn whichever way it went into an outcome.

    A merge that went through may still have changed what a resource does:
    each resource it turned from upserting a merged class into referencing it
    writes none of those records any more, which is a ``note``. So is each
    member key demoted beside a funnel of several branches: it no longer
    deduplicates the member's own records.
    """
    from .alignment import AlignmentConflictError
    from .merge import (
        MergeIdentityError,
        MergeNameConflictError,
        _merge_manifests,
    )

    try:
        merged, report = _merge_manifests(
            left, right, op, canonical_maps=canonical_maps, finish_init=False
        )
    except (
        MergeCanonicalConflictError,
        MergeIdentityError,
        MergeNameConflictError,
        AlignmentConflictError,
        ValueError,
    ) as exc:
        return (*outcome_from_exception(exc), [])
    notes = [
        MergeFinding(
            kind="reference_conversion",
            severity="note",
            message=(
                f"resource {ref.resource!r} upserted {ref.vertex!r} but no derived "
                f"identity branch names it, so it now looks {ref.vertex!r} up by "
                f"{ref.key!r} and writes none of those records; add it to a "
                "derived branch's sources if its rows carry the inputs"
            ),
            source="merge",
            nodes=[
                subject("merged", ref.vertex),
                *(subject(ref.side, member) for member in ref.members),
            ],
        )
        for ref in report.converted
    ]
    notes.extend(_demotion_notes(report.demoted))
    return outcome_from_manifest(merged), (), notes


def _demotion_notes(demoted: Sequence[DemotedKey]) -> list[MergeFinding]:
    """A note per member key that no longer deduplicates the member's records.

    A record keys on the first funnel branch it completes. When a member's
    records can complete two or more branches, one entity seen once with an
    earlier branch's attribute and once without becomes two vertices -- which
    its own key, now a non-unique lookup, used to prevent.
    """
    return [
        MergeFinding(
            kind="lookup_demotion",
            severity="note",
            message=(
                f"records of {key.side}:{key.member} key on the first of "
                f"{list(key.branches)} they complete, so two with the same "
                f"{list(key.fields)} that complete different ones become two "
                f"{key.vertex!r} vertices; {list(key.fields)} is now the "
                f"lookup-only secondary {key.secondary!r} and no longer "
                "deduplicates them"
            ),
            source="merge",
            nodes=[subject("merged", key.vertex), subject(key.side, key.member)],
        )
        for key in demoted
        if len(key.branches) >= 2
    ]


__all__ = [
    "EdgeKind",
    "FindingKind",
    "Merge3Preview",
    "MergeFinding",
    "MergeOutcome",
    "MergePreview",
    "NodeKind",
    "PreviewCluster",
    "PreviewEdge",
    "PreviewNode",
    "Severity",
    "SlotNode",
    "build_merge3_preview",
    "expected_kinds",
    "kind_for_check",
    "outcome_from_exception",
    "outcome_from_manifest",
    "preview_merge",
]
