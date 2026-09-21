"""Planning what to do about declared inverses: realize, repair, switch, withdraw.

Pure planners in the manner of :mod:`graflo.architecture.evolution.state_core.plan`:
each reads a manifest, emits primitive :data:`~graflo.architecture.evolution.ops.ManifestOp`
values and applies nothing. The op list is the reviewable artifact, ``apply_evolution``
applies it, ``invert_ops`` undoes it, and a commit records exactly those
primitives -- so a plan replays without this module.

What a planner adds over calling the ops by hand is the part that is awkward by
hand: deciding *which* relations an op may name, in which order the ops must
run, and saying what was left out and why. Each plan is checked against a copy
of the manifest before it is returned, so an op that would be refused is
reported as skipped rather than handed to the caller to trip over.

The line the planners hold is the one the audit draws
(:func:`graflo.architecture.profile.inverses.audit_inverses`): what one place
merely under-reports is propagated, and what two places contradict is listed
and left alone.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.steps.ref import EdgeStepRef
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.profile.inverses import (
    InverseReport,
    PairStatus,
    audit_inverses,
)
from graflo.architecture.schema.edge_direction import (
    REVERSE_TRAVERSAL_COST,
    ReverseTraversalCost,
    coerce_db_type,
)
from graflo.architecture.schema.inverse_realization import (
    InverseFinding,
    native_inverse_violations,
)

from .apply import apply_manifest_ops_inplace
from .ops import (
    AddEdgePropertiesOp,
    AddInverseEdgesOp,
    DeclareEdgeInversesOp,
    EdgeSelector,
    ManifestOp,
    RemoveEdgesOp,
    SetEdgeDirectedOp,
    SetInverseEmissionOp,
    SetNativeInversesOp,
)

RealizeStrategy = Literal["auto", "native", "materialized"]
Realization = Literal["native", "materialized"]
"""The two ways a declared pair can be stored. A pair with neither is simply
declared, which is the default and needs nothing."""

#: The order repairs run in. Directedness first, because a declaration that the
#: edges contradict keeps the schema from loading; then what the schema lacks;
#: then what the pipelines lack.
_REPAIR_ORDER = (
    "symmetric_on_directed",
    "undirected_not_symmetric",
    "partial_endpoint_pairs",
    "property_drift",
    "unfed_inverse",
    "idle_emit_inverse",
)


class Skipped(ConfigBaseModel):
    """One relation a plan left alone, and why."""

    relation: str
    code: str = PydanticField(..., description="Stable identifier of the reason.")
    reason: str


class InversePlan(ConfigBaseModel):
    """Ops that change how declared inverses are realized, with what was left out."""

    ops: list[ManifestOp] = PydanticField(default_factory=list)
    selected: list[str] = PydanticField(
        default_factory=list, description="Relations the ops act on."
    )
    skipped: list[Skipped] = PydanticField(default_factory=list)
    before: InverseReport = PydanticField(
        default_factory=InverseReport,
        description="The audit the plan was made from.",
    )
    after: InverseReport = PydanticField(
        default_factory=InverseReport,
        description="The audit of the manifest with the ops applied.",
    )

    @property
    def remaining(self) -> list[InverseFinding]:
        """Conflicts and unrepaired findings still present once the ops are applied."""
        return [f for f in self.after.findings if f.severity != "note"]

    def to_lines(self) -> list[str]:
        """The plan as text: what it does, what it skipped, what is left."""
        lines = [f"{len(self.ops)} op(s)"]
        lines += [f"  {op.op}: {_summary(op)}" for op in self.ops]
        if self.skipped:
            lines += ["", f"skipped ({len(self.skipped)}):"]
            lines += [f"  - {s.relation}: [{s.code}] {s.reason}" for s in self.skipped]
        if self.remaining:
            lines += ["", f"left untouched ({len(self.remaining)}):"]
            lines += [
                f"  - [{f.severity}/{f.kind}] {f.message}" for f in self.remaining
            ]
        return lines


def _summary(op: ManifestOp) -> str:
    if isinstance(op, (SetNativeInversesOp, AddInverseEdgesOp)):
        relations = op.relations if op.relations is not None else ["<every pair>"]
        suffix = "" if getattr(op, "enabled", True) else " (withdraw)"
        return ", ".join(relations) + suffix
    if isinstance(op, SetInverseEmissionOp):
        where = ", ".join(
            f"{name}:{ref}" for name, refs in op.steps.items() for ref in refs
        )
        return where + ("" if op.enabled else " (clear)")
    if isinstance(op, DeclareEdgeInversesOp):
        return ", ".join(
            [*op.symmetric, *(f"{a}<->{b}" for a, b in op.inverses.items())]
        )
    if isinstance(op, (SetEdgeDirectedOp, RemoveEdgesOp)):
        return ", ".join(str(selector.edge_id()) for selector in op.edges)
    if isinstance(op, AddEdgePropertiesOp):
        return ", ".join(f"{r}: {op.field_names(r)}" for r in op.additions)
    return ""


# ---------------------------------------------------------------------------
# Shared machinery
# ---------------------------------------------------------------------------


def _selectors(edge_ids: Iterable[EdgeId]) -> list[EdgeSelector]:
    return [
        EdgeSelector(source=source, target=target, relation=relation)
        for source, target, relation in edge_ids
    ]


def _try(manifest: GraphManifest, ops: Sequence[ManifestOp]) -> str | None:
    """Apply *ops* to *manifest* in place; the refusal if there is one.

    *manifest* is a working copy the planner owns. On refusal it may be left
    half-changed, so the caller re-copies before going on.
    """
    try:
        apply_manifest_ops_inplace(manifest, list(ops))
    except (ValueError, TypeError) as exc:
        return str(exc)
    return None


def _pairs_for(
    report: InverseReport, relations: Sequence[str] | None
) -> tuple[list[tuple[PairStatus, str | None]], list[Skipped]]:
    """The pairs a plan acts on, each with the relation the caller named, if any."""
    if relations is None:
        return [(pair, None) for pair in report.pairs], []
    chosen: list[tuple[PairStatus, str | None]] = []
    skipped: list[Skipped] = []
    for name in dict.fromkeys(relations):
        pair = report.pair(name)
        if pair is None:
            reason = (
                "is symmetric: it is its own inverse, and its edges being "
                "undirected is its realization"
                if name in report.symmetric
                else "has no declared inverse; declare the pair first "
                "(declare_edge_inverses)"
            )
            skipped.append(Skipped(relation=name, code="undeclared", reason=reason))
        elif all(pair is not other for other, _named in chosen):
            chosen.append((pair, name))
    return chosen, skipped


def _stored_forward(pair: PairStatus, named: str | None) -> str | None:
    """The relation of *pair* to treat as the stored, forward side."""
    if named is not None and named in pair.stored_sides:
        return named
    return pair.stored_sides[0] if pair.stored_sides else None


def _mirror_edges(manifest: GraphManifest, forward: str, inverse: str) -> list[EdgeId]:
    """Declared edges ``(T, S, inverse)`` that mirror a declared ``(S, T, forward)``."""
    assert manifest.graph_schema is not None
    edges = manifest.graph_schema.core_schema.edge_config.edges
    forward_ids = {e.edge_id for e in edges if e.relation == forward}
    return [
        e.edge_id
        for e in edges
        if e.relation == inverse and (e.target, e.source, forward) in forward_ids
    ]


def _finish(
    manifest: GraphManifest,
    before: InverseReport,
    candidates: list[tuple[str, list[ManifestOp]]],
    skipped: list[Skipped],
) -> InversePlan:
    """Check each candidate against a working copy and assemble the plan."""
    working = manifest.model_copy(deep=True)
    ops: list[ManifestOp] = []
    selected: list[str] = []
    for relation, candidate in candidates:
        trial = working.model_copy(deep=True)
        refusal = _try(trial, candidate)
        if refusal is not None:
            skipped.append(Skipped(relation=relation, code="refused", reason=refusal))
            continue
        working = trial
        ops += candidate
        selected.append(relation)
    return InversePlan(
        ops=_merged(ops),
        selected=selected,
        skipped=skipped,
        before=before,
        after=audit_inverses(working),
    )


def _merged(ops: list[ManifestOp]) -> list[ManifestOp]:
    """Adjacent ops of one kind folded into one, where that cannot change the outcome.

    Naming ten relations in one ``set_native_inverses`` reads better than ten
    ops, and each was already checked on its own.
    """
    out: list[ManifestOp] = []
    for op in ops:
        last = out[-1] if out else None
        if (
            isinstance(op, SetNativeInversesOp)
            and isinstance(last, SetNativeInversesOp)
            and op.enabled == last.enabled
        ):
            out[-1] = SetNativeInversesOp(
                relations=[*last.relations, *op.relations], enabled=op.enabled
            )
        elif (
            isinstance(op, AddInverseEdgesOp)
            and isinstance(last, AddInverseEdgesOp)
            and op.relations is not None
            and last.relations is not None
        ):
            out[-1] = AddInverseEdgesOp(relations=[*last.relations, *op.relations])
        else:
            out.append(op)
    return out


# ---------------------------------------------------------------------------
# Realize
# ---------------------------------------------------------------------------


def _auto_target(manifest: GraphManifest) -> tuple[Realization | None, str]:
    """What ``auto`` picks for this manifest's target, and the mechanism behind it."""
    assert manifest.graph_schema is not None
    flavor = manifest.graph_schema.db_profile.db_flavor
    cost = REVERSE_TRAVERSAL_COST.get(coerce_db_type(flavor))
    if cost is ReverseTraversalCost.SCHEMA_TIME_ONLY:
        return "native", (
            f"reverse reads on {str(flavor)!r} are decided when the edge type is "
            "created, so the database maintains the inverse"
        )
    if cost is ReverseTraversalCost.MATERIALIZATION_REQUIRED:
        return "materialized", (
            f"direction is the storage key on {str(flavor)!r}, so the reverse view "
            "has to be written"
        )
    label = cost.value if cost is not None else "unknown"
    return None, (
        f"a reverse read on {str(flavor)!r} is {label}: the inverse name already "
        "resolves to a reverse traversal, and storing it would only duplicate edges"
    )


def plan_realize_inverses(
    manifest: GraphManifest,
    *,
    strategy: RealizeStrategy = "auto",
    relations: Sequence[str] | None = None,
) -> InversePlan:
    """Ops that realize declared pairs, and every pair left as it is with the reason.

    Args:
        manifest: The manifest to plan against; not changed.
        strategy: ``native`` has the database maintain each eligible pair,
            ``materialized`` stores the inverse as declared edges fed by the same
            rows, and ``auto`` picks per target backend from what a reverse read
            costs there -- which, on most backends, is *nothing*, so ``auto``
            realizes nothing, leaves the pair declared, and says so.
        relations: Restrict to the pairs these relations belong to (either
            side). Omitted: every declared pair.

    A pair is never realized two ways: one that is already realized the other
    way is skipped, and :func:`plan_switch_realization` moves it.
    """
    before = audit_inverses(manifest)
    if manifest.graph_schema is None:
        return InversePlan(before=before, after=before)
    chosen, skipped = _pairs_for(before, relations)
    target, why = _auto_target(manifest) if strategy == "auto" else (strategy, "")

    candidates: list[tuple[str, list[ManifestOp]]] = []
    for pair, named in chosen:
        forward = _stored_forward(pair, named)
        label = named or forward or pair.relation
        if pair.state == "conflicting":
            kinds = sorted(
                {
                    f.kind
                    for f in before.conflicts()
                    if set(f.relations) & {pair.relation, pair.inverse}
                }
            )
            skipped.append(
                Skipped(
                    relation=label,
                    code="conflicting",
                    reason=f"the pair has unresolved conflicts: {kinds}",
                )
            )
        elif forward is None:
            skipped.append(
                Skipped(
                    relation=label,
                    code="no_edge",
                    reason="neither relation of the pair labels a declared edge",
                )
            )
        elif target is None and pair.state == "declared":
            skipped.append(
                Skipped(relation=label, code="declaration_suffices", reason=why)
            )
        elif target is None:
            # `auto` never withdraws what is already stored: whoever stored it may
            # have had a reason the backend's read cost does not show.
            skipped.append(
                Skipped(
                    relation=label,
                    code="stored_not_needed",
                    reason=(
                        f"is already {pair.state}, which this backend does not "
                        f"need ({why}); left as it is -- withdrawing the "
                        "realization (plan_withdraw_realization; `graflo inverses "
                        "withdraw`) drops the stored inverse and keeps the "
                        "declaration"
                    ),
                )
            )
        elif target == "native":
            candidates += _native_candidates(pair, label, skipped)
        else:
            candidates += _materialized_candidates(pair, forward, label, skipped)
    return _finish(manifest, before, candidates, skipped)


def _native_candidates(
    pair: PairStatus, label: str, skipped: list[Skipped]
) -> list[tuple[str, list[ManifestOp]]]:
    if pair.state == "native":
        skipped.append(
            Skipped(
                relation=label,
                code="already_native",
                reason=f"the database already maintains it ({pair.native_side!r})",
            )
        )
        return []
    if pair.state in ("materialized", "partial"):
        skipped.append(
            Skipped(
                relation=label,
                code="materialized",
                reason=(
                    "the inverse is stored as declared edges; a pair is realized "
                    "one way, so switch it (plan_switch_realization; `graflo inverses switch`) rather than "
                    "adding a second"
                ),
            )
        )
        return []
    violations = pair.native_eligibility or []
    if violations:
        skipped.extend(
            Skipped(relation=label, code=v.code, reason=v.message) for v in violations
        )
        return []
    assert pair.native_candidate is not None
    return [(label, [SetNativeInversesOp(relations=[pair.native_candidate])])]


def _materialized_candidates(
    pair: PairStatus, forward: str, label: str, skipped: list[Skipped]
) -> list[tuple[str, list[ManifestOp]]]:
    if pair.state == "native":
        skipped.append(
            Skipped(
                relation=label,
                code="native",
                reason=(
                    "the database maintains the inverse; a pair is realized one "
                    "way, so switch it (plan_switch_realization; `graflo inverses switch`) rather than "
                    "adding a second"
                ),
            )
        )
        return []
    if pair.state == "materialized":
        skipped.append(
            Skipped(
                relation=label,
                code="already_materialized",
                reason=(
                    "every edge of the pair already has its mirror; what a "
                    "resource fails to feed is a repair (plan_repair_inverses; `graflo inverses repair`)"
                ),
            )
        )
        return []
    return [(label, [AddInverseEdgesOp(relations=list(pair.stored_sides))])]


# ---------------------------------------------------------------------------
# Repair
# ---------------------------------------------------------------------------


def _repair_ops(finding: InverseFinding) -> list[ManifestOp]:
    """The ops that propagate what *finding* says is missing."""
    if finding.kind == "symmetric_on_directed":
        return [SetEdgeDirectedOp(edges=_selectors(finding.edges), directed=False)]
    if finding.kind == "undirected_not_symmetric":
        return [DeclareEdgeInversesOp(symmetric=list(finding.relations))]
    if finding.kind == "partial_endpoint_pairs":
        return [AddInverseEdgesOp(relations=[finding.relations[0]])]
    if finding.kind == "property_drift":
        additions = {
            relation: list(names)
            for relation, names in finding.detail.get("missing", {}).items()
            if names
        }
        return [AddEdgePropertiesOp(additions=additions)] if additions else []
    if finding.kind in ("unfed_inverse", "idle_emit_inverse"):
        steps: dict[str, list[EdgeStepRef]] = {}
        for address in finding.steps:
            steps.setdefault(address.resource, []).append(
                EdgeStepRef(at=address.at, step=address.step, link=address.link)
            )
        if not steps:
            return []
        return [
            SetInverseEmissionOp(steps=steps, enabled=finding.kind == "unfed_inverse")
        ]
    return []


def plan_repair_inverses(manifest: GraphManifest) -> InversePlan:
    """Ops that propagate what the manifest under-reports; contradictions are left alone.

    Works through the ``repairable`` findings of the audit in a fixed order, and
    keeps a repair only if, applied to a working copy, the finding it answers is
    gone and no new finding has appeared. A repair that is refused, or that
    trades one finding for another, is reported as skipped. Conflicts are never
    touched: they are in ``remaining``.

    The manifest need not load -- one assembled by a merge often does not --
    see :func:`graflo.architecture.profile.inverses.manifest_for_audit`.
    """
    before = audit_inverses(manifest)
    working = manifest.model_copy(deep=True)
    ops: list[ManifestOp] = []
    selected: list[str] = []
    skipped: list[Skipped] = []

    for kind in _REPAIR_ORDER:
        for finding in [f for f in before.repairable() if f.kind == kind]:
            current = audit_inverses(working)
            if finding.key not in {f.key for f in current.findings}:
                continue  # an earlier repair already took care of it
            label = finding.relations[0] if finding.relations else kind
            candidate = _repair_ops(finding)
            if not candidate:
                continue
            trial = working.model_copy(deep=True)
            refusal = _try(trial, candidate)
            if refusal is not None:
                skipped.append(Skipped(relation=label, code="refused", reason=refusal))
                continue
            after = audit_inverses(trial)
            introduced = [
                f for f in after.introduced_since(current) if f.severity != "note"
            ]
            still_there = finding.key in {f.key for f in after.findings}
            if still_there or introduced:
                skipped.append(
                    Skipped(
                        relation=label,
                        code="not_a_clean_repair",
                        reason=(
                            f"propagating [{finding.kind}] "
                            + (
                                "left the finding in place"
                                if still_there
                                else "introduced "
                                + ", ".join(sorted({f.kind for f in introduced}))
                            )
                        ),
                    )
                )
                continue
            working = trial
            ops += candidate
            selected.append(label)

    return InversePlan(
        ops=ops,
        selected=list(dict.fromkeys(selected)),
        skipped=skipped,
        before=before,
        after=audit_inverses(working),
    )


# ---------------------------------------------------------------------------
# Switch and withdraw
# ---------------------------------------------------------------------------


def _subject(
    pair: PairStatus, named: str | None, skipped: list[Skipped]
) -> tuple[str, str] | None:
    """``(label, forward relation)`` of a pair a plan may act on, or None with the reason recorded."""
    forward = _stored_forward(pair, named)
    label = named or forward or pair.relation
    if forward is None:
        skipped.append(
            Skipped(
                relation=label,
                code="no_edge",
                reason="neither relation of the pair labels a declared edge",
            )
        )
        return None
    if pair.state == "conflicting":
        skipped.append(
            Skipped(
                relation=label,
                code="conflicting",
                reason="the pair has unresolved conflicts; settle them first",
            )
        )
        return None
    return label, forward


def _withdrawal(
    manifest: GraphManifest, pair: PairStatus, forward: str
) -> tuple[str, list[ManifestOp]]:
    """The relation that stays stored, and the ops that undo the pair's realization."""
    if pair.state == "native":
        assert pair.native_side is not None
        return pair.native_side, [
            SetNativeInversesOp(relations=[pair.native_side], enabled=False)
        ]
    inverse = pair.inverse if forward == pair.relation else pair.relation
    mirrors = _mirror_edges(manifest, forward, inverse)
    return forward, [RemoveEdgesOp(edges=_selectors(mirrors))] if mirrors else []


def plan_switch_realization(
    manifest: GraphManifest, relations: Sequence[str], *, to: Realization
) -> InversePlan:
    """Ops that move pairs from the realization they have to ``to``.

    A pair is realized one way, so switching is withdraw-then-add, in that
    order. Each relation named is the side that stays stored: switching
    ``employed_by`` to ``native`` removes the declared ``employs`` edges and has
    the database maintain them instead. Eligibility for ``native`` is checked
    *as the schema will be after the withdrawal*, before anything is planned, so
    a pair is never left withdrawn and unrealized. A pair that is only declared
    has nothing to withdraw, so switching it is the same as realizing it.
    """
    before = audit_inverses(manifest)
    if manifest.graph_schema is None:
        return InversePlan(before=before, after=before)
    chosen, skipped = _pairs_for(before, relations)
    schema = manifest.graph_schema

    candidates: list[tuple[str, list[ManifestOp]]] = []
    for pair, named in chosen:
        subject = _subject(pair, named, skipped)
        if subject is None:
            continue
        label, forward = subject
        if pair.state == to:
            skipped.append(
                Skipped(relation=label, code="already", reason=f"already {to}")
            )
            continue
        withdraw: list[ManifestOp] = []
        if pair.state == "native" or (
            pair.state in ("materialized", "partial") and to != "materialized"
        ):
            forward, withdraw = _withdrawal(manifest, pair, forward)

        add: list[ManifestOp] = []
        if to == "native":
            trial = manifest.model_copy(deep=True)
            refusal = _try(trial, withdraw)
            violations = (
                []
                if refusal is not None or trial.graph_schema is None
                else native_inverse_violations(
                    trial.graph_schema.db_profile,
                    trial.graph_schema.core_schema.edge_config,
                    {v.name for v in schema.core_schema.vertex_config.vertices},
                    candidates=[forward],
                )
            )
            if violations:
                skipped.extend(
                    Skipped(relation=label, code=v.code, reason=v.message)
                    for v in violations
                    if v.relation == forward
                )
                continue
            add.append(SetNativeInversesOp(relations=[forward]))
        else:
            add.append(AddInverseEdgesOp(relations=[forward]))
        candidates.append((label, [*withdraw, *add]))
    return _finish(manifest, before, candidates, skipped)


def plan_withdraw_realization(
    manifest: GraphManifest, relations: Sequence[str]
) -> InversePlan:
    """Ops that stop storing the inverse of pairs, keeping their declaration.

    The reverse of realizing: a native inverse is handed back to nothing, and the
    declared edges of a materialized inverse are removed -- which also clears the
    ``emit_inverse`` flags that fed them. Each relation named is the side that
    stays stored. The pair remains in ``edge_config.inverses``, so the inverse
    name keeps resolving on reads wherever the backend can follow an edge from
    its target.
    """
    before = audit_inverses(manifest)
    if manifest.graph_schema is None:
        return InversePlan(before=before, after=before)
    chosen, skipped = _pairs_for(before, relations)

    candidates: list[tuple[str, list[ManifestOp]]] = []
    for pair, named in chosen:
        subject = _subject(pair, named, skipped)
        if subject is None:
            continue
        label, forward = subject
        if pair.state == "declared":
            skipped.append(
                Skipped(
                    relation=label,
                    code="already",
                    reason="nothing realizes the pair; it is only declared",
                )
            )
            continue
        _forward, withdraw = _withdrawal(manifest, pair, forward)
        if withdraw:
            candidates.append((label, withdraw))
    return _finish(manifest, before, candidates, skipped)


# ---------------------------------------------------------------------------
# Symmetric
# ---------------------------------------------------------------------------


def plan_declare_symmetric(
    manifest: GraphManifest, relations: Sequence[str]
) -> InversePlan:
    """Ops that make relations symmetric: their edges undirected, then the declaration.

    ``directed: false`` and ``symmetric`` state one fact at two granularities and
    the schema refuses either without the other, so they are two ops in a fixed
    order. Keeping them two ops -- rather than one op that does both -- is what
    lets each be undone exactly.
    """
    before = audit_inverses(manifest)
    if manifest.graph_schema is None:
        return InversePlan(before=before, after=before)
    edge_config = manifest.graph_schema.core_schema.edge_config
    skipped: list[Skipped] = []
    candidates: list[tuple[str, list[ManifestOp]]] = []
    for name in dict.fromkeys(relations):
        edges = [edge for edge in edge_config.edges if edge.relation == name]
        if not edges:
            skipped.append(
                Skipped(relation=name, code="no_edge", reason="labels no declared edge")
            )
            continue
        if before.pair(name) is not None:
            skipped.append(
                Skipped(
                    relation=name,
                    code="paired",
                    reason=(
                        "has a declared inverse; a relation is either paired with "
                        "another or its own inverse (retract_edge_inverses first)"
                    ),
                )
            )
            continue
        ops: list[ManifestOp] = []
        directed = [edge.edge_id for edge in edges if edge.directed]
        if directed:
            ops.append(SetEdgeDirectedOp(edges=_selectors(directed), directed=False))
        if name not in edge_config.symmetric:
            ops.append(DeclareEdgeInversesOp(symmetric=[name]))
        if not ops:
            skipped.append(
                Skipped(relation=name, code="already", reason="already symmetric")
            )
            continue
        candidates.append((name, ops))
    return _finish(manifest, before, candidates, skipped)


__all__ = [
    "InversePlan",
    "Realization",
    "RealizeStrategy",
    "Skipped",
    "plan_declare_symmetric",
    "plan_realize_inverses",
    "plan_repair_inverses",
    "plan_switch_realization",
    "plan_withdraw_realization",
]
