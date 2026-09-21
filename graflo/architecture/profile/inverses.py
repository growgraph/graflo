"""Audit of declared inverses across the whole manifest, and its ``inverses`` profile.

:mod:`graflo.architecture.schema.inverse_realization` says how each declared
pair is realized *in the schema*. That is half the picture: a materialized
inverse is an edge somebody has to feed, and whether every resource that writes
the forward relation also feeds the inverse is a question about pipelines.
:func:`audit_inverses` answers both halves in one report.

The split that matters in the findings is between what one side merely
*under-reports* and what two sides *contradict*:

``repairable``
    The manifest states a fact in one place and omits it in another -- the
    inverse edge exists but a resource writing the forward relation feeds
    nothing into it; one mirror declares a property the other lacks; a relation
    is undirected everywhere but not declared symmetric. Propagating the fact
    cannot change meaning, so a planner may do it and say what it did.
``conflict``
    The two places disagree -- a pair read from the same side, mirrors keyed
    differently, a native inverse whose reverse name is taken. Only the author
    knows which is right; these are listed and never touched.

Nothing here raises, and nothing needs the manifest to have been through
``finish_init``: a manifest produced by a merge that no longer loads is exactly
the one worth auditing. A schema runs its cross-checks while it is validated,
so such a manifest cannot be built the usual way; :func:`manifest_for_audit`
builds it block by block instead.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.model import IngestionModel
from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.ingestion.steps.ref import (
    EdgeStepView,
    iter_edge_steps,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.profile.context import CheckContext
from graflo.architecture.profile.model import AssertionResult, Finding, roll_up
from graflo.architecture.profile.runner import Assertion, Profile
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import EdgeConfig, inverse_map
from graflo.architecture.schema.inverse_realization import (
    InverseFinding,
    NativeInverseViolation,
    PairRealization,
    StepAddress,
    native_inverse_violations,
    pair_realizations,
    schema_inverse_findings,
)
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import VertexConfig

PROFILE_VERSION = "1"

Feeding = Literal["emit_inverse", "step", "inference", "none"]
"""How one resource feeds a materialized inverse: by mirroring its forward
steps, by a step of its own, by edge inference, or not at all."""


class PairStatus(PairRealization):
    """A declared pair across schema and ingestion."""

    feeding: dict[str, Feeding] = PydanticField(
        default_factory=dict,
        description=(
            "Per resource that writes either relation of the pair: how it feeds "
            "the materialized inverse. Empty when nothing is materialized."
        ),
    )
    native_eligibility: list[NativeInverseViolation] | None = PydanticField(
        default=None,
        description=(
            "Rules that would be broken if the database maintained this pair; "
            "empty means eligible. None when the pair is already native."
        ),
    )
    native_candidate: str | None = PydanticField(
        default=None,
        description=(
            "The relation that would be listed in `native_inverses`: the one "
            "that has edges while its inverse has none."
        ),
    )


class InverseReport(ConfigBaseModel):
    """Everything known about how a manifest realizes its declared inverses."""

    pairs: list[PairStatus] = PydanticField(default_factory=list)
    symmetric: list[str] = PydanticField(default_factory=list)
    findings: list[InverseFinding] = PydanticField(default_factory=list)

    def repairable(self) -> list[InverseFinding]:
        """Findings a planner may fix by propagation."""
        return [f for f in self.findings if f.severity == "repairable"]

    def conflicts(self) -> list[InverseFinding]:
        """Findings only the author can settle."""
        return [f for f in self.findings if f.severity == "conflict"]

    def notes(self) -> list[InverseFinding]:
        return [f for f in self.findings if f.severity == "note"]

    def pair(self, relation: str) -> PairStatus | None:
        """The status of the pair that ``relation`` belongs to, either side."""
        return next(
            (p for p in self.pairs if relation in (p.relation, p.inverse)), None
        )

    def introduced_since(self, before: InverseReport) -> list[InverseFinding]:
        """Findings of this report that *before* did not have.

        ``audit_inverses(after).introduced_since(audit_inverses(before))`` is what
        a change did to the inverses, whatever produced the change.
        """
        known = {finding.key for finding in before.findings}
        return [finding for finding in self.findings if finding.key not in known]

    def to_lines(self) -> list[str]:
        """The report as text. The one renderer."""
        lines: list[str] = []
        for pair in self.pairs:
            # A mirror count only says something once the inverse is stored as edges.
            stored = pair.state in ("materialized", "partial")
            counts = f" ({pair.mirrored}/{pair.total} mirrored)" if stored else ""
            lines.append(f"{pair.relation} <-> {pair.inverse}: {pair.state}{counts}")
            for resource, feeding in sorted(pair.feeding.items()):
                lines.append(f"    fed in {resource}: {feeding}")
        for name in self.symmetric:
            lines.append(f"{name}: symmetric")
        if not self.pairs and not self.symmetric:
            lines.append("no declared inverses")
        for severity in ("conflict", "repairable", "note"):
            group = [f for f in self.findings if f.severity == severity]
            if not group:
                continue
            lines.append("")
            lines.append(f"{severity} ({len(group)}):")
            for finding in group:
                lines.append(f"  - [{finding.kind}] {finding.message}")
                for step in finding.steps:
                    lines.append(f"      at {step}")
        return lines


# ---------------------------------------------------------------------------
# Ingestion side
# ---------------------------------------------------------------------------


def _address(resource: ResourceConfig, view: EdgeStepView) -> StepAddress:
    return StepAddress(
        resource=resource.name,
        at=list(view.ref.at),
        step=view.ref.step,
        link=view.ref.link,
    )


def _may_write(view: EdgeStepView, relation: str) -> bool:
    """Whether the step can write ``relation`` (open-ended steps can write anything)."""
    written = view.relations_written()
    return written is None or relation in written


def _endpoints_may_match(view: EdgeStepView, edge_id: EdgeId) -> bool:
    """Whether the step's endpoints are, or could resolve to, those of ``edge_id``."""
    source, target, _relation = edge_id
    return view.source in (None, source) and view.target in (None, target)


def _inference_feeds(resource: ResourceConfig, edge_id: EdgeId) -> bool:
    """Whether edge inference in *resource* would write ``edge_id``.

    Mirrors the runtime's rule: inference is on, the edge passes the resource's
    ``infer_edge_only`` / ``infer_edge_except`` selectors, and -- unless an
    allow-list is given -- no fixed-endpoint step already owns the pair.
    """
    if not resource.infer_edges:
        return False
    if resource.infer_edge_only:
        return any(spec.matches(edge_id) for spec in resource.infer_edge_only)
    if any(spec.matches(edge_id) for spec in resource.infer_edge_except):
        return False
    source, target, _relation = edge_id
    for view in iter_edge_steps(resource.pipeline):
        if view.source is None or view.target is None:
            continue
        owned = {(view.source, view.target)}
        if view.emit_inverse:
            owned.add((view.target, view.source))
        if (source, target) in owned:
            return False
    return True


def _feeding(
    resource: ResourceConfig,
    forward_ids: list[EdgeId],
    inverse: str,
) -> tuple[Feeding, list[EdgeStepView]]:
    """How *resource* feeds the mirrors of ``forward_ids``; the forward steps found.

    ``forward_ids`` are the declared edges of one relation whose mirror is
    declared too. Returns ``"none"`` with the unflagged forward steps when the
    resource writes the forward relation and nothing feeds the inverse.
    """
    views = list(iter_edge_steps(resource.pipeline))
    relation = forward_ids[0][2]
    assert relation is not None
    forward_steps = [
        view
        for view in views
        if _may_write(view, relation)
        and any(_endpoints_may_match(view, edge_id) for edge_id in forward_ids)
    ]
    if not forward_steps:
        return "none", []
    if any(view.emit_inverse for view in forward_steps):
        unflagged = [view for view in forward_steps if not view.emit_inverse]
        return "emit_inverse", unflagged
    mirror_ids = [(target, source, inverse) for source, target, _r in forward_ids]
    own_step = any(
        view.relations_written() is not None
        and inverse in (view.relations_written() or set())
        and any(_endpoints_may_match(view, mirror) for mirror in mirror_ids)
        for view in views
    )
    if own_step:
        return "step", []
    if any(_inference_feeds(resource, mirror) for mirror in mirror_ids):
        return "inference", []
    return "none", forward_steps


def _ingestion_findings(
    manifest: GraphManifest, schema: Schema
) -> tuple[list[InverseFinding], dict[frozenset[str], dict[str, Feeding]]]:
    """Findings about who feeds each materialized inverse, and the feeding table."""
    findings: list[InverseFinding] = []
    feeding_table: dict[frozenset[str], dict[str, Feeding]] = {}
    ingestion = manifest.ingestion_model
    if ingestion is None:
        return findings, feeding_table

    edge_config = schema.core_schema.edge_config
    paired = inverse_map(edge_config.inverses)
    declared = {edge.edge_id for edge in edge_config.edges}
    relations = {edge.relation for edge in edge_config.edges if edge.relation}
    # Forward edges whose mirror is declared, grouped by forward relation.
    materialized: dict[str, list[EdgeId]] = {}
    for edge in edge_config.edges:
        relation = edge.relation
        inverse = paired.get(relation) if relation is not None else None
        if inverse is None or not edge.directed:
            continue
        if (edge.target, edge.source, inverse) in declared:
            assert relation is not None
            materialized.setdefault(relation, []).append(edge.edge_id)

    for resource in ingestion.resources:
        views = list(iter_edge_steps(resource.pipeline))
        for relation, forward_ids in sorted(materialized.items()):
            inverse = paired[relation]
            feeding, steps = _feeding(resource, forward_ids, inverse)
            if feeding == "none" and not steps:
                continue
            feeding_table.setdefault(frozenset({relation, inverse}), {})[
                resource.name
            ] = feeding
            if feeding == "none":
                certain = all(view.static_edge_id is not None for view in steps)
                findings.append(
                    InverseFinding(
                        kind="unfed_inverse",
                        severity="repairable",
                        relations=[relation, inverse],
                        edges=forward_ids,
                        steps=[_address(resource, view) for view in steps],
                        message=(
                            f"resource {resource.name!r} writes {relation!r} but "
                            f"feeds nothing into its materialized inverse "
                            f"{inverse!r}; the two relations will disagree"
                        ),
                        detail={"certain": certain},
                    )
                )
            elif feeding == "emit_inverse" and steps:
                findings.append(
                    InverseFinding(
                        kind="unfed_inverse",
                        severity="repairable",
                        relations=[relation, inverse],
                        edges=forward_ids,
                        steps=[_address(resource, view) for view in steps],
                        message=(
                            f"resource {resource.name!r} mirrors {relation!r} into "
                            f"{inverse!r} from some of its steps but not all"
                        ),
                        detail={"certain": False},
                    )
                )

        for view in views:
            written = view.relations_written()
            if view.emit_inverse:
                live = (
                    bool(materialized)
                    if written is None
                    else any(name in materialized for name in written)
                )
                if not live:
                    findings.append(
                        InverseFinding(
                            kind="idle_emit_inverse",
                            severity="repairable",
                            relations=sorted(written or ()),
                            steps=[_address(resource, view)],
                            message=(
                                f"resource {resource.name!r}: a step sets "
                                "emit_inverse but no relation it writes has a "
                                "materialized inverse, so it mirrors nothing"
                            ),
                        )
                    )
                static = view.static_edge_id
                if static is not None and static[2] in paired:
                    mirror = (static[1], static[0], paired[static[2]])
                    twin = [other for other in views if other.static_edge_id == mirror]
                    if twin:
                        findings.append(
                            InverseFinding(
                                kind="double_fed",
                                severity="note",
                                relations=[static[2], paired[static[2]]],
                                edges=[static, mirror],
                                steps=[
                                    _address(resource, view),
                                    *(_address(resource, other) for other in twin),
                                ],
                                message=(
                                    f"resource {resource.name!r} writes {mirror} both "
                                    "by mirroring and with a step of its own; one of "
                                    "the two is redundant"
                                ),
                            )
                        )
            if written and all(
                name in paired and name not in relations for name in written
            ):
                findings.append(
                    InverseFinding(
                        kind="orphan_inverse_step",
                        severity="conflict",
                        relations=sorted(written),
                        steps=[_address(resource, view)],
                        message=(
                            f"resource {resource.name!r}: a step writes only "
                            f"{sorted(written)}, declared inverses that label no "
                            "declared edge; what it writes is dropped. Remove the "
                            "step, or declare the inverse edges"
                        ),
                    )
                )
    return findings, feeding_table


def _native_candidate(pair: PairRealization) -> str:
    """The relation that would be native: the side with edges whose inverse has none."""
    if len(pair.stored_sides) == 1:
        return pair.stored_sides[0]
    return pair.relation


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def manifest_for_audit(config: Mapping[str, Any]) -> GraphManifest:
    """The manifest *config* describes, built without the cross-block checks.

    Each block is still validated on its own -- vertices, edges and the inverse
    table, the physical profile, the resources -- so what comes back is
    well-formed. What is skipped is everything that relates one block to
    another, which is exactly what an audit is there to report on. A manifest
    that loads normally is returned as loaded.

    Raises:
        ValueError: when a block is malformed in itself.
    """
    try:
        manifest = GraphManifest.from_config(dict(config))
        manifest.finish_init()
        return manifest
    except ValueError:
        pass

    schema_block = config.get("schema", config.get("graph_schema"))
    schema: Schema | None = None
    if isinstance(schema_block, Mapping):
        graph = schema_block.get("graph", schema_block.get("core_schema")) or {}
        core = CoreSchema.model_construct(
            vertex_config=VertexConfig.model_validate(graph.get("vertex_config") or {}),
            edge_config=EdgeConfig.model_validate(graph.get("edge_config") or {}),
        )
        schema = Schema.model_construct(
            metadata=GraphMetadata.model_validate(schema_block.get("metadata") or {}),
            core_schema=core,
            db_profile=DatabaseProfile.model_validate(
                schema_block.get("db_profile") or {}
            ),
        )
    ingestion_block = config.get("ingestion_model")
    ingestion = (
        IngestionModel.model_validate(ingestion_block)
        if isinstance(ingestion_block, Mapping)
        else None
    )
    return GraphManifest.model_construct(
        graph_schema=schema, ingestion_model=ingestion, bindings=None
    )


def audit_inverses(manifest: GraphManifest) -> InverseReport:
    """Report how *manifest* realizes every declared inverse, and what is wrong with it.

    Reads the schema, the physical profile and the ingestion model; changes
    nothing, raises nothing, and does not need the manifest to have loaded.
    """
    schema = manifest.graph_schema
    if schema is None:
        return InverseReport()
    edge_config = schema.core_schema.edge_config
    profile = schema.db_profile
    vertex_names = {vertex.name for vertex in schema.core_schema.vertex_config.vertices}

    findings = schema_inverse_findings(schema)
    ingestion_findings, feeding_table = _ingestion_findings(manifest, schema)
    findings = [*findings, *ingestion_findings]

    pairs: list[PairStatus] = []
    for pair in pair_realizations(schema, findings=findings):
        candidate = _native_candidate(pair)
        eligibility = (
            None
            if pair.native_side is not None
            else native_inverse_violations(
                profile, edge_config, vertex_names, candidates=[candidate]
            )
        )
        if eligibility is not None:
            eligibility = [v for v in eligibility if v.relation == candidate]
        pairs.append(
            PairStatus(
                **pair.model_dump(),
                feeding=feeding_table.get(frozenset({pair.relation, pair.inverse}), {}),
                native_eligibility=eligibility,
                native_candidate=None if pair.native_side else candidate,
            )
        )
    return InverseReport(
        pairs=pairs, symmetric=list(edge_config.symmetric), findings=findings
    )


# ---------------------------------------------------------------------------
# The ``inverses`` profile
# ---------------------------------------------------------------------------

_CONSISTENT = "inverses-consistent"
_COMPLETE = "inverses-complete"


def _target(finding: InverseFinding) -> str:
    if finding.steps:
        return f"resource:{finding.steps[0]}"
    if finding.edges:
        source, target, relation = finding.edges[0]
        return f"edge:{source}-{relation or '?'}->{target}"
    if finding.relations:
        return f"relation:{finding.relations[0]}"
    return "manifest"


def _assertion_result(
    assertion_id: str,
    title: str,
    findings: list[Finding],
    checked: int,
) -> AssertionResult:
    if checked == 0 and not findings:
        status = "not_applicable"
    else:
        status = roll_up([f.status for f in findings]) if findings else "pass"
    return AssertionResult(
        id=assertion_id,
        title=title,
        required=True,
        status=status,
        checked=checked,
        findings=findings,
    )


def check_inverses_consistent(context: CheckContext) -> AssertionResult:
    """No two places of the manifest contradict each other about an inverse."""
    report = audit_inverses(context.manifest)
    findings = [
        Finding(
            assertion=_CONSISTENT,
            status="fail",
            severity="error",
            target=_target(finding),
            message=finding.message,
            detail={"kind": finding.kind, **finding.detail},
        )
        for finding in report.conflicts()
    ]
    return _assertion_result(
        _CONSISTENT,
        "Declared inverses are realized without contradiction",
        findings,
        len(report.pairs) + len(report.symmetric),
    )


def check_inverses_complete(context: CheckContext) -> AssertionResult:
    """Nothing about an inverse is stated in one place and omitted in another."""
    report = audit_inverses(context.manifest)
    findings = [
        Finding(
            assertion=_COMPLETE,
            status="warn",
            severity="warning",
            target=_target(finding),
            message=finding.message,
            detail={"kind": finding.kind, **finding.detail},
        )
        for finding in report.repairable()
    ] + [
        Finding(
            assertion=_COMPLETE,
            status="pass",
            severity="info",
            target=_target(finding),
            message=finding.message,
            detail={"kind": finding.kind, **finding.detail},
        )
        for finding in report.notes()
    ]
    return _assertion_result(
        _COMPLETE,
        "Every realization of an inverse is complete",
        findings,
        len(report.pairs) + len(report.symmetric),
    )


INVERSES_PROFILE = Profile(
    name="inverses",
    version=PROFILE_VERSION,
    assertions=(
        Assertion(
            _CONSISTENT,
            "Declared inverses are realized without contradiction",
            True,
            check_inverses_consistent,
        ),
        Assertion(
            _COMPLETE,
            "Every realization of an inverse is complete",
            True,
            check_inverses_complete,
        ),
    ),
)


__all__ = [
    "INVERSES_PROFILE",
    "Feeding",
    "InverseReport",
    "PairStatus",
    "audit_inverses",
    "check_inverses_complete",
    "check_inverses_consistent",
    "manifest_for_audit",
]
