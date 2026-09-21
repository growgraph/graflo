"""How declared inverses are realized, and what is wrong with a realization.

A declared pair ``{a, b}`` (``edge_config.inverses``) is a statement about
relation names only, and it is already useful as it stands: ``b`` is a name for
reading ``a`` from its target, and a read resolves it to a reverse traversal of
the ``a`` edge. Nothing needs storing for that.

Storing the reverse reading is a separate, optional step -- a *realization* --
and a pair has at most one:

``native``
    The database maintains the reverse type itself
    (``db_profile.native_inverses``; TigerGraph ``WITH REVERSE_EDGE``).
``materialized``
    ``(T, S, b)`` is an ordinary declared edge, fed at ingestion.

This module reports the state of every declared pair -- ``declared`` when
nothing realizes it, ``native`` or ``materialized`` when something does, and the
two diagnostic states ``partial`` and ``conflicting`` -- and what it found as
typed :class:`InverseFinding` records. Nothing here raises and nothing mutates:
the same checks back the load-time refusals (which raise), the string
advisories, and the audit, so the three cannot drift apart.

Everything works on a schema that has **not** been through ``finish_init``, so
a manifest that no longer loads can still be inspected.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import Edge, EdgeConfig, inverse_map
from graflo.architecture.schema.edge_direction import (
    REVERSE_TRAVERSAL_COST,
    ReverseTraversalCost,
    coerce_db_type,
)
from graflo.onto import DBType

if TYPE_CHECKING:
    from graflo.architecture.schema.database_features import DatabaseProfile
    from graflo.architecture.schema.document import Schema

PairState = Literal["declared", "native", "materialized", "partial", "conflicting"]
"""``declared``: the pair is declared and nothing realizes it, which is the
default and needs no remedy. ``native`` / ``materialized``: how it is realized.
``partial``: mirrored for some endpoint pairs only. ``conflicting``: touched by
a ``conflict`` finding."""

FindingSeverity = Literal["repairable", "conflict", "note"]
"""``repairable``: one side under-reports what the other states, and propagating
it cannot change meaning. ``conflict``: the two sides contradict each other, and
only the author can say which is right. ``note``: nothing is wrong."""

NativeInverseCode = Literal[
    "not_tigergraph",
    "no_edge",
    "symmetric",
    "undeclared",
    "both_sides",
    "collides_with_edges",
    "collides_with_vertex",
    "split_physical_names",
]


class StepAddress(ConfigBaseModel):
    """Where an edge step sits: a resource, a path into its pipeline, a position.

    ``at`` descends through nested pipelines by step index, ``step`` is the
    index within the pipeline reached, and ``link`` selects one entry of a
    step's ``links`` list.
    """

    resource: str
    at: list[int] = PydanticField(default_factory=list)
    step: int
    link: int | None = None

    def __str__(self) -> str:
        path = "/".join(str(index) for index in [*self.at, self.step])
        suffix = "" if self.link is None else f"#link{self.link}"
        return f"{self.resource}:{path}{suffix}"


class InverseFinding(ConfigBaseModel):
    """One thing worth knowing about how declared inverses are realized."""

    kind: str = PydanticField(..., description="Stable identifier of the finding.")
    severity: FindingSeverity
    relations: list[str] = PydanticField(default_factory=list)
    edges: list[EdgeId] = PydanticField(default_factory=list)
    steps: list[StepAddress] = PydanticField(
        default_factory=list,
        description="Ingestion steps involved; empty for a schema-only finding.",
    )
    message: str
    detail: dict[str, Any] = PydanticField(default_factory=dict)

    @property
    def key(self) -> tuple[Any, ...]:
        """Identity of the finding, for comparing two reports of one manifest."""
        return (
            self.kind,
            tuple(self.relations),
            tuple(self.edges),
            tuple(str(step) for step in self.steps),
        )


class NativeInverseViolation(ConfigBaseModel):
    """One rule a native inverse breaks, for one relation."""

    relation: str
    code: NativeInverseCode
    message: str


class PairRealization(ConfigBaseModel):
    """How one declared pair is realized in a schema."""

    relation: str
    inverse: str
    state: PairState
    stored_sides: list[str] = PydanticField(
        default_factory=list,
        description="Relations of the pair that name at least one declared edge.",
    )
    native_side: str | None = PydanticField(
        default=None,
        description="The relation listed in ``db_profile.native_inverses``, if any.",
    )
    mirrored: int = PydanticField(
        default=0, description="Directed edges of the pair whose mirror is declared."
    )
    total: int = PydanticField(
        default=0, description="Directed edges naming either relation of the pair."
    )


class ResolvedRelation(ConfigBaseModel):
    """One declared edge that answers a read of a relation name.

    ``reversed`` means the name is the declared inverse of the edge's relation
    and nothing is stored under it, so the read follows the edge from its
    target: the caller flips the direction it was asked for.
    """

    edge: Edge
    reversed: bool = False
    state: PairState | None = None
    native_inverse_type: str | None = None


# ---------------------------------------------------------------------------
# Edge-level findings
# ---------------------------------------------------------------------------


def _drift_keys(edge: Edge) -> set[frozenset[str]]:
    # Endpoints are always part of an edge key; only the rest can drift.
    return {
        frozenset(key) - {"source", "target", "relation"} for key in edge.identities
    }


def _property_types(edge: Edge) -> dict[str, tuple[Any, Any]]:
    return {field.name: (field.type, field.item_type) for field in edge.properties}


def _mirrors(edge_config: EdgeConfig) -> list[tuple[Edge, Edge | None, str]]:
    """``(edge, its declared mirror or None, inverse name)`` for every paired directed edge."""
    paired = inverse_map(edge_config.inverses)
    by_id = {edge.edge_id: edge for edge in edge_config.edges}
    out: list[tuple[Edge, Edge | None, str]] = []
    for edge in edge_config.edges:
        if edge.relation is None or not edge.directed:
            continue
        inverse = paired.get(edge.relation)
        if inverse is None:
            continue
        out.append((edge, by_id.get((edge.target, edge.source, inverse)), inverse))
    return out


def _relation_wide_addition_is_exact(
    edge_config: EdgeConfig, additions: dict[str, set[str]]
) -> bool:
    """Whether adding ``{relation: names}`` to *every* edge of each relation repairs the drift only.

    Edge properties are added per relation, not per edge. The repair is exact
    when every edge the addition would touch is one that lacks the names only
    because its mirror states them.
    """
    paired = inverse_map(edge_config.inverses)
    grown: dict[EdgeId, set[str]] = {
        edge.edge_id: set(edge.property_names)
        | additions.get(edge.relation or "", set())
        for edge in edge_config.edges
    }
    for edge in edge_config.edges:
        inverse = paired.get(edge.relation) if edge.relation is not None else None
        if inverse is None or not edge.directed:
            continue
        mirror = grown.get((edge.target, edge.source, inverse))
        if mirror is None:
            # An unmirrored edge of the relation would gain properties nobody
            # stated for it.
            if grown[edge.edge_id] != set(edge.property_names):
                return False
        elif mirror != grown[edge.edge_id]:
            return False
    return True


def edge_inverse_findings(edge_config: EdgeConfig) -> list[InverseFinding]:
    """Findings that need only the logical edges and the inverse table.

    Covers what the load-time checks refuse (so an unloadable config can still
    be described) and what they let through because realizing a pair is
    optional: drift between an edge and its mirror, a relation mirrored for some
    endpoint pairs only, and a pair read from the same side.
    """
    paired = inverse_map(edge_config.inverses)
    by_id = {edge.edge_id: edge for edge in edge_config.edges}
    findings: list[InverseFinding] = []

    realized: dict[str, list[tuple[Edge, bool]]] = {}
    property_gaps: dict[str, set[str]] = {}
    property_drifts: list[tuple[Edge, Edge]] = []

    for edge, mirror, inverse in _mirrors(edge_config):
        relation = edge.relation
        assert relation is not None
        realized.setdefault(relation, []).append((edge, mirror is not None))
        same_side = (edge.source, edge.target, inverse)
        if edge.source != edge.target and same_side in by_id and relation < inverse:
            findings.append(
                InverseFinding(
                    kind="same_side_pair",
                    severity="conflict",
                    relations=[relation, inverse],
                    edges=[edge.edge_id, same_side],
                    message=(
                        f"{edge.edge_id} and {same_side} read a declared pair from "
                        "the same side; one of them is probably reversed"
                    ),
                )
            )
        if mirror is None or relation > inverse:
            continue
        ours, theirs = _property_types(edge), _property_types(mirror)
        retyped = sorted(
            name for name in ours.keys() & theirs.keys() if ours[name] != theirs[name]
        )
        if retyped:
            findings.append(
                InverseFinding(
                    kind="property_type_drift",
                    severity="conflict",
                    relations=[relation, inverse],
                    edges=[edge.edge_id, mirror.edge_id],
                    message=(
                        f"inverse edges {edge.edge_id} and {mirror.edge_id} type "
                        f"these properties differently: {retyped}"
                    ),
                    detail={"properties": retyped},
                )
            )
        if set(ours) != set(theirs):
            property_drifts.append((edge, mirror))
            property_gaps.setdefault(relation, set()).update(set(theirs) - set(ours))
            property_gaps.setdefault(inverse, set()).update(set(ours) - set(theirs))
        if _drift_keys(edge) != _drift_keys(mirror):
            findings.append(
                InverseFinding(
                    kind="identity_drift",
                    severity="conflict",
                    relations=[relation, inverse],
                    edges=[edge.edge_id, mirror.edge_id],
                    message=(
                        f"inverse edges {edge.edge_id} and {mirror.edge_id} declare "
                        "different identity keys"
                    ),
                )
            )

    exact = bool(property_drifts) and _relation_wide_addition_is_exact(
        edge_config, property_gaps
    )
    for edge, mirror in property_drifts:
        assert edge.relation is not None and mirror.relation is not None
        findings.append(
            InverseFinding(
                kind="property_drift",
                severity="repairable" if exact else "conflict",
                relations=[edge.relation, mirror.relation],
                edges=[edge.edge_id, mirror.edge_id],
                message=(
                    f"inverse edges {edge.edge_id} and {mirror.edge_id} declare "
                    f"different properties: {sorted(edge.property_names)} vs "
                    f"{sorted(mirror.property_names)}"
                ),
                detail={
                    "missing": {
                        edge.relation: sorted(
                            set(mirror.property_names) - set(edge.property_names)
                        ),
                        mirror.relation: sorted(
                            set(edge.property_names) - set(mirror.property_names)
                        ),
                    }
                },
            )
        )

    for relation, flags in sorted(realized.items()):
        if any(flag for _edge, flag in flags) and not all(
            flag for _edge, flag in flags
        ):
            findings.append(
                InverseFinding(
                    kind="partial_endpoint_pairs",
                    severity="repairable",
                    relations=[relation, paired[relation]],
                    edges=[edge.edge_id for edge, flag in flags if not flag],
                    message=(
                        f"relation {relation!r} has explicit inverse edges for some "
                        "endpoint pairs but not all; add_inverse_edges completes it"
                    ),
                )
            )

    findings.extend(_directedness_findings(edge_config))
    return findings


def mixed_directed_relations(edge_config: EdgeConfig) -> dict[str, list[EdgeId]]:
    """Relations whose edges disagree on ``directed``, with every edge of each."""
    by_relation: dict[str, list[Edge]] = {}
    for edge in edge_config.edges:
        if edge.relation is not None:
            by_relation.setdefault(edge.relation, []).append(edge)
    return {
        relation: [edge.edge_id for edge in edges]
        for relation, edges in sorted(by_relation.items())
        if len({edge.directed for edge in edges}) > 1
    }


def _directedness_findings(edge_config: EdgeConfig) -> list[InverseFinding]:
    """``directed`` against the inverse table: the same fact at two granularities."""
    paired = inverse_map(edge_config.inverses)
    findings: list[InverseFinding] = []
    mixed = mixed_directed_relations(edge_config)
    for relation, edge_ids in mixed.items():
        findings.append(
            InverseFinding(
                kind="mixed_directed",
                severity="conflict",
                relations=[relation],
                edges=edge_ids,
                message=(
                    f"edges of relation {relation!r} disagree on `directed`; a "
                    "relation is symmetric or it is not"
                ),
            )
        )

    by_relation: dict[str, list[Edge]] = {}
    for edge in edge_config.edges:
        if edge.relation is not None and edge.relation not in mixed:
            by_relation.setdefault(edge.relation, []).append(edge)
    for relation, edges in sorted(by_relation.items()):
        undirected = not edges[0].directed
        edge_ids = [edge.edge_id for edge in edges]
        if relation in paired and undirected:
            findings.append(
                InverseFinding(
                    kind="paired_on_undirected",
                    severity="conflict",
                    relations=[relation, paired[relation]],
                    edges=edge_ids,
                    message=(
                        f"relation {relation!r} has a declared inverse but its "
                        "edges are undirected; an undirected edge already reads "
                        "both ways"
                    ),
                )
            )
        elif relation in edge_config.symmetric and not undirected:
            findings.append(
                InverseFinding(
                    kind="symmetric_on_directed",
                    severity="repairable",
                    relations=[relation],
                    edges=edge_ids,
                    message=(
                        f"relation {relation!r} is declared symmetric but its edges "
                        "are directed; undirected is its realization"
                    ),
                )
            )
        elif undirected and relation not in edge_config.symmetric:
            findings.append(
                InverseFinding(
                    kind="undirected_not_symmetric",
                    severity="repairable",
                    relations=[relation],
                    edges=edge_ids,
                    message=(
                        f"every edge of relation {relation!r} is undirected but the "
                        "relation is not declared symmetric"
                    ),
                )
            )
    return findings


# ---------------------------------------------------------------------------
# Native inverses
# ---------------------------------------------------------------------------


def native_inverse_violations(
    profile: DatabaseProfile,
    edge_config: EdgeConfig,
    vertex_names: set[str],
    *,
    candidates: Iterable[str] | None = None,
) -> list[NativeInverseViolation]:
    """Every rule broken by a native inverse, without changing the profile.

    A native inverse is the database realizing a *declared* pair for a whole
    relation, so it needs the declaration, must not coexist with explicit
    inverse edges, and exists only where the database can maintain one: on a
    single TigerGraph edge type whose reverse name is free.

    Args:
        profile: The physical profile; read only.
        edge_config: Declared edges and inverse table.
        vertex_names: Logical vertex type names, which share a namespace with
            edge types on TigerGraph.
        candidates: Relations to check *as if* they were listed in
            ``native_inverses``, in addition to those already listed. This is
            the eligibility pre-check: ask before asking the profile to change.
    """
    native = set(profile.native_inverses) | set(candidates or ())
    if not native:
        return []
    out: list[NativeInverseViolation] = []

    def report(relation: str, code: NativeInverseCode, message: str) -> None:
        out.append(
            NativeInverseViolation(relation=relation, code=code, message=message)
        )

    edges_by_relation: dict[str, list[EdgeId]] = {}
    for edge in edge_config.edges:
        if edge.relation is not None:
            edges_by_relation.setdefault(edge.relation, []).append(edge.edge_id)

    for relation in sorted(native):
        if profile.db_flavor != DBType.TIGERGRAPH:
            report(
                relation,
                "not_tigergraph",
                f"{relation!r}: native inverses are TigerGraph-only (db_flavor is "
                f"{str(profile.db_flavor)!r}); use explicit inverse edges "
                "(add_inverse_edges) for a portable inverse",
            )
        edge_ids = edges_by_relation.get(relation)
        if not edge_ids:
            report(relation, "no_edge", f"{relation!r}: names no declared edge")
            continue
        if edge_config.is_symmetric(relation):
            report(
                relation,
                "symmetric",
                f"{relation!r}: a symmetric relation has no reverse type; its "
                "edges are undirected",
            )
            continue
        inverse = edge_config.inverse_of(relation)
        if inverse is None:
            report(
                relation,
                "undeclared",
                f"{relation!r}: a native inverse needs a declared pair; add "
                f"{{relation: {relation}, inverse: <name>}} to edge_config.inverses",
            )
            continue
        if inverse in native and relation < inverse:
            report(
                relation,
                "both_sides",
                f"{relation!r} and {inverse!r}: only one side of a pair can be "
                "native; the other is the reverse type the database creates",
            )
        # TigerGraph edge type names share one namespace with each other and
        # with vertex types, so the reverse type must not shadow either --
        # including an explicit inverse edge, which would store the fact twice.
        if inverse in edges_by_relation:
            report(
                relation,
                "collides_with_edges",
                f"{relation!r}: native inverse {inverse!r} collides with the "
                "declared edges of that name; keep one realization of the pair",
            )
        elif inverse in vertex_names:
            report(
                relation,
                "collides_with_vertex",
                f"{relation!r}: native inverse {inverse!r} collides with a vertex type",
            )
        # WITH REVERSE_EDGE belongs to one edge type; a relation spread over
        # several physical names would ask for one reverse name twice.
        physical = sorted(
            {
                profile.edge_relation_name(edge_id, default_relation=relation)
                or relation
                for edge_id in edge_ids
            }
        )
        if len(physical) > 1:
            report(
                relation,
                "split_physical_names",
                f"{relation!r}: a native inverse needs the relation stored as "
                f"one edge type, but relation_name splits it into {physical}",
            )
    return out


# ---------------------------------------------------------------------------
# Schema-level view
# ---------------------------------------------------------------------------


def _vertex_names(schema: Schema) -> set[str]:
    return {vertex.name for vertex in schema.core_schema.vertex_config.vertices}


def schema_inverse_findings(schema: Schema) -> list[InverseFinding]:
    """Every finding that needs the schema but not the ingestion model."""
    edge_config = schema.core_schema.edge_config
    profile = schema.db_profile
    findings = edge_inverse_findings(edge_config)

    for violation in native_inverse_violations(
        profile, edge_config, _vertex_names(schema)
    ):
        inverse = edge_config.inverse_of(violation.relation)
        findings.append(
            InverseFinding(
                kind=f"native_{violation.code}",
                severity="conflict",
                relations=[
                    violation.relation,
                    *([inverse] if inverse and inverse != violation.relation else []),
                ],
                message=f"invalid native inverse: {violation.message}",
            )
        )

    cost = REVERSE_TRAVERSAL_COST.get(coerce_db_type(profile.db_flavor))
    if cost in (
        ReverseTraversalCost.SCHEMA_TIME_ONLY,
        ReverseTraversalCost.MATERIALIZATION_REQUIRED,
    ):
        for pair in pair_realizations(schema, findings=()):
            if pair.state != "declared" or not pair.stored_sides:
                continue
            findings.append(
                InverseFinding(
                    kind="reverse_unreadable",
                    severity="note",
                    relations=[pair.relation, pair.inverse],
                    message=(
                        f"pair {pair.relation!r} / {pair.inverse!r} stores one side "
                        f"only, and backend {str(profile.db_flavor)!r} cannot read "
                        "an edge from its target without a stored reverse "
                        f"({cost.value}); realize the pair to make the reverse "
                        "reading answerable"
                    ),
                    detail={"reverse_traversal_cost": cost.value},
                )
            )
    return findings


def pair_realizations(
    schema: Schema, *, findings: Iterable[InverseFinding] | None = None
) -> list[PairRealization]:
    """How each declared pair is realized.

    Args:
        schema: The schema to classify.
        findings: Findings already computed for this schema; a pair touched by a
            ``conflict`` is reported as ``conflicting``. Omitted: computed here.
    """
    edge_config = schema.core_schema.edge_config
    profile = schema.db_profile
    if findings is None:
        findings = schema_inverse_findings(schema)
    conflicted: set[str] = {
        relation
        for finding in findings
        if finding.severity == "conflict"
        for relation in finding.relations
    }

    relations = {edge.relation for edge in edge_config.edges if edge.relation}
    mirrors = _mirrors(edge_config)
    out: list[PairRealization] = []
    for pair in edge_config.inverses:
        names = (pair.relation, pair.inverse)
        ours = [
            (edge, mirror) for edge, mirror, _inv in mirrors if edge.relation in names
        ]
        mirrored = sum(1 for _edge, mirror in ours if mirror is not None)
        native_side = next(
            (name for name in names if name in profile.native_inverses), None
        )
        if conflicted & set(names):
            state: PairState = "conflicting"
        elif native_side is not None:
            state = "native"
        elif mirrored == 0:
            state = "declared"
        elif mirrored == len(ours):
            state = "materialized"
        else:
            state = "partial"
        out.append(
            PairRealization(
                relation=pair.relation,
                inverse=pair.inverse,
                state=state,
                stored_sides=[name for name in names if name in relations],
                native_side=native_side,
                mirrored=mirrored,
                total=len(ours),
            )
        )
    return out


def materialized_inverse_id(edge_config: EdgeConfig, edge_id: EdgeId) -> EdgeId | None:
    """The declared edge that mirroring ``edge_id`` writes into, or None.

    ``(s, t, a)`` mirrors into ``(t, s, b)`` when ``a`` is paired with ``b`` and
    that edge -- or a relation-less template between the same types -- is
    declared. That second condition is what *materialized* means: a pair that is
    only declared, or that the database maintains, has nothing to write into.
    """
    source, target, relation = edge_id
    if relation is None:
        return None
    inverse = inverse_map(edge_config.inverses).get(relation)
    if inverse is None:
        return None
    declared = {edge.edge_id for edge in edge_config.edges}
    if (target, source, inverse) in declared or (target, source, None) in declared:
        return target, source, inverse
    return None


def inverse_emission_refusal(edge_config: EdgeConfig, edge_id: EdgeId) -> str | None:
    """Why an edge step writing exactly ``edge_id`` cannot set ``emit_inverse``, or None.

    Applies to a step whose endpoints and relation are all fixed, so it names one
    edge and everything can be decided from the schema: the relation needs a
    declared pair (a symmetric relation has no inverse edge -- its edges are
    undirected), and the inverse must be materialized.
    """
    source, target, relation = edge_id
    if relation is None:
        return "the step names no relation, so there is no inverse to write"
    if edge_config.is_symmetric(relation):
        return (
            f"relation {relation!r} is symmetric; it has no inverse edge, its "
            "edges are undirected"
        )
    inverse = edge_config.inverse_of(relation)
    if inverse is None:
        return (
            f"relation {relation!r} has no declared inverse; add "
            f"{{relation: {relation}, inverse: <name>}} to edge_config.inverses "
            "(declare_edge_inverses)"
        )
    if materialized_inverse_id(edge_config, edge_id) is None:
        return (
            f"the inverse edge {(target, source, inverse)} is not declared, so "
            "there is nothing to write into. Declare it (add_inverse_edges), or "
            "drop the flag and keep the inverse unstored -- it is still readable "
            f"as the reverse of {relation!r}, and on TigerGraph the database can "
            "maintain it (set_native_inverses)"
        )
    return None


def resolve_relation(schema: Schema, name: str) -> list[ResolvedRelation]:
    """The declared edges that answer a read of relation ``name``.

    A name that labels declared edges resolves to them as they are. A name that
    labels none, but is the declared inverse of a relation that does, resolves
    to *those* edges read backwards -- which is what makes a declared pair
    useful without storing anything. A name known neither way resolves to
    nothing.
    """
    edge_config = schema.core_schema.edge_config
    profile = schema.db_profile
    states = {
        side: pair.state
        for pair in pair_realizations(schema)
        for side in (pair.relation, pair.inverse)
    }

    def resolved(edge: Edge, *, backwards: bool) -> ResolvedRelation:
        return ResolvedRelation(
            edge=edge,
            reversed=backwards,
            state=states.get(edge.relation or ""),
            native_inverse_type=profile.native_inverse_of(edge.relation, edge_config),
        )

    direct = [edge for edge in edge_config.edges if edge.relation == name]
    if direct:
        return [resolved(edge, backwards=False) for edge in direct]
    forward = inverse_map(edge_config.inverses).get(name)
    if forward is None:
        return []
    return [
        resolved(edge, backwards=True)
        for edge in edge_config.edges
        if edge.relation == forward and edge.directed
    ]


__all__ = [
    "FindingSeverity",
    "InverseFinding",
    "NativeInverseCode",
    "NativeInverseViolation",
    "PairRealization",
    "PairState",
    "ResolvedRelation",
    "StepAddress",
    "edge_inverse_findings",
    "inverse_emission_refusal",
    "materialized_inverse_id",
    "mixed_directed_relations",
    "native_inverse_violations",
    "pair_realizations",
    "resolve_relation",
    "schema_inverse_findings",
]
