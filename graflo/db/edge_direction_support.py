"""Backend support for edge directionality (advisory — never raises).

``Edge.directed`` is a statement about the *model*: when false, endpoint order
carries no meaning and the two orientations denote one relationship. Backends
express that to wildly different degrees, and the difference matters mostly on
the read path — reaching an edge from its target endpoint is free on some
backends, needs an unemitted clause on others, and is a schema-time decision
that cannot be retrofitted on TigerGraph.

This module records that matrix so callers (schema application, traversal
planning, capability reporting) can consult one table instead of re-deriving
per-backend behaviour inline.

Unlike :mod:`graflo.db.field_type_support`, nothing here raises.
``directed=False`` is already expressible in shipped manifests and is silently
ignored by seven of the eight targets; refusing it now would reject working
schemas. Callers get diagnostics and decide.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from graflo.architecture.graph_types import EdgeDirection, EdgeId
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.onto import DBType


class UnsupportedEdgeDirectionError(ValueError):
    """Raised when a backend cannot answer a read in the requested direction.

    Only TigerGraph can reach this: reverse reachability there is fixed when the
    edge type is created (``WITH REVERSE_EDGE``), so no query rewrite recovers it.
    Failing loudly is deliberate — silently returning outgoing edges for an
    ``ANY`` request would under-report the neighbourhood with no signal.
    """


class ReverseTraversalCost(StrEnum):
    """What it costs to reach an edge from its *target* endpoint."""

    FREE = "free"
    """Both endpoints are indexed; the reverse query is the same price."""

    CHEAP = "cheap"
    """Relationships are stored bidirectionally; the reverse pattern is legal and fast."""

    CLAUSE_REQUIRED = "clause_required"
    """Cheap once asked for, but only via an explicit reverse/bidirectional clause."""

    INDEX_REQUIRED = "index_required"
    """Needs a secondary index on the target column before it is affordable."""

    SCHEMA_TIME_ONLY = "schema_time_only"
    """Decided at DDL time; no query rewrite can recover it afterwards."""

    MATERIALIZATION_REQUIRED = "materialization_required"
    """Direction is the storage partition key; the reverse view must be written out."""


# Backends with a genuine undirected edge *type* in their DDL.
_UNDIRECTED_NATIVE_DBS: frozenset[DBType] = frozenset({DBType.TIGERGRAPH})

_REVERSE_TRAVERSAL_COST: dict[DBType, ReverseTraversalCost] = {
    # Edge collections index `_from` and `_to`; INBOUND/ANY cost the same as OUTBOUND.
    DBType.ARANGO: ReverseTraversalCost.FREE,
    # Relationships are doubly linked; `<-[r]-` and `-[r]-` are O(degree).
    DBType.NEO4J: ReverseTraversalCost.CHEAP,
    DBType.MEMGRAPH: ReverseTraversalCost.CHEAP,
    # Adjacency matrices are stored alongside their transposes.
    DBType.FALKORDB: ReverseTraversalCost.CHEAP,
    # Edges are stored under both an out-key and an in-key, reachable via
    # `GO ... OVER <type> REVERSELY` / `BIDIRECT`.
    DBType.NEBULA: ReverseTraversalCost.CLAUSE_REQUIRED,
    # A directed edge type is reachable backwards only through a paired reverse
    # type declared as `WITH REVERSE_EDGE=...` when the type is created.
    DBType.TIGERGRAPH: ReverseTraversalCost.SCHEMA_TIME_ONLY,
    # Edge tables carry `source_id` / `target_id`; the reverse lookup needs its
    # own index (the composite unique index is leading-column `source_id`).
    DBType.POSTGRES: ReverseTraversalCost.INDEX_REQUIRED,
    # Edge batches are partitioned by (source, target, relation).
    DBType.GRAFLO_BACKEND: ReverseTraversalCost.MATERIALIZATION_REQUIRED,
}


@dataclass(frozen=True)
class EdgeDirectionDiagnostic:
    """One finding about how a backend will treat a logically undirected edge."""

    edge_id: EdgeId
    db_type: DBType
    severity: Literal["info", "warning"]
    message: str
    remedy: str

    def __str__(self) -> str:
        return f"{self.message} {self.remedy}"


# Per-cost wording for an edge declared `directed=false` on a backend with no
# native undirected type. Phrased as what the *stored graph* will and will not say.
_UNDIRECTED_FALLBACK: dict[ReverseTraversalCost, tuple[str, str]] = {
    ReverseTraversalCost.FREE: (
        (
            "is stored as a directed edge; the backend indexes both endpoints, so "
            "the assertion costs nothing to honour on reads"
        ),
        "Query the edge bidirectionally; nothing else is needed.",
    ),
    ReverseTraversalCost.CHEAP: (
        (
            "is stored as a directed relationship; the backend matches either "
            "orientation cheaply, so the assertion holds on reads but is not "
            "recorded in the graph itself"
        ),
        "Match the relationship without a direction arrow.",
    ),
    ReverseTraversalCost.CLAUSE_REQUIRED: (
        (
            "is stored as a directed edge; the backend can traverse it either way, "
            "but only when the query asks for it explicitly"
        ),
        "Traverse with the reverse/bidirectional clause.",
    ),
    ReverseTraversalCost.INDEX_REQUIRED: (
        (
            "is stored as a directed row; reaching it from the target side needs an "
            "index that is not created today"
        ),
        "Add a secondary index on the edge table's target column.",
    ),
    ReverseTraversalCost.MATERIALIZATION_REQUIRED: (
        (
            "cannot be honoured: direction is the storage partition key, so the "
            "reversed orientation is simply a different (absent) batch"
        ),
        (
            "Write the reversed edge explicitly, or target a backend with an "
            "undirected or bidirectionally indexed edge type."
        ),
    ),
    ReverseTraversalCost.SCHEMA_TIME_ONLY: (
        (
            "is stored as a directed edge type; reverse reachability is fixed when "
            "the type is created and cannot be added by a query"
        ),
        "Declare the edge type as undirected, or pair it with a reverse edge type.",
    ),
}


def supports_native_undirected(db_type: DBType) -> bool:
    """Whether the backend has an undirected edge *type* in its schema language."""
    return _coerce(db_type) in _UNDIRECTED_NATIVE_DBS


def reverse_traversal_cost(db_type: DBType) -> ReverseTraversalCost:
    """What it costs to reach an edge from its target endpoint on ``db_type``.

    Raises:
        KeyError: if ``db_type`` is not a supported write target.
    """
    coerced = _coerce(db_type)
    try:
        return _REVERSE_TRAVERSAL_COST[coerced]
    except KeyError:
        raise KeyError(
            f"No reverse-traversal cost recorded for backend '{_label(db_type)}'. "
            "Every target backend must have an entry."
        ) from None


def default_direction_for_edge(edge: Edge) -> EdgeDirection:
    """The direction a read should follow for ``edge`` when none is requested.

    This is where ``Edge.directed`` stops being an annotation and starts
    steering queries: an undirected edge reads as :attr:`EdgeDirection.ANY`,
    because both orientations denote the same relationship and anchoring on
    ``source`` alone would drop half the neighbourhood.
    """
    return EdgeDirection.OUT if edge.directed else EdgeDirection.ANY


def assert_direction_supported(
    db_type: DBType,
    direction: EdgeDirection,
    *,
    has_reverse_edge: bool = False,
    edge_is_undirected: bool = False,
) -> None:
    """Raise if ``db_type`` cannot answer a read in ``direction``.

    Args:
        db_type: Backend being queried.
        direction: Requested orientation.
        has_reverse_edge: Whether a paired reverse edge type is declared for the
            edge (``EdgePhysicalSpec.reverse_edge``). Only consulted on backends
            whose reverse reachability is decided at schema time.
        edge_is_undirected: Whether the edge type itself was created undirected.
            On a backend with native undirected edges that already answers both
            orientations, so no reverse type is needed.

    Raises:
        UnsupportedEdgeDirectionError: when the backend physically cannot follow
            the edge backwards.
    """
    if direction is EdgeDirection.OUT:
        return
    coerced = _coerce(db_type)
    if (
        _REVERSE_TRAVERSAL_COST.get(coerced)
        is not ReverseTraversalCost.SCHEMA_TIME_ONLY
    ):
        return
    if has_reverse_edge:
        return
    if edge_is_undirected and coerced in _UNDIRECTED_NATIVE_DBS:
        return
    raise UnsupportedEdgeDirectionError(
        f"Backend '{_label(db_type)}' cannot read edges with direction "
        f"'{direction.value}': reverse reachability is fixed when the edge type "
        "is created and no query rewrite recovers it. Declare the edge type as "
        "undirected (`directed: false`), or pair it with a reverse edge type via "
        "`db_profile.edge_specs[*].reverse_edge`."
    )


def iter_undirected_edges(schema: Schema) -> Iterable[EdgeId]:
    """Yield the id of every edge in ``schema`` declared logically undirected."""
    for edge in schema.core_schema.edge_config.values():
        if not edge.directed:
            yield edge.edge_id


def check_schema_edge_directions(
    db_type: DBType, schema: Schema
) -> list[EdgeDirectionDiagnostic]:
    """Report how ``db_type`` will treat each logically undirected edge.

    Returns an empty list when the schema declares no undirected edges, or when
    the backend represents them natively. Never raises: an unknown backend
    yields no diagnostics rather than blocking a schema application.
    """
    coerced = _coerce(db_type)
    if coerced not in _REVERSE_TRAVERSAL_COST:
        return []
    if coerced in _UNDIRECTED_NATIVE_DBS:
        return []

    cost = _REVERSE_TRAVERSAL_COST[coerced]
    effect, remedy = _UNDIRECTED_FALLBACK[cost]
    severity: Literal["info", "warning"] = (
        "warning"
        if cost
        in (
            ReverseTraversalCost.INDEX_REQUIRED,
            ReverseTraversalCost.MATERIALIZATION_REQUIRED,
        )
        else "info"
    )
    label = _label(db_type)
    return [
        EdgeDirectionDiagnostic(
            edge_id=edge_id,
            db_type=coerced,
            severity=severity,
            message=(
                f"Edge {edge_id!r} is declared undirected, but backend '{label}' has "
                f"no undirected edge type: it {effect}."
            ),
            remedy=remedy,
        )
        for edge_id in iter_undirected_edges(schema)
    ]


def _coerce(db_type: DBType) -> DBType:
    """Accept the bare strings that reach these helpers from validated config."""
    if isinstance(db_type, DBType):
        return db_type
    try:
        return DBType(db_type)
    except ValueError:
        return db_type


def _label(db_type: DBType) -> str:
    # ``db_flavor`` arrives as a bare string from some validated config models,
    # so the enum's ``.value`` is not always there to read.
    return str(getattr(db_type, "value", db_type))


__all__ = [
    "EdgeDirectionDiagnostic",
    "ReverseTraversalCost",
    "UnsupportedEdgeDirectionError",
    "assert_direction_supported",
    "check_schema_edge_directions",
    "default_direction_for_edge",
    "iter_undirected_edges",
    "reverse_traversal_cost",
    "supports_native_undirected",
]
