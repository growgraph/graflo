"""Backend support for edge directionality (advisory — never raises).

``Edge.directed`` is a statement about the *model*: when false, endpoint order
carries no meaning and the two orientations denote one relationship. Backends
express that to wildly different degrees, and the difference matters mostly on
the read path — reaching an edge from its target endpoint is free on some
backends, needs an unemitted clause on others, and is a schema-time decision
that cannot be retrofitted on TigerGraph.

The matrix itself lives in :mod:`graflo.architecture.schema.edge_direction`,
below every backend, and is re-exported here. This module adds what needs a
backend in view: the read-path assertion and the per-edge diagnostics.

Unlike :mod:`graflo.db.field_type_support`, nothing here raises.
``directed=False`` is already expressible in shipped manifests and is silently
ignored by seven of the eight targets; refusing it now would reject working
schemas. Callers get diagnostics and decide.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from graflo.architecture.graph_types import EdgeDirection, EdgeId
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge_direction import (
    REVERSE_TRAVERSAL_COST,
    UNDIRECTED_NATIVE_DBS,
    ReverseTraversalCost,
    coerce_db_type,
    db_type_label,
    default_direction_for_edge,
    reverse_traversal_cost,
    supports_native_undirected,
)
from graflo.onto import DBType


class UnsupportedEdgeDirectionError(ValueError):
    """Raised when a backend cannot answer a read in the requested direction.

    Only TigerGraph can reach this: reverse reachability there is fixed when the
    edge type is created (``WITH REVERSE_EDGE``), so no query rewrite recovers it.
    Failing loudly is deliberate — silently returning outgoing edges for an
    ``ANY`` request would under-report the neighbourhood with no signal.
    """


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
            "is stored as a directed row; reaching it from the target side is "
            "affordable only through an index on the target column"
        ),
        "Make sure the edge table's target column is indexed.",
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
        "Declare the edge type as undirected, or give its declared inverse a native inverse.",
    ),
}


def assert_direction_supported(
    db_type: DBType,
    direction: EdgeDirection,
    *,
    has_native_inverse: bool = False,
    edge_is_undirected: bool = False,
) -> None:
    """Raise if ``db_type`` cannot answer a read in ``direction``.

    Args:
        db_type: Backend being queried.
        direction: Requested orientation.
        has_native_inverse: Whether the database maintains the edge's declared
            inverse as a paired type (``DatabaseProfile.native_inverses``). Only consulted on backends
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
    coerced = coerce_db_type(db_type)
    if REVERSE_TRAVERSAL_COST.get(coerced) is not ReverseTraversalCost.SCHEMA_TIME_ONLY:
        return
    if has_native_inverse:
        return
    if edge_is_undirected and coerced in UNDIRECTED_NATIVE_DBS:
        return
    raise UnsupportedEdgeDirectionError(
        f"Backend '{db_type_label(db_type)}' cannot read edges with direction "
        f"'{direction.value}': reverse reachability is fixed when the edge type "
        "is created and no query rewrite recovers it. Declare the edge type as "
        "undirected (`directed: false`), or declare its inverse in "
        "`edge_config.inverses` and list its relation in `db_profile.native_inverses`."
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
    coerced = coerce_db_type(db_type)
    if coerced not in REVERSE_TRAVERSAL_COST:
        return []
    if coerced in UNDIRECTED_NATIVE_DBS:
        return []

    cost = REVERSE_TRAVERSAL_COST[coerced]
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
    label = db_type_label(db_type)
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
