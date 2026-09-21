"""What each backend can do with edge direction (a capability table, never raises).

``Edge.directed`` and the declared inverses of ``EdgeConfig`` are statements
about the *model*. Backends express them to wildly different degrees, and the
difference matters mostly on the read path: reaching an edge from its target
endpoint is free on some backends, needs an explicit clause on others, and is a
schema-time decision that cannot be retrofitted on TigerGraph.

The table lives beside the schema, below every backend, so that schema-level
reasoning (which realization of a declared inverse suits a target, what an
undirected edge will cost) can consult it without importing a database driver.
:mod:`graflo.db.edge_direction_support` builds the read-path assertions and the
per-edge diagnostics on top of it.
"""

from __future__ import annotations

from enum import StrEnum

from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema.edge import Edge
from graflo.onto import DBType


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
UNDIRECTED_NATIVE_DBS: frozenset[DBType] = frozenset({DBType.TIGERGRAPH})

REVERSE_TRAVERSAL_COST: dict[DBType, ReverseTraversalCost] = {
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
    # Edge tables carry `source_id` / `target_id`, and every edge table gets an
    # index on `target_id` when it is defined, so both lookups are indexed.
    DBType.POSTGRES: ReverseTraversalCost.FREE,
    # Edge batches are partitioned by (source, target, relation).
    DBType.GRAFLO_BACKEND: ReverseTraversalCost.MATERIALIZATION_REQUIRED,
}


def coerce_db_type(db_type: DBType) -> DBType:
    """Accept the bare strings that reach these helpers from validated config."""
    if isinstance(db_type, DBType):
        return db_type
    try:
        return DBType(db_type)
    except ValueError:
        return db_type


def db_type_label(db_type: DBType) -> str:
    """Printable backend name, whether ``db_type`` is the enum or a bare string."""
    # ``db_flavor`` arrives as a bare string from some validated config models,
    # so the enum's ``.value`` is not always there to read.
    return str(getattr(db_type, "value", db_type))


def supports_native_undirected(db_type: DBType) -> bool:
    """Whether the backend has an undirected edge *type* in its schema language."""
    return coerce_db_type(db_type) in UNDIRECTED_NATIVE_DBS


def reverse_traversal_cost(db_type: DBType) -> ReverseTraversalCost:
    """What it costs to reach an edge from its target endpoint on ``db_type``.

    Raises:
        KeyError: if ``db_type`` is not a supported write target.
    """
    coerced = coerce_db_type(db_type)
    try:
        return REVERSE_TRAVERSAL_COST[coerced]
    except KeyError:
        raise KeyError(
            f"No reverse-traversal cost recorded for backend '{db_type_label(db_type)}'. "
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


def reversed_direction(direction: EdgeDirection) -> EdgeDirection:
    """``direction`` as seen from the other end of the edge.

    Reading a relation through its declared inverse follows the stored edge from
    its target, so what the caller called outgoing is incoming in storage.
    """
    if direction is EdgeDirection.OUT:
        return EdgeDirection.IN
    if direction is EdgeDirection.IN:
        return EdgeDirection.OUT
    return direction


__all__ = [
    "REVERSE_TRAVERSAL_COST",
    "UNDIRECTED_NATIVE_DBS",
    "ReverseTraversalCost",
    "coerce_db_type",
    "db_type_label",
    "default_direction_for_edge",
    "reverse_traversal_cost",
    "reversed_direction",
    "supports_native_undirected",
]
