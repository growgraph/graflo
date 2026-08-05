"""DB-agnostic read contract: what an agent may ask of a live graph.

Layer 2. Imports `filter/` and `graph_types/` (layer 1) and nothing from `db/` —
enforcement has to be expressible without a driver, or "enforced in core" would
mean "enforced wherever a driver happens to be imported".

Not in `contract/`, which is the write-side manifest at layer 3: mixing a read
contract into it would muddy a boundary the layering test already guards.
"""

from graflo.architecture.query.caps import HARD_CAPS, CapExceededError, QueryCaps
from graflo.architecture.query.models import (
    AggregateQuery,
    GraphQuery,
    NeighborQuery,
    NodeQuery,
    TraverseQuery,
)
from graflo.architecture.query.result import QueryResult

__all__ = [
    "HARD_CAPS",
    "AggregateQuery",
    "CapExceededError",
    "GraphQuery",
    "NeighborQuery",
    "NodeQuery",
    "QueryCaps",
    "QueryResult",
    "TraverseQuery",
]
