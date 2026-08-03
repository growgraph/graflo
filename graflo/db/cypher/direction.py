"""Relationship-pattern direction for Cypher-family backends.

Neo4j, Memgraph and FalkorDB have no undirected relationship *type*, but they
store relationships bidirectionally, so matching without an arrow costs the same
as matching with one. Direction is therefore purely a pattern-rendering
question here — which is why all three share this one helper.
"""

from __future__ import annotations

from graflo.architecture.graph_types import EdgeDirection

_ARROWS: dict[EdgeDirection, tuple[str, str]] = {
    EdgeDirection.OUT: ("-", "->"),
    EdgeDirection.IN: ("<-", "-"),
    EdgeDirection.ANY: ("-", "-"),
}


def cypher_rel_pattern(
    edge_type: str | None,
    direction: EdgeDirection = EdgeDirection.OUT,
    *,
    variable: str = "r",
) -> str:
    """Render the relationship pattern between two node patterns.

    Args:
        edge_type: Relationship type to filter on, or None for any type.
        direction: Orientation followed from the left-hand (anchor) node.
        variable: Relationship variable name bound in the pattern.

    Returns:
        str: e.g. ``-[r:KNOWS]->`` (OUT), ``<-[r:KNOWS]-`` (IN), ``-[r:KNOWS]-`` (ANY).
    """
    left, right = _ARROWS[direction]
    body = f"[{variable}:{edge_type}]" if edge_type else f"[{variable}]"
    return f"{left}{body}{right}"
