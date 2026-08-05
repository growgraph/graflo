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
    min_hops: int | None = None,
    max_hops: int | None = None,
) -> str:
    """Render the relationship pattern between two node patterns.

    Args:
        edge_type: Relationship type to filter on, or None for any type.
        direction: Orientation followed from the left-hand (anchor) node.
        variable: Relationship variable name bound in the pattern.
        min_hops: Lower bound for a variable-length pattern. Defaults to 1 when
            only *max_hops* is given.
        max_hops: Upper bound for a variable-length pattern. Omitting both keeps
            the single-hop form, which is what every existing caller wants.

    Returns:
        str: e.g. ``-[r:KNOWS]->`` (OUT), ``<-[r:KNOWS]-`` (IN),
        ``-[r:KNOWS]-`` (ANY), ``-[r:KNOWS*1..3]->`` (variable length).

    Raises:
        ValueError: if the hop bounds are non-positive or inverted — an
            unbounded ``*`` pattern is a full-graph scan and is never emitted.
    """
    left, right = _ARROWS[direction]
    quantifier = _hop_quantifier(min_hops, max_hops)
    body = (
        f"[{variable}:{edge_type}{quantifier}]"
        if edge_type
        else f"[{variable}{quantifier}]"
    )
    return f"{left}{body}{right}"


def _hop_quantifier(min_hops: int | None, max_hops: int | None) -> str:
    """Render ``*min..max``, or the empty string for a single hop."""
    if min_hops is None and max_hops is None:
        return ""
    low = 1 if min_hops is None else min_hops
    high = low if max_hops is None else max_hops
    if low < 1:
        raise ValueError(f"min_hops must be >= 1, got {low}")
    if high < low:
        raise ValueError(f"max_hops ({high}) must be >= min_hops ({low})")
    return f"*{low}..{high}"
