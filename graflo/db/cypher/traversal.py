"""Native variable-length traversal for the Cypher family.

Neo4j, Memgraph and FalkorDB all express a bounded neighbourhood as a single
variable-length pattern, so one builder serves all three — the same reason
:func:`cypher_rel_pattern` is shared.

A native override is not only a performance choice here. ``fetch_edges`` on this
family returns ``RETURN r``, and a driver renders a bare relationship without its
endpoints, so the generic breadth-first default has nothing to walk to. Returning
the reached nodes directly is the only way the question is answerable at all.

The anchor value always travels as the query parameter ``$anchor_id``, never in
the query text: it comes from a request, and interpolating it would let a caller
rewrite the query.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.db.cypher.direction import cypher_rel_pattern

if TYPE_CHECKING:
    from graflo.architecture.schema import Schema
    from graflo.db.conn import Connection

logger = logging.getLogger(__name__)

#: Name of the query parameter carrying the anchor's key value.
ANCHOR_PARAM = "anchor_id"

#: One rendered pattern: relationship type(s), far storage label, far logical
#: type, and the direction followed. A far type of None means "read it from the
#: reached node's labels".
_NeighborQuery = tuple[str, str | None, str | None, EdgeDirection]


def _quote_identifier(name: str) -> str:
    """Backtick-quote a Cypher identifier, doubling any embedded backtick."""
    return "`" + name.replace("`", "``") + "`"


def cypher_neighbors_query(
    *,
    anchor_label: str,
    anchor_key_field: str = "id",
    edge_type: str | None,
    far_label: str | None,
    direction: EdgeDirection,
    hops: int,
    limit: int | None,
) -> str:
    """Render a bounded neighbourhood query.

    The anchor value is not part of the text: bind it as the ``$anchor_id``
    parameter when running the query.

    Returns the reached nodes, their distance and their labels, deduplicated.
    ``DISTINCT`` is load-bearing: a graph with a cycle reaches the same node by
    several paths, and without it the row count grows with path multiplicity
    rather than with neighbourhood size. The anchor itself is excluded, since a
    walk in both directions can come back to it.

    Args:
        anchor_label: Storage label of the anchor node.
        anchor_key_field: Property the anchor is matched on.
        edge_type: Relationship type to follow, several joined with ``|``, or
            None for any type.
        far_label: Storage label the reached node must carry, or None for any.
        direction: Orientation followed from the anchor.
        hops: Maximum hop distance.
        limit: Maximum reached nodes.
    """
    if hops < 1:
        raise ValueError(f"hops must be >= 1, got {hops}")
    pattern = cypher_rel_pattern(edge_type, direction, min_hops=1, max_hops=hops)
    far = f"(far:{far_label})" if far_label else "(far)"
    limit_clause = f"\nLIMIT {int(limit)}" if limit is not None else ""
    key = _quote_identifier(anchor_key_field)
    return (
        f"MATCH path = (anchor:{anchor_label} {{{key}: ${ANCHOR_PARAM}}})"
        f"{pattern}{far}\n"
        f"WHERE far <> anchor\n"
        f"RETURN DISTINCT properties(far) AS far, length(path) AS distance, "
        f"labels(far) AS labels"
        f"{limit_clause}"
    )


def _anchor_key(
    schema: Schema, db_aware: Any, vertex_type: str, key: str | dict[str, Any]
) -> tuple[str, Any]:
    """The property to match the anchor on, and the value to bind.

    A mapping key names its own property, which must be one the schema declares
    for *vertex_type*: the name is written into the query, so an arbitrary one
    from a request would be an injection point.
    """
    identity_fields = db_aware.vertex_config.identity_fields(vertex_type)
    if not isinstance(key, dict):
        return (identity_fields[0] if identity_fields else "id"), key
    if len(key) != 1:
        raise ValueError(
            f"Cypher graph_neighbors resolves a single-field anchor key; got {sorted(key)}"
        )
    from graflo.db.traversal import check_anchor_fields

    field, value = next(iter(key.items()))
    check_anchor_fields(schema, db_aware, vertex_type, [field])
    return field, value


def cypher_graph_neighbors(
    conn: Connection,
    *,
    vertex_type: str,
    key: str | dict[str, Any],
    hops: int,
    direction: EdgeDirection,
    edge_types: Sequence[str] | None,
    limit: int | None,
    schema: Schema | None,
    run: Callable[[str, dict[str, Any]], list[dict[str, Any]]],
) -> GraphContainer:
    """Run a bounded neighbourhood query and shape it as a ``GraphContainer``.

    Shared by Neo4j, Memgraph and FalkorDB; each supplies *run*, which is the
    only thing that differs between their drivers.

    One hop fans out one pattern per relation that touches the anchor type.
    More than one hop issues a single pattern over every allowed relation at
    once, so a walk can cross relation types — change → server → application →
    service — which a pattern per relation never could. The reached node's type
    is then read from its labels.

    Direction is decided per edge, exactly as in the backend-neutral default
    (:func:`graflo.db.traversal.bfs_neighbors`): an edge declared
    ``directed: false`` is followed both ways whatever *direction* says. A
    variable-length pattern has one direction for all its relationship types, so
    when the selected relations disagree the walk falls back to one-hop patterns
    issued hop by hop.

    Args:
        conn: The live connection, for flavor-aware name resolution.
        vertex_type: Logical anchor type.
        key: Anchor identity value, or a single-field mapping naming a declared
            property.
        hops: Maximum hop distance.
        direction: Orientation followed from the anchor.
        edge_types: Logical relation names to restrict to; None means every
            relation. A declared inverse that stores nothing reads the forward
            edge from its target.
        limit: Maximum reached nodes.
        schema: Required for logical -> storage naming.
        run: Executes a query string with its parameters and returns rows as
            dicts carrying ``far`` (the node's properties) and ``labels``.

    Returns:
        GraphContainer: reached vertices, keyed by logical type.
    """
    from graflo.architecture.schema.edge_direction import reversed_direction
    from graflo.db.traversal import (
        _edge_direction_for,
        _vertex_identity_value,
        edge_query_name,
        select_edges,
    )

    if hops < 1:
        raise ValueError(f"hops must be >= 1, got {hops}")
    if schema is None:
        raise ValueError(
            "graph_neighbors requires a schema: logical vertex and relation names "
            "cannot be resolved to storage names without one"
        )
    vertex_config = schema.core_schema.vertex_config
    if vertex_type not in vertex_config.vertex_set:
        raise ValueError(
            f"Unknown vertex type {vertex_type!r}; declared: "
            f"{sorted(vertex_config.vertex_set)}"
        )

    db_aware = schema.resolve_db_aware(conn.flavor)
    key_field, anchor_value = _anchor_key(schema, db_aware, vertex_type, key)

    # Direction is decided per edge, as in the generic default: an undirected
    # edge is followed both ways whatever the caller asked for, and a relation
    # read through its declared inverse is followed from the other end.
    edges = [
        (
            edge,
            _edge_direction_for(
                edge,
                direction if read_as is None else reversed_direction(direction),
            ),
            storage,
        )
        for edge, read_as in select_edges(schema, edge_types)
        if (storage := edge_query_name(db_aware, edge, conn.flavor)) is not None
    ]
    by_label = {
        db_aware.vertex_config.vertex_dbname(name): name
        for name in vertex_config.vertex_set
    }

    def reached(
        anchor_type: str,
        field: str,
        value: Any,
        queries: list[_NeighborQuery],
        max_hops: int,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Run *queries* from one anchor; reached docs with their logical type."""
        out: list[tuple[str, dict[str, Any]]] = []
        for storage, far_label, far_type, effective in queries:
            query = cypher_neighbors_query(
                anchor_label=db_aware.vertex_config.vertex_dbname(anchor_type),
                anchor_key_field=field,
                edge_type=storage,
                far_label=far_label,
                direction=effective,
                hops=max_hops,
                limit=limit,
            )
            try:
                rows = run(query, {ANCHOR_PARAM: value})
            except Exception:
                logger.exception(
                    "cypher graph_neighbors failed for edge type %s", storage
                )
                continue
            for row in rows:
                doc = row.get("far")
                if not isinstance(doc, dict):
                    continue
                row_type = far_type or next(
                    (
                        by_label[label]
                        for label in row.get("labels") or ()
                        if label in by_label
                    ),
                    None,
                )
                if row_type is not None:
                    out.append((row_type, doc))
        return out

    def one_hop_queries(anchor_type: str) -> list[_NeighborQuery]:
        """One pattern per relation touching *anchor_type*, each in its own direction."""
        return [
            (
                storage,
                db_aware.vertex_config.vertex_dbname(far_type),
                far_type,
                effective,
            )
            for edge, effective, storage in edges
            if anchor_type in (edge.source, edge.target)
            for far_type in [edge.target if edge.source == anchor_type else edge.source]
        ]

    container = GraphContainer()
    seen: set[tuple[str, str]] = set()

    def admit(found: list[tuple[str, dict[str, Any]]]) -> list[tuple[str, str]]:
        """Add unseen docs to the container; their (type, identity) for the next hop."""
        fresh: list[tuple[str, str]] = []
        for row_type, doc in found:
            identity = _vertex_identity_value(conn, schema, row_type, doc)
            if identity is None or (row_type, identity) in seen:
                continue
            seen.add((row_type, identity))
            container.vertices.setdefault(row_type, []).append(doc)
            fresh.append((row_type, identity))
        return fresh

    directions = {effective for _edge, effective, _storage in edges}
    if hops == 1:
        admit(
            reached(
                vertex_type, key_field, anchor_value, one_hop_queries(vertex_type), 1
            )
        )
    elif len(directions) <= 1:
        # One variable-length pattern over every allowed relation at once. A
        # far type of None means "read it from the reached node's labels".
        storages = sorted({storage for _edge, _effective, storage in edges})
        if storages:
            effective = next(iter(directions))
            admit(
                reached(
                    vertex_type,
                    key_field,
                    anchor_value,
                    [("|".join(storages), None, None, effective)],
                    hops,
                )
            )
    else:
        # A variable-length pattern carries one direction for all its types, so
        # relations that disagree (a directed walk crossing an undirected edge)
        # are walked hop by hop, each relation in its own direction.
        # Each one-hop pattern excludes only its own anchor, so a later hop
        # could walk back to the start; the single-pattern path never returns it.
        seen.add((vertex_type, str(anchor_value)))
        frontier = admit(
            reached(
                vertex_type, key_field, anchor_value, one_hop_queries(vertex_type), 1
            )
        )
        for _hop in range(hops - 1):
            next_frontier: list[tuple[str, str]] = []
            for far_type, identity in frontier:
                fields = db_aware.vertex_config.identity_fields(far_type)
                next_frontier += admit(
                    reached(
                        far_type,
                        fields[0] if fields else "id",
                        identity,
                        one_hop_queries(far_type),
                        1,
                    )
                )
            frontier = next_frontier
    container.pick_unique()
    return container
