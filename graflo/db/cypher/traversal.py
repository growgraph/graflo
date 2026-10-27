"""Native variable-length traversal for the Cypher family.

Neo4j, Memgraph and FalkorDB all express a bounded neighbourhood as a single
variable-length pattern, so one builder serves all three — the same reason
:func:`cypher_rel_pattern` is shared.

A native override is not only a performance choice here. ``fetch_edges`` on this
family returns ``RETURN r``, and a driver renders a bare relationship without its
endpoints, so the generic breadth-first default has nothing to walk to. The query
returns the reached nodes and the relationships of the paths that reached them,
and both are shaped as the default would return them: vertices by type, edge rows
by edge id with their ends under ``source`` and ``target``.

Anchor values always travel as query parameters (``$anchor_id``, then
``$anchor_id_1`` … for a composite key), never in the query text: they come from
a request, and interpolating them would let a caller rewrite the query. Filter and guard values travel as bound parameters too.

An edge filter holds for every relationship of a path, and a guard for every
node and relationship of it, the anchor included: a path through a hidden
element reaches nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from graflo.architecture.graph_types import EdgeDirection, EdgeId, GraphContainer
from graflo.db.cypher.direction import cypher_rel_pattern
from graflo.db.cypher.escape import cypher_name
from graflo.filter.onto import BoundParams, FilterExpression, parse_filter_expression
from graflo.onto import ExpressionFlavor

if TYPE_CHECKING:
    from graflo.architecture.schema import Schema
    from graflo.architecture.schema.edge import Edge
    from graflo.db.conn import Connection

logger = logging.getLogger(__name__)

#: Name of the query parameter carrying the anchor's key value.
ANCHOR_PARAM = "anchor_id"

#: The columns :func:`cypher_neighbors_query` returns, in order, for a driver
#: that hands rows back as positional lists.
NEIGHBOR_COLUMNS = ("far", "labels", "edges")

#: One rendered pattern: relationship type(s), far storage label, far logical
#: type, and the direction followed. A far type of None means "read it from the
#: reached node's labels".
_NeighborQuery = tuple[str | tuple[str, ...], str | None, str | None, EdgeDirection]


def anchor_param(position: int) -> str:
    """Name of the query parameter binding the anchor's *position*-th key field."""
    return ANCHOR_PARAM if position == 0 else f"{ANCHOR_PARAM}_{position}"


def cypher_neighbors_query(
    *,
    anchor_label: str,
    anchor_key_field: str | Sequence[str] = "id",
    edge_type: str | Sequence[str] | None,
    far_label: str | None,
    direction: EdgeDirection,
    hops: int,
    limit: int | None,
    conditions: Sequence[str] = (),
) -> str:
    """Render a bounded neighbourhood query.

    The anchor values are not part of the text: bind the first as
    ``$anchor_id`` and each further one as ``$anchor_id_<n>``
    (:func:`anchor_param`) when running the query.

    Returns one row per reached node: its properties (``far``), its labels, and
    the relationships of every path that reached it (``edges``: ``type``,
    ``properties``, and each end's properties and labels). Grouping by the node
    is load-bearing: a graph with a cycle reaches the same node by several
    paths, and without it the row count, and what ``LIMIT`` counts, grows with
    path multiplicity rather than with neighbourhood size. The anchor itself is
    excluded, since a walk in both directions can come back to it.

    Args:
        anchor_label: Storage label of the anchor node.
        anchor_key_field: Property, or properties, the anchor is matched on.
        edge_type: Relationship type to follow, several to follow any of
            them, or None for any type.
        far_label: Storage label the reached node must carry, or None for any.
        direction: Orientation followed from the anchor.
        hops: Maximum hop distance.
        limit: Maximum reached nodes.
        conditions: Further predicates over ``path``, ``anchor`` and ``far``,
            ANDed into the ``WHERE`` clause.
    """
    if hops < 1:
        raise ValueError(f"hops must be >= 1, got {hops}")
    where = " AND ".join(["far <> anchor", *conditions])
    pattern = cypher_rel_pattern(edge_type, direction, min_hops=1, max_hops=hops)
    far = f"(far:{cypher_name(far_label)})" if far_label else "(far)"
    limit_clause = f"\nLIMIT {int(limit)}" if limit is not None else ""
    fields = (
        [anchor_key_field] if isinstance(anchor_key_field, str) else anchor_key_field
    )
    key = ", ".join(
        f"{cypher_name(field, quoted=True)}: ${anchor_param(i)}"
        for i, field in enumerate(fields)
    )
    anchor = cypher_name(anchor_label)
    far_column, labels_column, edges_column = NEIGHBOR_COLUMNS
    return (
        f"MATCH path = (anchor:{anchor} {{{key}}})"
        f"{pattern}{far}\n"
        f"WHERE {where}\n"
        "UNWIND relationships(path) AS _r\n"
        "WITH far, collect(DISTINCT _r) AS _rs\n"
        f"RETURN properties(far) AS {far_column}, labels(far) AS {labels_column}, "
        "[_e IN _rs | {type: type(_e), properties: properties(_e), "
        "start: properties(startNode(_e)), start_labels: labels(startNode(_e)), "
        "end: properties(endNode(_e)), end_labels: labels(endNode(_e))}] "
        f"AS {edges_column}"
        f"{limit_clause}"
    )


def path_conditions(
    params: BoundParams,
    *,
    filters: Any | None = None,
    guard: FilterExpression | None = None,
) -> list[str]:
    """The ``WHERE`` predicates an edge filter and a guard put on ``path``.

    Values are bound into *params*.
    """
    kind = ExpressionFlavor.CYPHER
    conditions: list[str] = []
    if filters is not None:
        edge_filter = parse_filter_expression(filters)
        conditions.append(
            "all(_gr IN relationships(path) WHERE "
            f"{edge_filter(doc_name='_gr', kind=kind, params=params)})"
        )
    if guard is not None:
        conditions.append(
            "all(_gn IN nodes(path) WHERE "
            f"{guard(doc_name='_gn', kind=kind, params=params)})"
        )
        conditions.append(
            "all(_gr IN relationships(path) WHERE "
            f"{guard(doc_name='_gr', kind=kind, params=params)})"
        )
    return conditions


#: An anchor as the properties it is matched on, with the value bound to each.
_Anchor = list[tuple[str, Any]]


def _anchor_key(
    schema: Schema, db_aware: Any, vertex_type: str, key: str | dict[str, Any]
) -> _Anchor:
    """The properties to match the anchor on, and the values to bind.

    A mapping key names its own properties, which must be ones the schema
    declares for *vertex_type*: the names are written into the query, so
    arbitrary ones from a request would be an injection point. Which field sets
    form a key is decided above the backend, by ``Connection.graph_neighbors``.
    """
    identity_fields = db_aware.vertex_config.identity_fields(vertex_type)
    if not isinstance(key, dict):
        return [(identity_fields[0] if identity_fields else "id", key)]
    from graflo.db.traversal import check_anchor_fields

    check_anchor_fields(schema, db_aware, vertex_type, list(key))
    return list(key.items())


def _frozen(anchor: _Anchor) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(anchor, key=lambda pair: pair[0]))


def _doc_anchor(fields: Sequence[str], doc: dict[str, Any], address: str) -> _Anchor:
    """A reached node as the next hop's anchor: its full identity when stored."""
    if fields and all(doc.get(field) is not None for field in fields):
        return [(field, doc[field]) for field in fields]
    return [(fields[0] if fields else "id", address)]


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
    filters: Any | None = None,
    guard: FilterExpression | None = None,
) -> GraphContainer:
    """Run a bounded neighbourhood query and shape it as a ``GraphContainer``.

    Shared by Neo4j, Memgraph and FalkorDB; each supplies *run*, which is the
    only thing that differs between their drivers.

    One hop fans out one pattern per relation that touches the anchor type.
    More than one hop issues a single pattern over every allowed relation at
    once, so a walk can cross relation types — work order → machine → line →
    product — which a pattern per relation never could. The reached node's type
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
        key: Anchor identity value, or a mapping of declared properties to
            match, every one bound (a composite identity names them all).
        hops: Maximum hop distance.
        direction: Orientation followed from the anchor.
        edge_types: Logical relation names to restrict to; None means every
            relation. A declared inverse that stores nothing reads the forward
            edge from its target.
        limit: Maximum reached nodes.
        schema: Required for logical -> storage naming.
        run: Executes a query string with its parameters and returns rows as
            dicts carrying ``far`` (the node's properties) and ``labels``.
        filters: Edge filter, in stored names, that every relationship of a
            path must satisfy.
        guard: Trusted predicate, in stored names, that every node and
            relationship of a path must satisfy.

    Returns:
        GraphContainer: reached vertices, keyed by logical type.
    """
    from graflo.architecture.schema.edge_direction import reversed_direction
    from graflo.db.traversal import (
        _edge_direction_for,
        _row_marker,
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
    anchor = _anchor_key(schema, db_aware, vertex_type, key)

    # Direction is decided per edge, as in the generic default: an undirected
    # edge is followed both ways whatever the caller asked for, and a relation
    # read through its declared inverse is followed from the other end.
    selected = [
        (edge, read_as, storage)
        for edge, read_as in select_edges(schema, edge_types)
        if (storage := edge_query_name(db_aware, edge, conn.flavor)) is not None
    ]
    edges = [
        (
            edge,
            _edge_direction_for(
                edge,
                direction if read_as is None else reversed_direction(direction),
            ),
            storage,
        )
        for edge, read_as, storage in selected
    ]
    # A returned relationship is matched back to the selected edge by its stored
    # type and its two ends; one stored edge read under two names reports twice.
    by_relationship: dict[tuple[str, str, str], list[tuple[Edge, str | None]]] = {}
    for edge, read_as, storage in selected:
        by_relationship.setdefault((storage, edge.source, edge.target), []).append(
            (edge, read_as)
        )
    by_label = {
        db_aware.vertex_config.vertex_dbname(name): name
        for name in vertex_config.vertex_set
    }

    def type_of(labels: Sequence[str] | None) -> str | None:
        return next(
            (by_label[label] for label in labels or () if label in by_label), None
        )

    def reached(
        anchor_type: str,
        at: _Anchor,
        queries: list[_NeighborQuery],
        max_hops: int,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Run *queries* from one anchor; reached docs with their logical type."""
        out: list[tuple[str, dict[str, Any]]] = []
        for storage, far_label, far_type, effective in queries:
            # Rendered outside the try: a filter or guard that cannot be
            # written must fail the read, not narrow it silently.
            params = BoundParams(ExpressionFlavor.CYPHER)
            query = cypher_neighbors_query(
                anchor_label=db_aware.vertex_config.vertex_dbname(anchor_type),
                anchor_key_field=[field for field, _value in at],
                edge_type=storage,
                far_label=far_label,
                direction=effective,
                hops=max_hops,
                limit=limit,
                conditions=path_conditions(params, filters=filters, guard=guard),
            )
            try:
                bound = {anchor_param(i): value for i, (_f, value) in enumerate(at)}
                rows = run(query, {**bound, **params.values})
            except Exception:
                if filters is not None or guard is not None:
                    # A constrained read that failed is not an empty answer.
                    raise
                logger.exception(
                    "cypher graph_neighbors failed for edge type %s", storage
                )
                continue
            for row in rows:
                admit_edges(row.get("edges") or [])
                doc = row.get("far")
                if not isinstance(doc, dict):
                    continue
                row_type = far_type or type_of(row.get("labels"))
                if row_type is not None:
                    out.append((row_type, doc))
        return out

    def admit_edges(found: list[dict[str, Any]]) -> None:
        """Add the returned relationships as edge rows, shaped as the default walk's.

        A relationship read through a declared inverse is reported under that
        name, its ends in that reading's order. At most *limit* rows are kept,
        as ``graph_neighbors`` promises.
        """
        for rel in found:
            if limit is not None and len(seen_edges) >= limit:
                return
            start_type = type_of(rel.get("start_labels"))
            end_type = type_of(rel.get("end_labels"))
            if start_type is None or end_type is None:
                continue
            start = _vertex_identity_value(conn, schema, start_type, rel["start"])
            end = _vertex_identity_value(conn, schema, end_type, rel["end"])
            properties = dict(rel.get("properties") or {})
            for edge, read_as in by_relationship.get(
                (rel.get("type") or "", start_type, end_type), []
            ):
                edge_id, near, far_end = (
                    (edge.edge_id, start, end)
                    if read_as is None
                    else ((edge.target, edge.source, read_as), end, start)
                )
                marker = (edge_id, _row_marker(properties, near, far_end))
                if marker in seen_edges:
                    continue
                seen_edges.add(marker)
                container.edges.setdefault(edge_id, []).append(
                    {**properties, "source": near, "target": far_end}
                )

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
    # Keyed by the full identity: two vertices of a composite identity can share
    # its first field, and an address built from that alone would merge them.
    seen: set[tuple[str, tuple[tuple[str, Any], ...]]] = set()
    seen_edges: set[tuple[EdgeId, str]] = set()

    def admit(
        found: list[tuple[str, dict[str, Any]]],
    ) -> list[tuple[str, _Anchor]]:
        """Add unseen docs to the container; their (type, anchor) for the next hop."""
        fresh: list[tuple[str, _Anchor]] = []
        for row_type, doc in found:
            address = _vertex_identity_value(conn, schema, row_type, doc)
            if address is None:
                continue
            fields = db_aware.vertex_config.identity_fields(row_type)
            at = _doc_anchor(fields, doc, address)
            if (row_type, _frozen(at)) in seen:
                continue
            seen.add((row_type, _frozen(at)))
            container.vertices.setdefault(row_type, []).append(doc)
            fresh.append((row_type, at))
        return fresh

    directions = {effective for _edge, effective, _storage in edges}
    if hops == 1:
        admit(reached(vertex_type, anchor, one_hop_queries(vertex_type), 1))
    elif len(directions) <= 1:
        # One variable-length pattern over every allowed relation at once. A
        # far type of None means "read it from the reached node's labels".
        storages = sorted({storage for _edge, _effective, storage in edges})
        if storages:
            effective = next(iter(directions))
            admit(
                reached(
                    vertex_type,
                    anchor,
                    [(tuple(storages), None, None, effective)],
                    hops,
                )
            )
    else:
        # A variable-length pattern carries one direction for all its types, so
        # relations that disagree (a directed walk crossing an undirected edge)
        # are walked hop by hop, each relation in its own direction.
        # Each one-hop pattern excludes only its own anchor, so a later hop
        # could walk back to the start; the single-pattern path never returns it.
        seen.add((vertex_type, _frozen(anchor)))
        frontier = admit(reached(vertex_type, anchor, one_hop_queries(vertex_type), 1))
        for _hop in range(hops - 1):
            next_frontier: list[tuple[str, _Anchor]] = []
            for far_type, far_anchor in frontier:
                next_frontier += admit(
                    reached(far_type, far_anchor, one_hop_queries(far_type), 1)
                )
            frontier = next_frontier
    container.pick_unique()
    return container
