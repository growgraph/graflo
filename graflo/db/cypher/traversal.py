"""Native variable-length traversal for the Cypher family.

Neo4j, Memgraph and FalkorDB all express a bounded neighbourhood as a single
variable-length pattern, so one builder serves all three — the same reason
:func:`cypher_rel_pattern` is shared.

A native override is not only a performance choice here. ``fetch_edges`` on this
family returns ``RETURN r``, and a driver renders a bare relationship without its
endpoints, so the generic breadth-first default has nothing to walk to. Returning
the reached nodes directly is the only way the question is answerable at all.
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


def cypher_neighbors_query(
    *,
    anchor_label: str,
    anchor_id: str,
    anchor_key_field: str = "id",
    edge_type: str | None,
    far_label: str | None,
    direction: EdgeDirection,
    hops: int,
    limit: int | None,
) -> str:
    """Render a bounded neighbourhood query.

    Returns the reached nodes and their distance, deduplicated. ``DISTINCT`` is
    load-bearing: a graph with a cycle reaches the same node by several paths,
    and without it the row count grows with path multiplicity rather than with
    neighbourhood size.
    """
    if hops < 1:
        raise ValueError(f"hops must be >= 1, got {hops}")
    pattern = cypher_rel_pattern(edge_type, direction, min_hops=1, max_hops=hops)
    far = f"(far:{far_label})" if far_label else "(far)"
    limit_clause = f"\nLIMIT {int(limit)}" if limit is not None else ""
    return (
        f"MATCH path = (anchor:{anchor_label} "
        f"{{{anchor_key_field}: '{anchor_id}'}}){pattern}{far}\n"
        f"RETURN DISTINCT properties(far) AS far, length(path) AS distance"
        f"{limit_clause}"
    )


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
    run: Callable[[str], list[dict[str, Any]]],
) -> GraphContainer:
    """Run a bounded neighbourhood query and shape it as a ``GraphContainer``.

    Shared by Neo4j, Memgraph and FalkorDB; each supplies *run*, which is the
    only thing that differs between their drivers.

    Args:
        conn: The live connection, for flavor-aware name resolution.
        vertex_type: Logical anchor type.
        key: Anchor identity value, or a single-field mapping.
        hops: Maximum hop distance.
        direction: Orientation followed from the anchor.
        edge_types: Logical relation names to restrict to. One pattern is issued
            per allowed relation, since a variable-length pattern takes a single
            relationship type.
        limit: Maximum reached nodes.
        schema: Required for logical -> storage naming.
        run: Executes a query string and returns rows as dicts.

    Returns:
        GraphContainer: reached vertices, keyed by logical type.
    """
    from graflo.db.traversal import _vertex_identity_value, edge_query_name

    if hops < 1:
        raise ValueError(f"hops must be >= 1, got {hops}")
    if schema is None:
        raise ValueError(
            "graph_neighbors requires a schema: logical vertex and relation names "
            "cannot be resolved to storage names without one"
        )
    if vertex_type not in schema.core_schema.vertex_config.vertex_set:
        raise ValueError(
            f"Unknown vertex type {vertex_type!r}; declared: "
            f"{sorted(schema.core_schema.vertex_config.vertex_set)}"
        )

    db_aware = schema.resolve_db_aware(conn.flavor)
    anchor_label = db_aware.vertex_config.vertex_dbname(vertex_type)
    identity_fields = db_aware.vertex_config.identity_fields(vertex_type)
    key_field = identity_fields[0] if identity_fields else "id"
    if isinstance(key, dict):
        if len(key) != 1:
            raise ValueError(
                "Cypher graph_neighbors resolves a single-field anchor key; "
                f"got {sorted(key)}"
            )
        key_field, anchor_id = next(iter(key.items()))
        anchor_id = str(anchor_id)
    else:
        anchor_id = key

    allowed = set(edge_types) if edge_types is not None else None
    storage_edges: list[tuple[str, str]] = []
    for edge in schema.core_schema.edge_config.edges:
        if vertex_type not in (edge.source, edge.target):
            continue
        if allowed is not None and edge.relation not in allowed:
            continue
        storage = edge_query_name(db_aware, edge, conn.flavor)
        if storage is None:
            continue
        far_type = edge.target if edge.source == vertex_type else edge.source
        storage_edges.append((storage, far_type))

    container = GraphContainer()
    seen: set[tuple[str, str]] = {(vertex_type, str(anchor_id))}
    for storage, far_type in storage_edges:
        query = cypher_neighbors_query(
            anchor_label=anchor_label,
            anchor_id=str(anchor_id),
            anchor_key_field=key_field,
            edge_type=storage,
            far_label=db_aware.vertex_config.vertex_dbname(far_type),
            direction=direction,
            hops=hops,
            limit=limit,
        )
        try:
            rows = run(query)
        except Exception:
            logger.exception("cypher graph_neighbors failed for edge type %s", storage)
            continue
        for row in rows:
            doc = row.get("far")
            if not isinstance(doc, dict):
                continue
            identity = _vertex_identity_value(schema, far_type, doc)
            if identity is None or (far_type, identity) in seen:
                continue
            seen.add((far_type, identity))
            container.vertices.setdefault(far_type, []).append(doc)
    container.pick_unique()
    return container
