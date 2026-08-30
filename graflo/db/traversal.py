"""Backend-neutral multi-hop traversal, composed from single-hop primitives.

Every backend that can answer ``fetch_edges`` gets correct multi-hop semantics
from this module, in one shape: a
:class:`~graflo.architecture.graph_types.container.GraphContainer`. Backends with
a native multi-hop query override :meth:`Connection.graph_neighbors` for a single
round trip, and the conformance suite asserts the override agrees with this
default rather than merely "returning something".

Direction is decided per edge, not per request: an edge declared
``directed=False`` is followed both ways whatever the caller asked for, and a
backend that physically cannot follow the requested direction fails loudly
*before* any query runs, rather than returning a partial neighbourhood.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from graflo.architecture.graph_types import EdgeDirection, EdgeId, GraphContainer
from graflo.architecture.schema import Schema
from graflo.db.edge_direction_support import (
    assert_direction_supported,
)

if TYPE_CHECKING:
    from graflo.architecture.schema.edge import Edge
    from graflo.db.conn import Connection

logger = logging.getLogger(__name__)

#: Hard stop on accumulated edges when the caller sets no limit. Traversal
#: without a bound is how one agent request reads an entire graph.
DEFAULT_EDGE_LIMIT = 1000


def _edge_direction_for(edge: Edge, requested: EdgeDirection) -> EdgeDirection:
    """Direction actually followed for *edge*.

    An undirected edge is bidirectional regardless of the request; this mirrors
    :func:`default_direction_for_edge` on the schema plane so both planes agree
    about what ``directed: false`` means.
    """
    if not edge.directed:
        return EdgeDirection.ANY
    return requested


def edge_query_name(db_aware: Any, edge: Edge, flavor: Any) -> str | None:
    """The identifier a backend's read path uses for *edge*.

    Backends name edge types in incompatible ways, and no single accessor covers
    them: ``edge_storage_name`` is Arango-only by construction (it builds an edge
    *collection* name and returns ``None`` elsewhere), the Cypher family and
    Nebula key on the relation type, and PostgreSQL keys on a derived table name.
    Resolving that here keeps the choice in one place instead of in every caller.
    """
    from graflo.onto import DBType

    if flavor == DBType.POSTGRES:
        from graflo.db.postgres.target_write import edge_table_name

        return edge_table_name(edge.source, edge.target, edge.relation)

    storage = db_aware.edge_config.runtime(edge).storage_name()
    if storage is not None:
        return storage
    return db_aware.edge_config.relation_dbname(edge) or edge.relation


def _far_endpoint(edge_id: EdgeId, anchor_type: str) -> str:
    """The vertex type at the other end of *edge_id* from *anchor_type*."""
    source, target, _relation = edge_id
    return target if source == anchor_type else source


def _incident_edges(
    schema: Schema,
    vertex_type: str,
    *,
    edge_types: Sequence[str] | None,
) -> list[Edge]:
    """Declared edges touching *vertex_type*, honouring a relation allow-list."""
    allowed = set(edge_types) if edge_types is not None else None
    incident: list[Edge] = []
    for edge in schema.core_schema.edge_config.edges:
        if vertex_type not in (edge.source, edge.target):
            continue
        if allowed is not None and edge.relation not in allowed:
            continue
        incident.append(edge)
    return incident


def _vertex_identity_value(
    schema: Schema, vertex_type: str, doc: dict[str, Any]
) -> str | None:
    """Best-effort identity of *doc*, for cycle detection across hops."""
    vertex = schema.core_schema.vertex_config[vertex_type]
    for field in vertex.identity:
        value = doc.get(field)
        if value is not None:
            return str(value)
    for fallback in ("_key", "id", "_id"):
        value = doc.get(fallback)
        if value is not None:
            return str(value)
    return None


def bfs_neighbors(
    conn: Connection,
    *,
    anchor_type: str,
    anchor_key: str | dict[str, Any],
    hops: int = 1,
    direction: EdgeDirection = EdgeDirection.OUT,
    edge_types: Sequence[str] | None = None,
    filters: Any | None = None,
    limit: int | None = None,
    schema: Schema | None = None,
) -> GraphContainer:
    """Breadth-first neighbourhood around one anchor, as a ``GraphContainer``.

    Args:
        conn: Live connection. Only ``fetch_edges`` and ``fetch_docs`` are used —
            never ``execute``, which must stay off any agent-reachable path.
        anchor_type: Logical vertex type of the anchor.
        anchor_key: Anchor identity value, or a field mapping to resolve.
        hops: Maximum hop distance, at least 1.
        direction: Orientations followed from each frontier vertex.
        edge_types: Restrict to these logical relation names.
        filters: Optional edge filter, rendered per backend dialect.
        limit: Maximum accumulated edges. Defaults to ``DEFAULT_EDGE_LIMIT``.
        schema: Required — logical names must be resolved to storage names, or
            the "universal layer" leaks backend naming to the caller.

    Returns:
        GraphContainer: reached vertices and edges, deduplicated.

    Raises:
        ValueError: if *hops* < 1, *schema* is missing, or *anchor_type* is not
            declared in the schema.
        UnsupportedEdgeDirectionError: if a backend cannot follow an edge in the
            requested direction.
    """
    if hops < 1:
        raise ValueError(f"hops must be >= 1, got {hops}")
    if schema is None:
        raise ValueError(
            "graph_neighbors requires a schema: logical vertex and relation names "
            "cannot be resolved to storage names without one"
        )
    if anchor_type not in schema.core_schema.vertex_config.vertex_set:
        raise ValueError(
            f"Unknown vertex type {anchor_type!r}; declared: "
            f"{sorted(schema.core_schema.vertex_config.vertex_set)}"
        )

    max_edges = DEFAULT_EDGE_LIMIT if limit is None else limit
    db_aware = schema.resolve_db_aware(conn.flavor)

    container = GraphContainer()
    anchor_id = _resolve_anchor_id(conn, schema, db_aware, anchor_type, anchor_key)
    if anchor_id is None:
        return container

    visited: set[tuple[str, str]] = {(anchor_type, anchor_id)}
    frontier: list[tuple[str, str]] = [(anchor_type, anchor_id)]
    seen_edges: set[tuple[EdgeId, str]] = set()
    edge_count = 0

    for _hop in range(hops):
        if not frontier or edge_count >= max_edges:
            break
        next_frontier: list[tuple[str, str]] = []
        for current_type, current_id in frontier:
            for edge in _incident_edges(schema, current_type, edge_types=edge_types):
                if edge_count >= max_edges:
                    break
                effective = _edge_direction_for(edge, direction)
                # Assert before querying: a backend that cannot follow this
                # orientation must fail rather than silently return the half it
                # can answer.
                assert_direction_supported(
                    conn.flavor,
                    effective,
                    edge_is_undirected=not edge.directed,
                )
                anchor_side = _anchor_side(edge, current_type, effective)
                if anchor_side is None:
                    continue
                rows = _fetch_edge_rows(
                    conn,
                    db_aware=db_aware,
                    edge=edge,
                    anchor_type=current_type,
                    anchor_id=current_id,
                    direction=anchor_side,
                    filters=filters,
                    remaining=max_edges - edge_count,
                )
                if not rows:
                    continue
                edge_id = edge.edge_id
                far_type = _far_endpoint(edge_id, current_type)
                bucket = container.edges.setdefault(edge_id, [])
                far_ids: list[str] = []
                for row in rows:
                    properties, source_key, target_key = normalize_edge_row(row)
                    marker = (edge_id, _row_marker(properties, source_key, target_key))
                    if marker in seen_edges:
                        continue
                    seen_edges.add(marker)
                    # Normalize the endpoints into the row so a consumer reads
                    # one shape whatever backend answered.
                    bucket.append(
                        {**properties, "source": source_key, "target": target_key}
                    )
                    edge_count += 1
                    far = target_key if source_key == current_id else source_key
                    if far is not None and far != current_id or far is not None:
                        far_ids.append(far)

                for doc in _hydrate_far_endpoints(
                    conn, db_aware, schema, far_type, far_ids
                ):
                    identity = _vertex_identity_value(schema, far_type, doc)
                    if identity is None or (far_type, identity) in visited:
                        continue
                    visited.add((far_type, identity))
                    container.vertices.setdefault(far_type, []).append(doc)
                    next_frontier.append((far_type, identity))
        frontier = next_frontier

    if edge_count >= max_edges:
        logger.debug(
            "graph_neighbors hit the edge limit (%s); result is truncated", max_edges
        )
    container.pick_unique()
    return container


def _anchor_side(
    edge: Edge, anchor_type: str, direction: EdgeDirection
) -> EdgeDirection | None:
    """Direction to pass to ``fetch_edges`` when anchored at *anchor_type*.

    ``fetch_edges`` orients relative to the anchor, so an edge reached from its
    *target* has to be queried inbound even when the caller asked to go out.
    A self-loop is reachable both ways and always uses the requested direction.
    """
    is_source = edge.source == anchor_type
    is_target = edge.target == anchor_type
    if is_source and is_target:
        return direction
    if direction is EdgeDirection.ANY:
        return EdgeDirection.ANY
    if is_source:
        return direction if direction is EdgeDirection.OUT else None
    if is_target:
        return EdgeDirection.IN if direction is EdgeDirection.OUT else None
    return None


def _resolve_anchor_id(
    conn: Connection,
    schema: Schema,
    db_aware: Any,
    anchor_type: str,
    anchor_key: str | dict[str, Any],
) -> str | None:
    """Resolve *anchor_key* to the id string the backend indexes on."""
    if isinstance(anchor_key, str):
        return anchor_key
    storage = db_aware.vertex_config.vertex_dbname(anchor_type)
    leaves = [
        {"field": field, "cmp_operator": "==", "value": value}
        for field, value in anchor_key.items()
    ]
    filters = leaves[0] if len(leaves) == 1 else {"AND": leaves}
    docs = conn.fetch_docs(storage, filters=filters, limit=1)
    if not docs:
        return None
    return _vertex_identity_value(schema, anchor_type, docs[0])


def _fetch_edge_rows(
    conn: Connection,
    *,
    db_aware: Any,
    edge: Edge,
    anchor_type: str,
    anchor_id: str,
    direction: EdgeDirection,
    filters: Any | None,
    remaining: int,
) -> list[dict[str, Any]]:
    """One hop over one declared edge, in storage terms."""
    edge_storage = edge_query_name(db_aware, edge, conn.flavor)
    anchor_storage = db_aware.vertex_config.vertex_dbname(anchor_type)
    far_type = _far_endpoint(edge.edge_id, anchor_type)
    far_storage = db_aware.vertex_config.vertex_dbname(far_type)
    try:
        return list(
            conn.fetch_edges(
                anchor_storage,
                anchor_id,
                edge_type=edge_storage,
                to_type=far_storage,
                filters=filters,
                limit=remaining,
                direction=direction,
            )
            or []
        )
    except NotImplementedError:
        raise
    except Exception:
        logger.exception(
            "graph_neighbors: fetch_edges failed for %s anchored at %s",
            edge.edge_id,
            anchor_id,
        )
        return []


#: Keys under which backends report the two endpoints of an edge row.
#:
#: This is a *name union*, not a per-flavor contract: a dict row is matched
#: against every candidate in order. A backend whose ``fetch_edges`` returns
#: endpoints under some other name resolves to ``(None, None)`` here, and its
#: rows are then dropped from the neighbourhood — so a new backend must either
#: emit one of these names or add its own. :func:`normalize_edge_row` logs when
#: that happens rather than losing the row quietly.
_SOURCE_KEYS = ("_from", "source_id", "src", "_src", "from", "from_id", "_from_key")
_TARGET_KEYS = ("_to", "target_id", "dst", "_dst", "to", "to_id", "_to_key")

#: Rows already reported as unresolvable, keyed by their sorted column names, so
#: a mismatch is reported once per shape instead of once per row.
_reported_unresolved: set[tuple[str, ...]] = set()


def _strip_collection(value: Any) -> str | None:
    """``collection/key`` -> ``key``; anything else stringified."""
    if value is None:
        return None
    text = str(value)
    return text.split("/", 1)[1] if "/" in text else text


def normalize_edge_row(row: Any) -> tuple[dict[str, Any], str | None, str | None]:
    """Reduce a backend's edge row to (properties, source key, target key).

    ``fetch_edges`` predates this wave and returns whatever shape each driver
    finds natural: Arango yields an edge document with ``_from``/``_to``, the
    Cypher family yields the driver's ``(start props, type, end props)`` triple,
    PostgreSQL yields ``source_id``/``target_id`` columns. Normalizing here is
    what lets every backend answer in one ``GraphContainer`` without changing a
    read path other code already depends on.
    """
    if isinstance(row, (tuple, list)):
        # Cypher drivers render a relationship as (start, type, end).
        start = row[0] if len(row) > 0 and isinstance(row[0], dict) else {}
        end = row[2] if len(row) > 2 and isinstance(row[2], dict) else {}
        properties = row[1] if len(row) > 1 and isinstance(row[1], dict) else {}
        return (
            dict(properties),
            _first_present(start, ("id", "_key", "_id")),
            _first_present(end, ("id", "_key", "_id")),
        )
    if isinstance(row, dict):
        source = next(
            (_strip_collection(row[k]) for k in _SOURCE_KEYS if row.get(k) is not None),
            None,
        )
        target = next(
            (_strip_collection(row[k]) for k in _TARGET_KEYS if row.get(k) is not None),
            None,
        )
        if source is None and target is None:
            source = _strip_collection(row.get("_from_key"))
            target = _strip_collection(row.get("_to_key"))
        if source is None and target is None and row:
            # Neither endpoint resolved, so the caller will drop this row from
            # the neighbourhood. Silent loss reads as "no such neighbour", which
            # is indistinguishable from a correct empty result — say so instead.
            shape = tuple(sorted(str(k) for k in row))
            if shape not in _reported_unresolved:
                _reported_unresolved.add(shape)
                logger.warning(
                    "normalize_edge_row: no endpoint keys in edge row with columns "
                    "%s; rows of this shape are dropped from traversal. Expected one "
                    "of %s for the source and %s for the target.",
                    list(shape),
                    list(_SOURCE_KEYS),
                    list(_TARGET_KEYS),
                )
        return dict(row), source, target
    return {}, None, None


def _first_present(doc: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = doc.get(key)
        if value is not None:
            return _strip_collection(value)
    return None


def _match_any(field: str, values: list[str]) -> dict[str, Any]:
    """Filter matching *field* against any of *values*.

    A composite with a single dependency is rejected by ``FilterExpression``
    (only ``NOT`` is unary), so the one-value case has to render as a bare leaf.
    """
    leaves = [
        {"field": field, "cmp_operator": "==", "value": value} for value in values
    ]
    return leaves[0] if len(leaves) == 1 else {"OR": leaves}


def _row_marker(
    properties: dict[str, Any], source: str | None, target: str | None
) -> str:
    """Stable identity for an edge row, for cross-hop deduplication."""
    for key in ("_id", "_key", "id"):
        value = properties.get(key)
        if value is not None:
            return str(value)
    return f"{source}->{target}|" + repr(
        sorted((k, str(v)) for k, v in properties.items())
    )


def _hydrate_far_endpoints(
    conn: Connection,
    db_aware: Any,
    schema: Schema,
    far_type: str,
    ids: list[str],
) -> list[dict[str, Any]]:
    """Fetch the vertex documents for already-normalized far-endpoint *ids*."""
    if not ids:
        return []

    storage = db_aware.vertex_config.vertex_dbname(far_type)
    identity_fields = db_aware.vertex_config.identity_fields(far_type)
    if not identity_fields:
        return []
    key_field = identity_fields[0]
    filters = _match_any(key_field, list(dict.fromkeys(ids)))
    try:
        return list(conn.fetch_docs(storage, filters=filters) or [])
    except Exception:
        logger.exception("graph_neighbors: hydrating %s endpoints failed", far_type)
        return []
