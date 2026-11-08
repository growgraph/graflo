"""Query builders for NebulaGraph nGQL (v3.x) and ISO GQL (v5.x).

Each public function returns a query *string* ready to be passed to the
adapter's ``execute`` method.
"""

from __future__ import annotations

from collections.abc import Hashable, Iterable, Sequence
from typing import Any, TypeVar

from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema.vertex import Field
from graflo.db.nebula.util import (
    VID_SEPARATOR,
    escape_nebula_string,
    make_vid,
    nebula_type_for_field,
    serialize_nebula_value,
)

_K = TypeVar("_K", bound=Hashable)

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------


def create_space_ngql(
    space_name: str,
    *,
    vid_type: str,
    partition_num: int,
    replica_factor: int,
) -> str:
    return (
        f"CREATE SPACE IF NOT EXISTS `{space_name}` "
        f"(vid_type={vid_type}, partition_num={partition_num}, "
        f"replica_factor={replica_factor})"
    )


def drop_space_ngql(space_name: str) -> str:
    return f"DROP SPACE IF EXISTS `{space_name}`"


def create_tag_ngql(tag_name: str, fields: list[Field]) -> str:
    """``CREATE TAG IF NOT EXISTS Tag(prop type, ...)``."""
    if fields:
        cols = ", ".join(f"`{f.name}` {nebula_type_for_field(f)}" for f in fields)
        return f"CREATE TAG IF NOT EXISTS `{tag_name}` ({cols})"
    return f"CREATE TAG IF NOT EXISTS `{tag_name}` ()"


def create_edge_type_ngql(edge_type: str, fields: list[Field] | None = None) -> str:
    """``CREATE EDGE IF NOT EXISTS EdgeType(prop type, ...)``."""
    if fields:
        cols = ", ".join(f"`{f.name}` {nebula_type_for_field(f)}" for f in fields)
        return f"CREATE EDGE IF NOT EXISTS `{edge_type}` ({cols})"
    return f"CREATE EDGE IF NOT EXISTS `{edge_type}` ()"


def create_tag_index_ngql(
    index_name: str,
    tag_name: str,
    index_fields: list[str],
    string_index_length: int = 256,
    string_fields: set[str] | None = None,
) -> str:
    """``CREATE TAG INDEX IF NOT EXISTS idx ON tag(field(len), ...)``."""
    parts: list[str] = []
    for f in index_fields:
        if string_fields is None or f in string_fields:
            parts.append(f"`{f}`({string_index_length})")
        else:
            parts.append(f"`{f}`")
    fields_str = ", ".join(parts)
    return (
        f"CREATE TAG INDEX IF NOT EXISTS `{index_name}` ON `{tag_name}` ({fields_str})"
    )


def create_edge_index_ngql(
    index_name: str,
    edge_type: str,
    index_fields: list[str],
    string_index_length: int = 256,
) -> str:
    """``CREATE EDGE INDEX IF NOT EXISTS idx ON edge_type(field(len), ...)``."""
    parts: list[str] = []
    for f in index_fields:
        parts.append(f"`{f}`({string_index_length})")
    fields_str = ", ".join(parts)
    return (
        f"CREATE EDGE INDEX IF NOT EXISTS `{index_name}` "
        f"ON `{edge_type}` ({fields_str})"
    )


# ---------------------------------------------------------------------------
# DML – vertex operations (nGQL)
# ---------------------------------------------------------------------------


def upsert_vertex_ngql(
    tag_name: str,
    vid: str,
    props: dict[str, Any],
    tag_fields: list[str],
) -> str:
    """``UPSERT VERTEX ON tag "vid" SET prop=val, ...``."""
    set_parts = [
        f"`{k}` = {serialize_nebula_value(v)}"
        for k, v in props.items()
        if k in tag_fields
    ]
    if not set_parts:
        return ""
    set_clause = ", ".join(set_parts)
    escaped_vid = escape_nebula_string(vid)
    return f'UPSERT VERTEX ON `{tag_name}` "{escaped_vid}" SET {set_clause}'


def batch_upsert_vertices_ngql(
    tag_name: str,
    docs: list[dict[str, Any]],
    match_keys: list[str] | tuple[str, ...],
    tag_fields: list[str],
) -> list[str]:
    """Return a list of ``UPSERT VERTEX`` statements for a document batch."""
    statements: list[str] = []
    for doc in docs:
        vid = make_vid(tag_name, doc, match_keys)
        stmt = upsert_vertex_ngql(tag_name, vid, doc, tag_fields)
        if stmt:
            statements.append(stmt)
    return statements


def _values_statements(head: str, values: Iterable[str], max_bytes: int) -> list[str]:
    """``head VALUES v1, v2, …`` split on value boundaries, each at most *max_bytes*.

    Raises:
        ValueError: One value alone does not fit.
    """
    prefix = f"{head} VALUES "
    base = len(prefix.encode())
    statements: list[str] = []
    batch: list[str] = []
    size = base
    for value in values:
        width = len(value.encode())
        if base + width > max_bytes:
            raise ValueError(
                f"one row of {head!r} is {width} bytes, over the "
                f"{max_bytes}-byte statement limit"
            )
        if batch and size + 2 + width > max_bytes:
            statements.append(prefix + ", ".join(batch))
            batch, size = [], base
        size += width + (2 if batch else 0)
        batch.append(value)
    if batch:
        statements.append(prefix + ", ".join(batch))
    return statements


def _columns(columns: Sequence[str]) -> str:
    return ", ".join(f"`{c}`" for c in columns)


def _tuple(doc: dict[str, Any], columns: Sequence[str]) -> str:
    return "(" + ", ".join(serialize_nebula_value(doc.get(c)) for c in columns) + ")"


def insert_vertices_ngql(
    tag_name: str,
    columns: Sequence[str],
    rows: Iterable[tuple[str, dict[str, Any]]],
    *,
    max_bytes: int,
) -> list[str]:
    """``INSERT VERTEX tag(cols) VALUES "vid":(vals), …`` for ``(vid, doc)`` rows.

    Every column is written, a column the doc lacks as ``NULL``: an insert
    replaces the vertex's whole tag, so writing it again is harmless.
    """
    head = f"INSERT VERTEX `{tag_name}` ({_columns(columns)})"
    values = (
        f'"{escape_nebula_string(vid)}":{_tuple(doc, columns)}' for vid, doc in rows
    )
    return _values_statements(head, values, max_bytes)


#: ``(source VID, target VID, rank or None for 0, properties)``.
EdgeRow = tuple[str, str, int | None, dict[str, Any]]


def _edge_key(src_vid: str, dst_vid: str, rank: int | None) -> str:
    key = f'"{escape_nebula_string(src_vid)}"->"{escape_nebula_string(dst_vid)}"'
    return key if rank is None else f"{key}@{int(rank)}"


def insert_edges_ngql(
    edge_type: str,
    columns: Sequence[str],
    edges: Iterable[EdgeRow],
    *,
    max_bytes: int,
    if_not_exists: bool = False,
) -> list[str]:
    """``INSERT EDGE type(cols) VALUES "src"->"dst"[@rank]:(vals), …``.

    Like :func:`insert_vertices_ngql`, an insert replaces the edge whole;
    *if_not_exists* leaves an existing edge as it is instead.
    """
    guard = " IF NOT EXISTS" if if_not_exists else ""
    head = f"INSERT EDGE{guard} `{edge_type}` ({_columns(columns)})"
    values = (
        f"{_edge_key(src, dst, rank)}:{_tuple(props, columns)}"
        for src, dst, rank, props in edges
    )
    return _values_statements(head, values, max_bytes)


def upsert_edge_ngql(
    edge_type: str,
    src_vid: str,
    dst_vid: str,
    props: dict[str, Any],
    edge_fields: Sequence[str],
    rank: int | None = None,
) -> str:
    """``UPSERT EDGE ON type "src"->"dst"[@rank] SET prop=val, …``; ``""`` if none set."""
    set_parts = [
        f"`{k}` = {serialize_nebula_value(v)}"
        for k, v in props.items()
        if k in edge_fields
    ]
    if not set_parts:
        return ""
    return (
        f"UPSERT EDGE ON `{edge_type}` {_edge_key(src_vid, dst_vid, rank)} "
        f"SET {', '.join(set_parts)}"
    )


def delete_edges_ngql(edge_type: str, src_vid: str, dst_vids: Sequence[str]) -> str:
    """Delete every edge of *edge_type* from *src_vid* to any of *dst_vids*, at any rank."""
    targets = ", ".join(f'"{escape_nebula_string(d)}"' for d in dst_vids)
    return (
        f'GO FROM "{escape_nebula_string(src_vid)}" OVER `{edge_type}` '
        f"WHERE dst(edge) IN [{targets}] "
        "YIELD src(edge) AS s, dst(edge) AS d, rank(edge) AS r "
        f"| DELETE EDGE `{edge_type}` $-.s -> $-.d @ $-.r"
    )


def fold_rows(
    rows: Iterable[tuple[_K, dict[str, Any]]],
) -> dict[_K, dict[str, Any]]:
    """One row per key, in order of first occurrence; per field the latest value.

    A field a later row lacks keeps its earlier value, as an upsert would.
    """
    folded: dict[_K, dict[str, Any]] = {}
    for key, row in rows:
        folded.setdefault(key, {}).update(row)
    return folded


# ---------------------------------------------------------------------------
# DQL – queries (nGQL)
# ---------------------------------------------------------------------------


def fetch_docs_ngql(
    tag_name: str,
    filter_clause: str = "",
    limit: int | None = None,
    return_keys: list[str] | None = None,
) -> str:
    """``MATCH (v:Tag) WHERE ... RETURN v LIMIT n``."""
    where = f" WHERE {filter_clause}" if filter_clause else ""
    if return_keys:
        ret = ", ".join(f"v.`{tag_name}`.`{k}` AS `{k}`" for k in return_keys)
    else:
        ret = "v"
    lim = f" LIMIT {limit}" if limit else ""
    return f"MATCH (v:`{tag_name}`){where} RETURN {ret}{lim}"


# Nebula stores every edge under an out-key *and* an in-key, so the reverse and
# bidirectional forms cost the same as the forward one — they just have to be asked for.
_GO_DIRECTION_CLAUSE: dict[EdgeDirection, str] = {
    EdgeDirection.OUT: "",
    EdgeDirection.IN: " REVERSELY",
    EdgeDirection.ANY: " BIDIRECT",
}


def fetch_edges_ngql(
    from_tag: str,
    from_vid: str,
    edge_type: str | None = None,
    to_tag: str | None = None,
    to_vid: str | None = None,
    filter_clause: str = "",
    limit: int | None = None,
    direction: EdgeDirection = EdgeDirection.OUT,
    to_key: str | None = None,
) -> str:
    """Build a GO query for fetching edges incident to one vertex.

    ``$$`` is the vertex at the far end of whichever orientation ``GO`` walked,
    so the ``to_vid`` predicate keeps meaning "the other endpoint" under
    ``REVERSELY`` and ``BIDIRECT`` without special-casing. *to_key* matches the
    far end's VID without its tag, whatever tag it has.
    """
    escaped_from = escape_nebula_string(from_vid)
    over = f"`{edge_type}`" if edge_type else "*"
    where_parts: list[str] = []
    if to_vid:
        escaped_to = escape_nebula_string(to_vid)
        where_parts.append(f'id($$) == "{escaped_to}"')
    elif to_key:
        escaped_key = escape_nebula_string(f"{VID_SEPARATOR}{to_key}")
        where_parts.append(f'id($$) ENDS WITH "{escaped_key}"')
    if filter_clause:
        where_parts.append(filter_clause)
    where = " WHERE " + " AND ".join(where_parts) if where_parts else ""
    lim = f"| LIMIT {limit}" if limit else ""
    return (
        f'GO FROM "{escaped_from}" OVER {over}'
        f"{_GO_DIRECTION_CLAUSE[direction]}{where} "
        f"YIELD properties(edge) AS props, src(edge) AS src, dst(edge) AS dst, "
        f"type(edge) AS edge_type {lim}"
    )


def aggregate_ngql(
    tag_name: str,
    agg_func: str,
    discriminant: str | None = None,
    aggregated_field: str | None = None,
    filter_clause: str = "",
) -> str:
    """Build an aggregation query using MATCH."""
    where = f" WHERE {filter_clause}" if filter_clause else ""
    if agg_func == "COUNT":
        if discriminant:
            return (
                f"MATCH (v:`{tag_name}`){where} "
                f"RETURN v.`{tag_name}`.`{discriminant}` AS `key`, count(*) AS `count`"
            )
        return f"MATCH (v:`{tag_name}`){where} RETURN count(*) AS `count`"
    elif agg_func in ("MAX", "MIN", "AVG"):
        func = agg_func.lower()
        return (
            f"MATCH (v:`{tag_name}`){where} "
            f"RETURN {func}(v.`{tag_name}`.`{aggregated_field}`) AS `val`"
        )
    elif agg_func == "SORTED_UNIQUE":
        return (
            f"MATCH (v:`{tag_name}`){where} "
            f"RETURN DISTINCT v.`{tag_name}`.`{aggregated_field}` AS `val` "
            f"ORDER BY `val`"
        )
    raise ValueError(f"Unsupported aggregation: {agg_func}")


# ---------------------------------------------------------------------------
# DQL – queries (v5 ISO GQL / Cypher)
# ---------------------------------------------------------------------------


def fetch_docs_gql(
    tag_name: str,
    filter_clause: str = "",
    limit: int | None = None,
    return_keys: list[str] | None = None,
) -> str:
    where = f" WHERE {filter_clause}" if filter_clause else ""
    if return_keys:
        ret = ", ".join(f"v.`{k}` AS `{k}`" for k in return_keys)
    else:
        ret = "v"
    lim = f" LIMIT {limit}" if limit else ""
    return f"MATCH (v:`{tag_name}`){where} RETURN {ret}{lim}"


def upsert_vertex_gql(
    tag_name: str,
    vid: str,
    props: dict[str, Any],
    tag_fields: list[str],
) -> str:
    """v5: UPSERT VERTEX ON tag "vid" SET prop=val, ... (same syntax)."""
    return upsert_vertex_ngql(tag_name, vid, props, tag_fields)


def aggregate_gql(
    tag_name: str,
    agg_func: str,
    discriminant: str | None = None,
    aggregated_field: str | None = None,
    filter_clause: str = "",
) -> str:
    where = f" WHERE {filter_clause}" if filter_clause else ""
    if agg_func == "COUNT":
        if discriminant:
            return (
                f"MATCH (v:`{tag_name}`){where} "
                f"RETURN v.`{discriminant}` AS `key`, count(*) AS `count`"
            )
        return f"MATCH (v:`{tag_name}`){where} RETURN count(*) AS `count`"
    elif agg_func in ("MAX", "MIN", "AVG"):
        func = agg_func.lower()
        return (
            f"MATCH (v:`{tag_name}`){where} "
            f"RETURN {func}(v.`{aggregated_field}`) AS `val`"
        )
    elif agg_func == "SORTED_UNIQUE":
        return (
            f"MATCH (v:`{tag_name}`){where} "
            f"RETURN DISTINCT v.`{aggregated_field}` AS `val` "
            f"ORDER BY `val`"
        )
    raise ValueError(f"Unsupported aggregation: {agg_func}")
