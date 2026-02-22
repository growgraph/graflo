"""Query builders for NebulaGraph nGQL (v3.x) and ISO GQL (v5.x).

Each public function returns a query *string* ready to be passed to the
adapter's ``execute`` method.
"""

from __future__ import annotations

from typing import Any

from graflo.architecture.vertex import Field
from graflo.db.nebula.util import (
    escape_nebula_string,
    make_vid,
    nebula_type,
    serialize_nebula_value,
)

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------


def create_space_ngql(
    space_name: str,
    vid_type: str = "FIXED_STRING(256)",
    partition_num: int = 1,
    replica_factor: int = 1,
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
        cols = ", ".join(f"`{f.name}` {nebula_type(f.type)}" for f in fields)
        return f"CREATE TAG IF NOT EXISTS `{tag_name}` ({cols})"
    return f"CREATE TAG IF NOT EXISTS `{tag_name}` ()"


def create_edge_type_ngql(edge_type: str, fields: list[Field] | None = None) -> str:
    """``CREATE EDGE IF NOT EXISTS EdgeType(prop type, ...)``."""
    if fields:
        cols = ", ".join(f"`{f.name}` {nebula_type(f.type)}" for f in fields)
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
        vid = make_vid(doc, match_keys)
        stmt = upsert_vertex_ngql(tag_name, vid, doc, tag_fields)
        if stmt:
            statements.append(stmt)
    return statements


def insert_vertices_ngql(
    tag_name: str,
    docs: list[dict[str, Any]],
    match_keys: list[str] | tuple[str, ...],
    tag_fields: list[str],
) -> str:
    """Batch ``INSERT VERTEX IF NOT EXISTS tag(cols) VALUES "vid":(vals), ...``."""
    if not docs or not tag_fields:
        return ""
    ordered_fields = [f for f in tag_fields]
    cols = ", ".join(f"`{f}`" for f in ordered_fields)
    value_parts: list[str] = []
    for doc in docs:
        vid = make_vid(doc, match_keys)
        escaped_vid = escape_nebula_string(vid)
        vals = ", ".join(serialize_nebula_value(doc.get(f)) for f in ordered_fields)
        value_parts.append(f'"{escaped_vid}":({vals})')
    values_str = ", ".join(value_parts)
    return f"INSERT VERTEX IF NOT EXISTS `{tag_name}` ({cols}) VALUES {values_str}"


# ---------------------------------------------------------------------------
# DML – edge operations (nGQL)
# ---------------------------------------------------------------------------


def insert_edges_ngql(
    edge_type: str,
    edges: list[tuple[str, str, dict[str, Any]]],
    edge_fields: list[str] | None = None,
) -> str:
    """Batch ``INSERT EDGE IF NOT EXISTS type(cols) VALUES "src"->"dst":(vals), ...``."""
    if not edges:
        return ""

    if edge_fields:
        cols = ", ".join(f"`{f}`" for f in edge_fields)
        value_parts: list[str] = []
        for src_vid, dst_vid, props in edges:
            src = escape_nebula_string(src_vid)
            dst = escape_nebula_string(dst_vid)
            vals = ", ".join(serialize_nebula_value(props.get(f)) for f in edge_fields)
            value_parts.append(f'"{src}"->"{dst}":({vals})')
        values_str = ", ".join(value_parts)
        return f"INSERT EDGE IF NOT EXISTS `{edge_type}` ({cols}) VALUES {values_str}"
    else:
        value_parts_simple: list[str] = []
        for src_vid, dst_vid, _ in edges:
            src = escape_nebula_string(src_vid)
            dst = escape_nebula_string(dst_vid)
            value_parts_simple.append(f'"{src}"->"{dst}":()')
        values_str = ", ".join(value_parts_simple)
        return f"INSERT EDGE IF NOT EXISTS `{edge_type}` () VALUES {values_str}"


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


def fetch_edges_ngql(
    from_tag: str,
    from_vid: str,
    edge_type: str | None = None,
    to_tag: str | None = None,
    to_vid: str | None = None,
    filter_clause: str = "",
    limit: int | None = None,
) -> str:
    """Build a GO / MATCH query for fetching edges."""
    escaped_from = escape_nebula_string(from_vid)
    over = f"`{edge_type}`" if edge_type else "*"
    where_parts: list[str] = []
    if to_vid:
        escaped_to = escape_nebula_string(to_vid)
        where_parts.append(f'id($$) == "{escaped_to}"')
    if filter_clause:
        where_parts.append(filter_clause)
    where = " WHERE " + " AND ".join(where_parts) if where_parts else ""
    lim = f"| LIMIT {limit}" if limit else ""
    return (
        f'GO FROM "{escaped_from}" OVER {over}{where} '
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
