"""Relationship MERGE map fragments for parallel edges (property-graph backends)."""

from __future__ import annotations

import json
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any

from graflo.db.cypher.escape import cypher_map_key, cypher_name, cypher_string_literal
from graflo.db.cypher.union import union_assignment


def _normalized_prop_names(prop_names: Sequence[str]) -> list[str]:
    keys: list[str] = []
    for raw in prop_names:
        k = raw.strip().replace("`", "")
        if k and k not in keys:
            keys.append(k)
    return keys


def rel_merge_props_map_from_row_index(
    prop_names: Sequence[str], *, row_index: int = 2
) -> str:
    """Build `` `k`: row[n]['k'], ... `` for MERGE relationship properties.

    Matches batches shaped as ``row`` = ``[source_doc, target_doc, props]`` (Neo4j,
    FalkorDB-style ``row[2]``).
    """
    row_access = f"row[{row_index}]"
    parts: list[str] = []
    for key in _normalized_prop_names(prop_names):
        bk = cypher_map_key(key)
        lit = cypher_string_literal(key)
        parts.append(f"{bk}: {row_access}[{lit}]")
    return ", ".join(parts)


def rel_merge_props_map_from_row_props(
    prop_names: Sequence[str], *, props_expr: str = "row.props"
) -> str:
    """Build `` `k`: row.props['k'], ... `` (Memgraph-style batch rows)."""
    parts: list[str] = []
    for key in _normalized_prop_names(prop_names):
        bk = cypher_map_key(key)
        lit = cypher_string_literal(key)
        parts.append(f"{bk}: {props_expr}[{lit}]")
    return ", ".join(parts)


def _key_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def partition_by_absent_merge_props(
    rows: Sequence[Any],
    prop_names: Sequence[str],
    *,
    props_of: Callable[[Any], Mapping[str, Any] | None],
    ends_of: Callable[[Any], tuple[Any, Any]],
    union: Collection[str] = (),
) -> list[tuple[tuple[str, ...], list[Any]]]:
    """Group edge rows by the identity properties each one lacks.

    *prop_names* are the properties the edge's identity names beside its
    endpoints. Cypher refuses ``MERGE`` on a null property, so a row missing
    one (absent or ``None``) cannot use the property ``MERGE``. As on
    PostgreSQL (``NULLS NOT DISTINCT``), absence is part of the key: such a row
    matches an existing edge lacking the same properties. Rows lacking some are
    deduplicated on their endpoints and present merge values, later properties
    winning, because :func:`rel_absent_upsert_clause` cannot see edges created
    earlier in its own batch.

    With *union* fields the complete group is deduplicated as well: one row per
    edge carries the union of its rows' members, so no ``MERGE`` has to see a row
    written earlier in its own batch.

    Returns:
        ``(absent names, rows)`` pairs; the group lacking nothing comes first.
    """
    keys = _normalized_prop_names(prop_names)
    groups: dict[tuple[str, ...], list[Any]] = {}
    position: dict[tuple[tuple[str, ...], str], int] = {}
    for row in rows:
        props = props_of(row) or {}
        absent = tuple(k for k in keys if props.get(k) is None)
        group = groups.setdefault(absent, [])
        if not absent and not union:
            group.append(row)
            continue
        source, target = ends_of(row)
        ident = _key_text(
            [source, target, [props.get(k) for k in keys if k not in absent]]
        )
        at = position.get((absent, ident))
        if at is None:
            position[(absent, ident)] = len(group)
            group.append(row)
        else:
            group[at] = _merged_row(group[at], row, props_of, union)
    return sorted(groups.items(), key=lambda item: len(item[0]))


def _union_members(earlier: Any, later: Any) -> list[Any]:
    """Members of both lists in first-seen order, nulls dropped, as Cypher unions."""
    out: list[Any] = []
    for side in (earlier, later):
        for member in side if isinstance(side, list) else []:
            if member is not None and member not in out:
                out.append(member)
    return out


def _merged_row(
    earlier: Any,
    later: Any,
    props_of: Callable[[Any], Any],
    union: Collection[str] = (),
) -> Any:
    before = props_of(earlier) or {}
    after = props_of(later) or {}
    merged = {**before, **after}
    for field in union:
        if field in before or field in after:
            merged[field] = _union_members(before.get(field), after.get(field))
    if isinstance(later, dict):
        return {**later, "props": merged}
    return [*later[:2], merged, *later[3:]]


def rel_union_rows(rows: Sequence[Any], union: Sequence[str]) -> list[list[Any]]:
    """``[source, target, props]`` rows as ``[source, target, replaced, unioned]``.

    The union fields move out of ``row[2]`` into ``row[3]``, so ``r += row[2]``
    can never overwrite a stored list (:func:`rel_upsert_clause`).
    """
    out: list[list[Any]] = []
    for row in rows:
        props = (row[2] if len(row) > 2 else None) or {}
        out.append(
            [
                row[0],
                row[1],
                {k: v for k, v in props.items() if k not in union},
                {k: props[k] for k in union if k in props},
            ]
        )
    return out


def _union_set(var: str, union: Sequence[str], union_expr: str) -> str:
    """`` SET var.f = <union>, ...`` for *union* fields, or nothing."""
    if not union:
        return ""
    assignments = ", ".join(
        union_assignment(var, field, f"{union_expr}[{cypher_string_literal(field)}]")
        for field in union
    )
    return f" SET {assignments}"


def rel_upsert_clause(
    relation_name: str,
    prop_names: Sequence[str],
    absent: Sequence[str],
    *,
    union: Sequence[str] = (),
    props_expr: str = "row[2]",
    union_expr: str = "row[3]",
) -> str:
    """Upsert one group of :func:`partition_by_absent_merge_props` rows.

    ``MERGE`` on the endpoints (and *prop_names* when the group has them all),
    or :func:`rel_absent_upsert_clause` when it lacks some. Follows a ``MATCH``
    binding ``source``, ``target`` and ``row``. *union* fields are read from
    *union_expr* (:func:`rel_union_rows`) and written as the union of the
    stored list and the row's.
    """
    if absent:
        return rel_absent_upsert_clause(
            relation_name,
            prop_names,
            absent,
            props_expr=props_expr,
            union=union,
            union_expr=union_expr,
        )
    rel = cypher_name(relation_name)
    if prop_names:
        map_clause = rel_merge_props_map_from_row_index(prop_names)
        pattern = f"MERGE (source)-[r:{rel} {{{map_clause}}}]->(target)"
    else:
        pattern = f"MERGE (source)-[r:{rel}]->(target)"
    return f"{pattern}\nSET r += {props_expr}" + _union_set("r", union, union_expr)


def rel_absent_upsert_clause(
    relation_name: str,
    prop_names: Sequence[str],
    absent: Sequence[str],
    *,
    props_expr: str,
    source: str = "source",
    target: str = "target",
    union: Sequence[str] = (),
    union_expr: str = "row[3]",
) -> str:
    """Upsert a relationship whose identity properties *absent* are missing.

    Matches an existing ``relation_name`` edge between *source* and *target*
    whose present identity properties equal the row's and whose *absent* ones are
    unset, and updates it; creates one when none matches. Follows a ``MATCH``
    binding *source*, *target* and ``row``. *union* fields are read from
    *union_expr* and unioned with what the edge holds.
    """
    rel = cypher_name(relation_name)
    conditions = []
    for key in _normalized_prop_names(prop_names):
        bk = cypher_map_key(key)
        if key in absent:
            conditions.append(f"existing.{bk} IS NULL")
        else:
            conditions.append(
                f"existing.{bk} = {props_expr}[{cypher_string_literal(key)}]"
            )
    return (
        f"OPTIONAL MATCH ({source})-[existing:{rel}]->({target})\n"
        f"WHERE {' AND '.join(conditions)}\n"
        "FOREACH (_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |\n"
        f"    CREATE ({source})-[r:{rel}]->({target}) SET r += {props_expr}"
        f"{_union_set('r', union, union_expr)})\n"
        "FOREACH (_ IN CASE WHEN existing IS NULL THEN [] ELSE [1] END |\n"
        f"    SET existing += {props_expr}"
        f"{_union_set('existing', union, union_expr)})"
    )
