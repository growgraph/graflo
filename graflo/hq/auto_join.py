"""Auto-JOIN generation for edge resources.

When a Resource's pipeline contains an EdgeActor whose edge has
``match_source`` / ``match_target``, and the source/target vertex types
have known TablePatterns, this module can auto-generate JoinClauses and
IS_NOT_NULL filters on the edge resource's TablePattern so that the
resulting SQL fetches fully resolved rows.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from graflo.architecture.actor import ActorWrapper, EdgeActor
from graflo.architecture.resource import Resource
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.util.onto import JoinClause, TablePattern

if TYPE_CHECKING:
    from graflo.architecture.vertex import VertexConfig
    from graflo.util.onto import Patterns

logger = logging.getLogger(__name__)

# Alias prefixes assigned to source / target joins.
_SOURCE_ALIAS = "s"
_TARGET_ALIAS = "t"


def enrich_edge_pattern_with_joins(
    resource: Resource,
    pattern: TablePattern,
    patterns: Patterns,
    vertex_config: VertexConfig,
) -> None:
    """Mutate *pattern* in-place, adding JoinClauses + IS_NOT_NULL filters.

    The function inspects the Resource's actor pipeline for EdgeActors and,
    for each edge that declares ``match_source`` **and** ``match_target``,
    looks up the source / target vertex TablePatterns and primary keys to
    construct LEFT JOINs and NOT-NULL guards.

    If the pattern already has joins, this function is a no-op (the user
    provided explicit join specs).

    Args:
        resource: The Resource whose pipeline is inspected.
        pattern: The TablePattern to enrich (mutated in-place).
        patterns: The Patterns collection holding all vertex TablePatterns.
        vertex_config: VertexConfig for looking up primary keys.
    """
    if pattern.joins:
        return

    edge_actors = _collect_edge_actors(resource.root)
    if not edge_actors:
        return

    new_joins: list[JoinClause] = []
    new_filters: list[FilterExpression] = []

    for ea in edge_actors:
        edge = ea.edge
        if not edge.match_source or not edge.match_target:
            continue

        source_info = _vertex_table_info(edge.source, patterns, vertex_config)
        target_info = _vertex_table_info(edge.target, patterns, vertex_config)
        if source_info is None or target_info is None:
            logger.debug(
                "Skipping auto-join for edge %s->%s: missing vertex pattern",
                edge.source,
                edge.target,
            )
            continue

        src_table, src_schema, src_pk = source_info
        tgt_table, tgt_schema, tgt_pk = target_info

        src_alias = _SOURCE_ALIAS
        tgt_alias = _TARGET_ALIAS

        new_joins.append(
            JoinClause(
                table=src_table,
                schema_name=src_schema,
                alias=src_alias,
                on_self=edge.match_source,
                on_other=src_pk,
                join_type="LEFT",
            )
        )
        new_joins.append(
            JoinClause(
                table=tgt_table,
                schema_name=tgt_schema,
                alias=tgt_alias,
                on_self=edge.match_target,
                on_other=tgt_pk,
                join_type="LEFT",
            )
        )

        new_filters.append(
            FilterExpression(
                kind="leaf",
                field=f"{src_alias}.{src_pk}",
                cmp_operator=ComparisonOperator.IS_NOT_NULL,
            )
        )
        new_filters.append(
            FilterExpression(
                kind="leaf",
                field=f"{tgt_alias}.{tgt_pk}",
                cmp_operator=ComparisonOperator.IS_NOT_NULL,
            )
        )

    if new_joins:
        pattern.joins = new_joins
        pattern.filters = list(pattern.filters) + new_filters


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _collect_edge_actors(wrapper: ActorWrapper) -> list[EdgeActor]:
    """Recursively collect all EdgeActors from an ActorWrapper tree."""
    result: list[EdgeActor] = []
    for actor in wrapper.collect_actors():
        if isinstance(actor, EdgeActor):
            result.append(actor)
    return result


def _vertex_table_info(
    vertex_name: str,
    patterns: Patterns,
    vertex_config: VertexConfig,
) -> tuple[str, str | None, str] | None:
    """Return (table_name, schema_name, primary_key_field) for a vertex.

    Returns None if the vertex has no TablePattern in *patterns*.
    """
    tp = patterns.table_patterns.get(vertex_name)
    if tp is None:
        return None
    try:
        pk_fields = vertex_config.index(vertex_name).fields
    except (KeyError, IndexError):
        return None
    if not pk_fields:
        return None
    return tp.table_name, tp.schema_name, pk_fields[0]
