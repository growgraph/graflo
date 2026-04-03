"""Auto-JOIN generation for edge resources.

When a Resource's pipeline contains an EdgeActor whose ``derivation`` declares
``match_source`` / ``match_target``, and the source/target vertex types
have known table connectors, this module can auto-generate JoinClauses and
IS_NOT_NULL filters on the edge resource's table connector so that the
resulting SQL fetches fully resolved rows.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from graflo.architecture.pipeline.runtime.actor import ActorWrapper, EdgeActor
from graflo.architecture.contract.declarations.resource import Resource
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.architecture.contract.bindings import JoinClause, TableConnector

if TYPE_CHECKING:
    from graflo.architecture.schema.vertex import VertexConfig
    from graflo.architecture.contract.bindings import Bindings

logger = logging.getLogger(__name__)

# Alias prefixes assigned to source / target joins.
_SOURCE_ALIAS = "s"
_TARGET_ALIAS = "t"


def enrich_edge_connector_with_joins(
    resource: Resource,
    connector: TableConnector,
    bindings: Bindings,
    vertex_config: VertexConfig,
) -> None:
    """Mutate *connector* in-place, adding JoinClauses + IS_NOT_NULL filters.

    The function inspects the Resource's actor pipeline for EdgeActors and,
    for each edge that declares ``match_source`` **and** ``match_target``,
    looks up the source / target vertex table connectors and primary keys to
    construct LEFT JOINs and NOT-NULL guards.

    If the connector already has joins, this function is a no-op (the user
    provided explicit join specs).

    Args:
        resource: The Resource whose pipeline is inspected.
        connector: The table connector to enrich (mutated in-place).
        bindings: The Bindings collection holding all vertex table connectors.
        vertex_config: VertexConfig for looking up primary keys.
    """
    if connector.joins:
        return

    edge_actors = _collect_edge_actors(resource.root)
    if not edge_actors:
        return

    new_joins: list[JoinClause] = []
    new_filters: list[FilterExpression] = []

    for ea in edge_actors:
        edge = ea.edge
        der = ea.derivation
        if not der.match_source or not der.match_target:
            continue

        source_info = _vertex_table_info(edge.source, bindings, vertex_config)
        target_info = _vertex_table_info(edge.target, bindings, vertex_config)
        if source_info is None or target_info is None:
            logger.debug(
                "Skipping auto-join for edge %s->%s: missing vertex connector",
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
                on_self=der.match_source,
                on_other=src_pk,
                join_type="LEFT",
            )
        )
        new_joins.append(
            JoinClause(
                table=tgt_table,
                schema_name=tgt_schema,
                alias=tgt_alias,
                on_self=der.match_target,
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
        connector.joins = new_joins
        connector.filters = list(connector.filters) + new_filters


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
    bindings: Bindings,
    vertex_config: VertexConfig,
) -> tuple[str, str | None, str] | None:
    """Return (table_name, schema_name, primary_key_field) for a vertex.

    Returns None if the vertex has no table connector in *bindings*.
    Raises ValueError if more than one :class:`TableConnector` is bound to
    the same *vertex_name* (auto-join requires a unique SQL source).
    """
    table_connectors = [
        c
        for c in bindings.get_connectors_for_resource(vertex_name)
        if isinstance(c, TableConnector)
    ]
    if not table_connectors:
        return None
    if len(table_connectors) > 1:
        refs = ", ".join(c.name or c.hash for c in table_connectors)
        raise ValueError(
            f"Multiple TableConnectors bound to resource/vertex key '{vertex_name}' "
            f"({refs}); disambiguate before using auto-join."
        )
    connector = table_connectors[0]
    try:
        pk_fields = vertex_config.identity_fields(vertex_name)
    except (KeyError, IndexError):
        return None
    if not pk_fields:
        return None
    return connector.table_name, connector.schema_name, pk_fields[0]
