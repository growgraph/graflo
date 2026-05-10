"""Resource mapping from PostgreSQL tables to graflo Resources.

This module provides functionality to map PostgreSQL tables (both vertex and edge tables)
to graflo Resource objects that can be used for data ingestion.

Resources here use the original PostgreSQL column names. Reserved-word /
target-DB-specific renames are applied a posteriori via
:class:`graflo.hq.sanitizer.Sanitizer`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from graflo.architecture.contract.declarations.resource import Resource
from graflo.architecture.schema.vertex import VertexConfig
from .conn import EdgeTableInfo, SchemaIntrospectionResult
from .inference_utils import (
    detect_separator,
    split_by_separator,
)
from ...architecture import EdgeConfig

if TYPE_CHECKING:
    from graflo.hq.fuzzy_matcher import FuzzyMatcher

logger = logging.getLogger(__name__)


class PostgresResourceMapper:
    """Maps PostgreSQL tables to graflo Resources.

    This class creates Resource objects that map PostgreSQL tables to graph vertices
    and edges, enabling ingestion of relational data into graph databases.
    """

    def __init__(self, fuzzy_threshold: float = 0.8):
        """Initialize the resource mapper.

        Args:
            fuzzy_threshold: Similarity threshold for fuzzy matching (0.0 to 1.0, default 0.8)
        """
        self.fuzzy_threshold = fuzzy_threshold

    def create_vertex_resource(
        self,
        table_name: str,
        vertex_name: str,
    ) -> Resource:
        """Create a Resource for a vertex table.

        The resulting pipeline is a single ``{"vertex": vertex_name}`` step.
        Field renames driven by reserved-word sanitization are added later by
        :class:`graflo.hq.sanitizer.Sanitizer`.

        Args:
            table_name: Name of the PostgreSQL table
            vertex_name: Name of the vertex type (typically same as table_name)

        Returns:
            Resource: Resource configured to ingest vertex data
        """
        apply: list[dict[str, Any]] = [{"vertex": vertex_name}]
        resource = Resource(
            name=table_name,
            pipeline=apply,
        )

        logger.debug(
            f"Created vertex resource '{table_name}' for vertex '{vertex_name}'"
        )

        return resource

    def create_edge_resource(
        self,
        edge_table_info: EdgeTableInfo,
        vertex_config: VertexConfig,
        matcher: FuzzyMatcher,
    ) -> Resource:
        """Create a Resource for an edge table.

        Field renames driven by reserved-word sanitization are added later by
        :class:`graflo.hq.sanitizer.Sanitizer`.

        Args:
            edge_table_info: Edge table information from introspection
            vertex_config: Vertex configuration for source/target validation
            matcher: Optional fuzzy matcher for better performance (with caching enabled)

        Returns:
            Resource: Resource configured to ingest edge data
        """
        table_name = edge_table_info.name
        source_table = edge_table_info.source_table
        target_table = edge_table_info.target_table
        source_column = edge_table_info.source_column
        target_column = edge_table_info.target_column
        relation = edge_table_info.relation

        if source_table not in vertex_config.vertex_set:
            raise ValueError(
                f"Source vertex '{source_table}' for edge table '{table_name}' "
                f"not found in vertex config"
            )

        if target_table not in vertex_config.vertex_set:
            raise ValueError(
                f"Target vertex '{target_table}' for edge table '{table_name}' "
                f"not found in vertex config"
            )

        source_vertex_obj = vertex_config[source_table]
        target_vertex_obj = vertex_config[target_table]

        source_pk_fields = list(source_vertex_obj.identity)
        target_pk_fields = list(target_vertex_obj.identity)

        source_pk_field = self._infer_pk_field_from_column(
            source_column,
            source_table,
            source_pk_fields,
            matcher,
        )
        target_pk_field = self._infer_pk_field_from_column(
            target_column,
            target_table,
            target_pk_fields,
            matcher,
        )

        apply: list[dict[str, Any]] = []

        if source_column:
            apply.append(
                {
                    "vertex": source_table,
                    "from": {source_pk_field: source_column},
                }
            )

        if target_column:
            apply.append(
                {
                    "vertex": target_table,
                    "from": {target_pk_field: target_column},
                }
            )

        resource = Resource(
            name=table_name,
            pipeline=apply,
        )

        relation_info = f" with relation '{relation}'" if relation else ""
        logger.debug(
            f"Created edge resource '{table_name}' from {source_table} to {target_table}"
            f"{relation_info} "
            f"(source_col: {source_column} -> {source_pk_field}, "
            f"target_col: {target_column} -> {target_pk_field})"
        )

        return resource

    def _infer_pk_field_from_column(
        self,
        column_name: str,
        vertex_name: str,
        pk_fields: list[str],
        matcher: FuzzyMatcher,
    ) -> str:
        """Infer primary key field name from column name using heuristics.

        Uses fuzzy matching to identify vertex name fragments in column names,
        then matches to the appropriate PK field. Handles cases like:
        - "user_id" -> "user" vertex -> use first PK field (e.g., "id")
        - "bla_user" -> "user" vertex -> use first PK field
        - "user_id_2" -> "user" vertex -> use first PK field
        - "source_user_id" -> "user" vertex -> use first PK field
        - "bla_user" and "bla_user_2" -> both map to "user" vertex PK field

        The heuristic works by:
        1. Splitting the column name into fragments
        2. Fuzzy matching fragments to vertex names
        3. If a fragment matches the target vertex_name, use the vertex's PK field
        4. Otherwise, fall back to first PK field or "id"

        Args:
            column_name: Name of the column (e.g., "user_id", "bla_user", "bla_user_2")
            vertex_name: Name of the target vertex (already known from edge table info)
            pk_fields: List of primary key field names for the vertex
            matcher: Optional fuzzy matcher for better performance (with caching enabled)

        Returns:
            Primary key field name (defaults to first PK field or "id" if no match)
        """
        # Split column name into fragments
        separator = detect_separator(column_name)
        fragments = split_by_separator(column_name, separator)

        # Try to find a fragment that matches the target vertex name
        # This confirms that the column is indeed related to this vertex
        for fragment in fragments:
            # Fuzzy match fragment to vertex names
            matched_vertex = matcher.get_match(fragment)

            # If we found a match to our target vertex, use its PK field
            if matched_vertex == vertex_name:
                if pk_fields:
                    # Use the first PK field (most common case is single-column PK)
                    return pk_fields[0]
                else:
                    # No PK fields available, use "id" as default
                    return "id"

        # No fragment matched the target vertex, but we still have vertex_name
        # This might happen if the column name doesn't contain the vertex name fragment
        # In this case, trust that vertex_name is correct and use its PK field
        if pk_fields:
            return pk_fields[0]

        # Last resort: use "id" as default
        # This is better than failing, but ideally pk_fields should always be available
        logger.debug(
            f"No PK fields found for vertex '{vertex_name}', using 'id' as default "
            f"for column '{column_name}'"
        )
        return "id"

    def create_resources_from_tables(
        self,
        introspection_result: SchemaIntrospectionResult,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        fuzzy_threshold: float | None = None,
    ) -> list[Resource]:
        """Create Resources from PostgreSQL tables.

        Creates Resources for both vertex and edge tables, enabling ingestion
        of the entire database schema.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()
            vertex_config: Inferred vertex configuration
            edge_config: Inferred edge configuration
            fuzzy_threshold: Similarity threshold for fuzzy matching (0.0 to 1.0)

        Returns:
            list[Resource]: List of Resources for all tables
        """
        resources = []

        vertex_names = list(vertex_config.vertex_set)
        threshold = (
            fuzzy_threshold if fuzzy_threshold is not None else self.fuzzy_threshold
        )
        from graflo.hq.fuzzy_matcher import FuzzyMatcher

        matcher = FuzzyMatcher(vertex_names, threshold, enable_cache=True)

        vertex_tables = introspection_result.vertex_tables
        for table_info in vertex_tables:
            table_name = table_info.name
            vertex_name = table_name
            resource = self.create_vertex_resource(table_name, vertex_name)
            resources.append(resource)

        edge_tables = introspection_result.edge_tables
        for edge_table_info in edge_tables:
            try:
                resource = self.create_edge_resource(
                    edge_table_info, vertex_config, matcher
                )
                resources.append(resource)
            except ValueError as e:
                logger.warning(f"Skipping edge resource creation: {e}")
                continue

        logger.info(
            f"Mapped {len(vertex_tables)} vertex tables and {len(edge_tables)} edge tables "
            f"to {len(resources)} resources"
        )

        return resources
