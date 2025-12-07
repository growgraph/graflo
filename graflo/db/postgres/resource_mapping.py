"""Resource mapping from PostgreSQL tables to graflo Resources.

This module provides functionality to map PostgreSQL tables (both vertex and edge tables)
to graflo Resource objects that can be used for data ingestion.
"""

import logging
from typing import Any

from graflo.architecture.edge import EdgeConfig
from graflo.architecture.resource import Resource
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


class PostgresResourceMapper:
    """Maps PostgreSQL tables to graflo Resources.

    This class creates Resource objects that map PostgreSQL tables to graph vertices
    and edges, enabling ingestion of relational data into graph databases.
    """

    def create_vertex_resource(self, table_name: str, vertex_name: str) -> Resource:
        """Create a Resource for a vertex table.

        Args:
            table_name: Name of the PostgreSQL table
            vertex_name: Name of the vertex type (typically same as table_name)

        Returns:
            Resource: Resource configured to ingest vertex data
        """
        # Create apply list with VertexActor
        # The actor wrapper will interpret {"vertex": vertex_name} as VertexActor
        apply = [{"vertex": vertex_name}]

        resource = Resource(
            resource_name=table_name,
            apply=apply,
        )

        logger.debug(
            f"Created vertex resource '{table_name}' for vertex '{vertex_name}'"
        )

        return resource

    def create_edge_resource(
        self,
        edge_table_info: dict[str, Any],
        vertex_config: VertexConfig,
    ) -> Resource:
        """Create a Resource for an edge table.

        Args:
            edge_table_info: Edge table information from introspection
            vertex_config: Vertex configuration for source/target validation

        Returns:
            Resource: Resource configured to ingest edge data
        """
        table_name = edge_table_info["name"]
        source_table = edge_table_info["source_table"]
        target_table = edge_table_info["target_table"]

        # Verify source and target vertices exist
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

        # Create apply list with EdgeActor
        # The actor wrapper will interpret {"source": source, "target": target} as EdgeActor
        apply = [{"source": source_table, "target": target_table}]

        resource = Resource(
            resource_name=table_name,
            apply=apply,
        )

        logger.debug(
            f"Created edge resource '{table_name}' from {source_table} to {target_table}"
        )

        return resource

    def map_tables_to_resources(
        self,
        introspection_result: dict[str, Any],
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
    ) -> list[Resource]:
        """Map all PostgreSQL tables to Resources.

        Creates Resources for both vertex and edge tables, enabling ingestion
        of the entire database schema.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()
            vertex_config: Inferred vertex configuration
            edge_config: Inferred edge configuration

        Returns:
            list[Resource]: List of Resources for all tables
        """
        resources = []

        # Map vertex tables to resources
        vertex_tables = introspection_result.get("vertex_tables", [])
        for table_info in vertex_tables:
            table_name = table_info["name"]
            vertex_name = table_name  # Use table name as vertex name
            resource = self.create_vertex_resource(table_name, vertex_name)
            resources.append(resource)

        # Map edge tables to resources
        edge_tables = introspection_result.get("edge_tables", [])
        for edge_table_info in edge_tables:
            try:
                resource = self.create_edge_resource(edge_table_info, vertex_config)
                resources.append(resource)
            except ValueError as e:
                logger.warning(f"Skipping edge resource creation: {e}")
                continue

        logger.info(
            f"Mapped {len(vertex_tables)} vertex tables and {len(edge_tables)} edge tables "
            f"to {len(resources)} resources"
        )

        return resources
