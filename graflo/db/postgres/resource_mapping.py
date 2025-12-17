"""Resource mapping from PostgreSQL tables to graflo Resources.

This module provides functionality to map PostgreSQL tables (both vertex and edge tables)
to graflo Resource objects that can be used for data ingestion.
"""

import logging

from graflo.architecture.edge import EdgeConfig
from graflo.architecture.resource import Resource
from graflo.architecture.vertex import VertexConfig

from .conn import EdgeTableInfo, SchemaIntrospectionResult

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
        edge_table_info: EdgeTableInfo,
        vertex_config: VertexConfig,
    ) -> Resource:
        """Create a Resource for an edge table.

        Args:
            edge_table_info: Edge table information from introspection
            vertex_config: Vertex configuration for source/target validation

        Returns:
            Resource: Resource configured to ingest edge data
        """
        table_name = edge_table_info.name
        source_table = edge_table_info.source_table
        target_table = edge_table_info.target_table
        source_column = edge_table_info.source_column
        target_column = edge_table_info.target_column

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

        # Get primary key fields for source and target vertices
        source_vertex_obj = vertex_config._vertices_map[source_table]
        target_vertex_obj = vertex_config._vertices_map[target_table]

        # Get the primary key field(s) from the first index (primary key)
        source_pk_fields = (
            source_vertex_obj.indexes[0].fields if source_vertex_obj.indexes else []
        )
        target_pk_fields = (
            target_vertex_obj.indexes[0].fields if target_vertex_obj.indexes else []
        )

        # For simplicity, use the first PK field (most common case is single-column PK)
        # If composite keys are needed, this would need to be extended
        source_pk_field = source_pk_fields[0] if source_pk_fields else "id"
        target_pk_field = target_pk_fields[0] if target_pk_fields else "id"

        # Create apply list using source_vertex and target_vertex pattern
        # This pattern explicitly specifies which vertex type each mapping targets,
        # avoiding attribute collisions between different vertex types
        apply = []

        # First mapping: map source foreign key column to source vertex's primary key field
        if source_column:
            source_map_config = {
                "target_vertex": source_table,
                "map": {source_column: source_pk_field},
            }
            apply.append(source_map_config)

        # Second mapping: map target foreign key column to target vertex's primary key field
        if target_column:
            target_map_config = {
                "target_vertex": target_table,
                "map": {target_column: target_pk_field},
            }
            apply.append(target_map_config)

        resource = Resource(
            resource_name=table_name,
            apply=apply,
        )

        logger.debug(
            f"Created edge resource '{table_name}' from {source_table} to {target_table} "
            f"(source_col: {source_column} -> {source_pk_field}, "
            f"target_col: {target_column} -> {target_pk_field})"
        )

        return resource

    def map_tables_to_resources(
        self,
        introspection_result: SchemaIntrospectionResult,
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
        vertex_tables = introspection_result.vertex_tables
        for table_info in vertex_tables:
            table_name = table_info.name
            vertex_name = table_name  # Use table name as vertex name
            resource = self.create_vertex_resource(table_name, vertex_name)
            resources.append(resource)

        # Map edge tables to resources
        edge_tables = introspection_result.edge_tables
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
