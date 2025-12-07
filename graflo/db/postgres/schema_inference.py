"""Schema inference from PostgreSQL database introspection.

This module provides functionality to infer graflo Schema objects from PostgreSQL
3NF database schemas by analyzing table structures, relationships, and column types.
"""

import logging
from typing import Any, Optional

from graflo.architecture.edge import Edge, EdgeConfig, WeightConfig
from graflo.architecture.onto import Index, IndexType
from graflo.architecture.schema import Schema, SchemaMetadata
from graflo.architecture.vertex import Field, Vertex, VertexConfig
from graflo.onto import DBFlavor

from .types import PostgresTypeMapper

logger = logging.getLogger(__name__)


class PostgresSchemaInferencer:
    """Infers graflo Schema from PostgreSQL schema introspection results.

    This class takes the output from PostgresConnection.introspect_schema() and
    generates a complete graflo Schema with vertices, edges, and weights.
    """

    def __init__(self, db_flavor: DBFlavor = DBFlavor.ARANGO):
        """Initialize the schema inferencer.

        Args:
            db_flavor: Target database flavor for the inferred schema
        """
        self.db_flavor = db_flavor
        self.type_mapper = PostgresTypeMapper()

    def infer_vertex_config(self, introspection_result: dict[str, Any]) -> VertexConfig:
        """Infer VertexConfig from vertex tables.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()

        Returns:
            VertexConfig: Inferred vertex configuration
        """
        vertex_tables = introspection_result.get("vertex_tables", [])
        vertices = []

        for table_info in vertex_tables:
            table_name = table_info["name"]
            columns = table_info["columns"]
            pk_columns = table_info["primary_key"]

            # Create fields from columns
            fields = []
            for col in columns:
                field_name = col["name"]
                field_type = self.type_mapper.map_type(col["type"])
                fields.append(Field(name=field_name, type=field_type))

            # Create indexes from primary key
            indexes = []
            if pk_columns:
                indexes.append(
                    Index(fields=pk_columns, type=IndexType.PERSISTENT, unique=True)
                )

            # Create vertex
            vertex = Vertex(
                name=table_name,
                dbname=table_name,
                fields=fields,
                indexes=indexes,
            )

            vertices.append(vertex)
            logger.debug(
                f"Inferred vertex '{table_name}' with {len(fields)} fields and "
                f"{len(indexes)} indexes"
            )

        return VertexConfig(vertices=vertices, db_flavor=self.db_flavor)

    def infer_edge_weights(
        self, edge_table_info: dict[str, Any]
    ) -> Optional[WeightConfig]:
        """Infer edge weights from edge table columns.

        Args:
            edge_table_info: Edge table information from introspection

        Returns:
            WeightConfig if there are weight columns, None otherwise
        """
        columns = edge_table_info["columns"]
        pk_columns = set(edge_table_info["primary_key"])
        fk_columns = {fk["column"] for fk in edge_table_info["foreign_keys"]}

        # Find non-PK, non-FK columns (these become weights)
        weight_columns = [
            col
            for col in columns
            if col["name"] not in pk_columns and col["name"] not in fk_columns
        ]

        if not weight_columns:
            return None

        # Extract column names as direct weights
        direct_weights = [col["name"] for col in weight_columns]

        logger.debug(
            f"Inferred {len(direct_weights)} weights for edge table "
            f"'{edge_table_info['name']}': {direct_weights}"
        )

        return WeightConfig(direct=direct_weights)

    def infer_edge_config(
        self,
        introspection_result: dict[str, Any],
        vertex_config: VertexConfig,
    ) -> EdgeConfig:
        """Infer EdgeConfig from edge tables.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()
            vertex_config: Inferred vertex configuration

        Returns:
            EdgeConfig: Inferred edge configuration
        """
        edge_tables = introspection_result.get("edge_tables", [])
        edges = []

        vertex_names = vertex_config.vertex_set

        for edge_table_info in edge_tables:
            table_name = edge_table_info["name"]
            source_table = edge_table_info["source_table"]
            target_table = edge_table_info["target_table"]
            fk_columns = edge_table_info["foreign_keys"]
            pk_columns = edge_table_info["primary_key"]

            # Verify source and target vertices exist
            if source_table not in vertex_names:
                logger.warning(
                    f"Source vertex '{source_table}' for edge table '{table_name}' "
                    f"not found in vertex config, skipping"
                )
                continue

            if target_table not in vertex_names:
                logger.warning(
                    f"Target vertex '{target_table}' for edge table '{table_name}' "
                    f"not found in vertex config, skipping"
                )
                continue

            # Infer weights
            weights = self.infer_edge_weights(edge_table_info)

            # Create indexes from primary key and foreign keys
            indexes = []
            if pk_columns:
                indexes.append(
                    Index(fields=pk_columns, type=IndexType.PERSISTENT, unique=True)
                )

            # Add indexes for foreign keys (for efficient lookups)
            # Note: Only add index if not already covered by primary key
            pk_set = set(pk_columns)
            for fk in fk_columns:
                fk_column_name = fk["column"]
                # Skip if FK column is part of primary key (already indexed)
                if fk_column_name not in pk_set:
                    indexes.append(
                        Index(
                            fields=[fk_column_name],
                            type=IndexType.PERSISTENT,
                            unique=False,
                        )
                    )

            # Create edge
            edge = Edge(
                source=source_table,
                target=target_table,
                indexes=indexes,
                weights=weights,
                collection_name=table_name,
            )

            edges.append(edge)
            logger.debug(
                f"Inferred edge '{table_name}' from {source_table} to {target_table} "
                f"with {len(indexes)} indexes"
            )

        return EdgeConfig(edges=edges)

    def infer_schema(
        self, introspection_result: dict[str, Any], schema_name: str | None = None
    ) -> Schema:
        """Infer complete Schema from PostgreSQL introspection.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()
            schema_name: Optional schema name (defaults to schema_name from introspection)

        Returns:
            Schema: Complete inferred schema with vertices, edges, and metadata
        """
        if schema_name is None:
            schema_name = introspection_result.get("schema_name", "public")

        logger.info(f"Inferring schema from PostgreSQL schema '{schema_name}'")

        # Infer vertex configuration
        vertex_config = self.infer_vertex_config(introspection_result)
        logger.info(f"Inferred {len(vertex_config.vertices)} vertices")

        # Infer edge configuration
        edge_config = self.infer_edge_config(introspection_result, vertex_config)
        edges_count = len(list(edge_config.edges_list()))
        logger.info(f"Inferred {edges_count} edges")

        # Create schema metadata
        metadata = SchemaMetadata(name=schema_name)

        # Create schema (resources will be added separately)
        schema = Schema(
            general=metadata,
            vertex_config=vertex_config,
            edge_config=edge_config,
            resources=[],  # Resources will be created separately
        )

        logger.info(
            f"Successfully inferred schema '{schema_name}' with "
            f"{len(vertex_config.vertices)} vertices and "
            f"{len(list(edge_config.edges_list()))} edges"
        )

        return schema
