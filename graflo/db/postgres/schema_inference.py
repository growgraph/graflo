"""Schema inference from PostgreSQL database introspection.

This module provides functionality to infer graflo Schema objects from PostgreSQL
3NF database schemas by analyzing table structures, relationships, and column types.
"""

from __future__ import annotations

import logging

from graflo.architecture.edge import Edge, EdgeConfig, WeightConfig
from graflo.architecture.onto import Index, IndexType
from graflo.architecture.schema import Schema, SchemaMetadata
from graflo.architecture.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.onto import DBFlavor

from ...architecture.onto_sql import EdgeTableInfo, SchemaIntrospectionResult
from .conn import PostgresConnection
from .types import PostgresTypeMapper

logger = logging.getLogger(__name__)


class PostgresSchemaInferencer:
    """Infers graflo Schema from PostgreSQL schema introspection results.

    This class takes the output from PostgresConnection.introspect_schema() and
    generates a complete graflo Schema with vertices, edges, and weights.
    """

    def __init__(
        self,
        db_flavor: DBFlavor = DBFlavor.ARANGO,
        conn: PostgresConnection | None = None,
    ):
        """Initialize the schema inferencer.

        Args:
            db_flavor: Target database flavor for the inferred schema
            conn: PostgreSQL connection for sampling data to infer types (optional)
        """
        self.db_flavor = db_flavor
        self.type_mapper = PostgresTypeMapper()
        self.conn = conn

    def infer_vertex_config(
        self, introspection_result: SchemaIntrospectionResult
    ) -> VertexConfig:
        """Infer VertexConfig from vertex tables.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()

        Returns:
            VertexConfig: Inferred vertex configuration
        """
        vertex_tables = introspection_result.vertex_tables
        vertices = []

        for table_info in vertex_tables:
            table_name = table_info.name
            columns = table_info.columns
            pk_columns = table_info.primary_key

            # Create fields from columns
            fields = []
            for col in columns:
                field_name = col.name
                field_type = self.type_mapper.map_type(col.type)
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

    def _infer_type_from_samples(
        self, table_name: str, schema_name: str, column_name: str, pg_type: str
    ) -> str:
        """Infer field type by sampling 5 rows from the table.

        Uses heuristics to determine if a column contains integers, floats, datetimes, etc.
        Falls back to PostgreSQL type mapping if sampling fails or is unavailable.

        Args:
            table_name: Name of the table
            schema_name: Schema name
            column_name: Name of the column to sample
            pg_type: PostgreSQL type from schema introspection

        Returns:
            str: FieldType value (INT, FLOAT, DATETIME, STRING, etc.)
        """
        # First try PostgreSQL type mapping
        mapped_type = self.type_mapper.map_type(pg_type)

        # If we have a connection, sample data to refine the type
        if self.conn is None:
            logger.debug(
                f"No connection available for sampling, using mapped type '{mapped_type}' "
                f"for column '{column_name}' in table '{table_name}'"
            )
            return mapped_type

        try:
            # Sample 5 rows from the table
            query = (
                f'SELECT "{column_name}" FROM "{schema_name}"."{table_name}" LIMIT 5'
            )
            samples = self.conn.read(query)

            if not samples:
                logger.debug(
                    f"No samples found for column '{column_name}' in table '{table_name}', "
                    f"using mapped type '{mapped_type}'"
                )
                return mapped_type

            # Extract non-None values
            values = [
                row[column_name] for row in samples if row[column_name] is not None
            ]

            if not values:
                logger.debug(
                    f"All samples are NULL for column '{column_name}' in table '{table_name}', "
                    f"using mapped type '{mapped_type}'"
                )
                return mapped_type

            # Heuristics to infer type from values
            # Check for integers (all values are integers)
            if all(isinstance(v, int) for v in values):
                logger.debug(
                    f"Inferred INT type for column '{column_name}' in table '{table_name}' "
                    f"from samples"
                )
                return FieldType.INT.value

            # Check for floats (all values are floats or ints that could be floats)
            if all(isinstance(v, (int, float)) for v in values):
                # If any value has decimal part, it's a float
                if any(isinstance(v, float) and v != float(int(v)) for v in values):
                    logger.debug(
                        f"Inferred FLOAT type for column '{column_name}' in table '{table_name}' "
                        f"from samples"
                    )
                    return FieldType.FLOAT.value
                # All integers, but might be stored as float - check PostgreSQL type
                if mapped_type == FieldType.FLOAT.value:
                    return FieldType.FLOAT.value
                return FieldType.INT.value

            # Check for datetime/date objects
            from datetime import date, datetime, time

            if all(isinstance(v, (datetime, date, time)) for v in values):
                logger.debug(
                    f"Inferred DATETIME type for column '{column_name}' in table '{table_name}' "
                    f"from samples"
                )
                return FieldType.DATETIME.value

            # Check for ISO format datetime strings
            if all(isinstance(v, str) for v in values):
                # Try to parse as ISO datetime
                iso_datetime_count = 0
                for v in values:
                    try:
                        # Try ISO format (with or without timezone)
                        datetime.fromisoformat(v.replace("Z", "+00:00"))
                        iso_datetime_count += 1
                    except (ValueError, AttributeError):
                        # Try other common formats
                        try:
                            datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                            iso_datetime_count += 1
                        except ValueError:
                            try:
                                datetime.strptime(v, "%Y-%m-%d")
                                iso_datetime_count += 1
                            except ValueError:
                                pass

                # If most values look like datetimes, infer DATETIME
                if iso_datetime_count >= len(values) * 0.8:  # 80% threshold
                    logger.debug(
                        f"Inferred DATETIME type for column '{column_name}' in table '{table_name}' "
                        f"from ISO format strings"
                    )
                    return FieldType.DATETIME.value

            # Default to mapped type
            logger.debug(
                f"Using mapped type '{mapped_type}' for column '{column_name}' in table '{table_name}' "
                f"(could not infer from samples)"
            )
            return mapped_type

        except Exception as e:
            logger.warning(
                f"Error sampling data for column '{column_name}' in table '{table_name}': {e}. "
                f"Using mapped type '{mapped_type}'"
            )
            return mapped_type

    def infer_edge_weights(self, edge_table_info: EdgeTableInfo) -> WeightConfig | None:
        """Infer edge weights from edge table columns with types.

        Uses PostgreSQL column types and optionally samples data to infer accurate types.

        Args:
            edge_table_info: Edge table information from introspection

        Returns:
            WeightConfig if there are weight columns, None otherwise
        """
        columns = edge_table_info.columns
        pk_columns = set(edge_table_info.primary_key)
        fk_columns = {fk.column for fk in edge_table_info.foreign_keys}

        # Find non-PK, non-FK columns (these become weights)
        weight_columns = [
            col
            for col in columns
            if col.name not in pk_columns and col.name not in fk_columns
        ]

        if not weight_columns:
            return None

        # Create Field objects with types for each weight column
        direct_weights = []
        for col in weight_columns:
            # Infer type: use PostgreSQL type first, then sample if needed
            field_type = self._infer_type_from_samples(
                edge_table_info.name,
                edge_table_info.schema_name,
                col.name,
                col.type,
            )
            direct_weights.append(Field(name=col.name, type=field_type))

        logger.debug(
            f"Inferred {len(direct_weights)} weights for edge table "
            f"'{edge_table_info.name}': {[f.name for f in direct_weights]}"
        )

        return WeightConfig(direct=direct_weights)

    def infer_edge_config(
        self,
        introspection_result: SchemaIntrospectionResult,
        vertex_config: VertexConfig,
    ) -> EdgeConfig:
        """Infer EdgeConfig from edge tables.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()
            vertex_config: Inferred vertex configuration

        Returns:
            EdgeConfig: Inferred edge configuration
        """
        edge_tables = introspection_result.edge_tables
        edges = []

        vertex_names = vertex_config.vertex_set

        for edge_table_info in edge_tables:
            table_name = edge_table_info.name
            source_table = edge_table_info.source_table
            target_table = edge_table_info.target_table

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
            indexes = []
            # Create edge
            edge = Edge(
                source=source_table,
                target=target_table,
                indexes=indexes,
                weights=weights,
                relation=edge_table_info.relation,
            )

            edges.append(edge)
            logger.debug(
                f"Inferred edge '{table_name}' from {source_table} to {target_table}"
            )

        return EdgeConfig(edges=edges)

    def infer_schema(
        self,
        introspection_result: SchemaIntrospectionResult,
        schema_name: str | None = None,
    ) -> Schema:
        """Infer complete Schema from PostgreSQL introspection.

        Args:
            introspection_result: Result from PostgresConnection.introspect_schema()
            schema_name: Schema name (defaults to schema_name from introspection if None)

        Returns:
            Schema: Complete inferred schema with vertices, edges, and metadata
        """
        if schema_name is None:
            schema_name = introspection_result.schema_name

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
