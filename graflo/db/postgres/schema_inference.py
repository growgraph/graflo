"""Schema inference from PostgreSQL database introspection.

This module provides functionality to infer graflo Schema objects from PostgreSQL
3NF database schemas by analyzing table structures, relationships, and column types.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING


from graflo.architecture.edge import Edge, EdgeConfig, WeightConfig
from graflo.architecture.onto import Index, IndexType
from graflo.architecture.schema import Schema, SchemaMetadata
from graflo.architecture.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.onto import DBFlavor

from ...architecture.onto_sql import EdgeTableInfo, SchemaIntrospectionResult
from ..util import load_reserved_words, sanitize_attribute_name
from .conn import PostgresConnection
from .types import PostgresTypeMapper

if TYPE_CHECKING:
    from graflo.architecture.resource import Resource

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
        # Load reserved words for the target database flavor
        self.reserved_words = load_reserved_words(db_flavor)

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

    def _sanitize_schema_attributes(self, schema: Schema) -> Schema:
        """Sanitize attribute names and vertex names in the schema to avoid reserved words.

        This method modifies:
        - Field names in vertices and edges
        - Vertex names themselves
        - Edge source/target/by references to vertices
        - Resource apply lists that reference vertices

        The sanitization is deterministic: the same input always produces the same output.

        Args:
            schema: The schema to sanitize

        Returns:
            Schema with sanitized attribute names and vertex names
        """
        if not self.reserved_words:
            # No reserved words to check, return schema as-is
            return schema

        # Track name mappings for attributes (fields/weights)
        attribute_mappings: dict[str, str] = {}
        # Track name mappings for vertex names (separate from attributes)

        # First pass: Sanitize vertex dbnames
        for vertex in schema.vertex_config.vertices:
            sanitized_vertex_name = sanitize_attribute_name(
                vertex.dbname, self.reserved_words, suffix="_vertex"
            )
            if sanitized_vertex_name != vertex.dbname:
                logger.debug(
                    f"Sanitizing vertex name '{vertex.dbname}' -> '{sanitized_vertex_name}'"
                )
                vertex.dbname = sanitized_vertex_name

        # Second pass: Sanitize vertex field names
        for vertex in schema.vertex_config.vertices:
            for field in vertex.fields:
                original_name = field.name
                if original_name not in attribute_mappings:
                    sanitized_name = sanitize_attribute_name(
                        original_name, self.reserved_words
                    )
                    if sanitized_name != original_name:
                        attribute_mappings[original_name] = sanitized_name
                        logger.debug(
                            f"Sanitizing field name '{original_name}' -> '{sanitized_name}' "
                            f"in vertex '{vertex.name}'"
                        )
                    else:
                        attribute_mappings[original_name] = original_name
                else:
                    sanitized_name = attribute_mappings[original_name]

                # Update field name if it changed
                if sanitized_name != original_name:
                    field.name = sanitized_name

            # Update index field references if they were sanitized
            for index in vertex.indexes:
                updated_fields = []
                for field_name in index.fields:
                    sanitized_field_name = attribute_mappings.get(
                        field_name, field_name
                    )
                    updated_fields.append(sanitized_field_name)
                index.fields = updated_fields

        # Third pass: Normalize edge indexes for TigerGraph
        # TigerGraph requires that edges with the same relation have consistent source and target indexes
        # 1) group edges by relation
        # 2) check that for each group specified by relation the sources have the same index
        # and separately the targets have the same index
        # 3) if this is not the case, identify the most popular index
        # 4) for vertices that don't comply with the chose source/target index, we want to prepare a mapping
        # and rename relevant fields indexes
        field_index_mappings: dict[
            str, dict[str, str]
        ] = {}  # vertex_name -> {old_field: new_field}

        if schema.vertex_config.db_flavor == DBFlavor.TIGERGRAPH:
            # Group edges by relation
            edges_by_relation: dict[str | None, list[Edge]] = {}
            for edge in schema.edge_config.edges:
                relation = edge.relation
                if relation not in edges_by_relation:
                    edges_by_relation[relation] = []
                edges_by_relation[relation].append(edge)

            # Process each relation group
            for relation, relation_edges in edges_by_relation.items():
                if len(relation_edges) <= 1:
                    # Only one edge with this relation, no normalization needed
                    continue

                # Collect all vertex/index pairs using a list to capture all occurrences
                # This handles cases where a vertex appears multiple times in edges for the same relation
                source_vertex_indexes: list[tuple[str, tuple[str, ...]]] = []
                target_vertex_indexes: list[tuple[str, tuple[str, ...]]] = []

                for edge in relation_edges:
                    source_vertex = edge.source
                    target_vertex = edge.target

                    # Get primary index for source vertex
                    source_index = schema.vertex_config.index(source_vertex)
                    source_vertex_indexes.append(
                        (source_vertex, tuple(source_index.fields))
                    )

                    # Get primary index for target vertex
                    target_index = schema.vertex_config.index(target_vertex)
                    target_vertex_indexes.append(
                        (target_vertex, tuple(target_index.fields))
                    )

                # Normalize source indexes
                self._normalize_vertex_indexes(
                    source_vertex_indexes,
                    relation,
                    schema,
                    field_index_mappings,
                    "source",
                )

                # Normalize target indexes
                self._normalize_vertex_indexes(
                    target_vertex_indexes,
                    relation,
                    schema,
                    field_index_mappings,
                    "target",
                )

        # Fourth pass: the field maps from edge/relation normalization should be applied to resources:
        # new transforms should be added mapping old index names to those identified in the previous step
        if field_index_mappings:
            for resource in schema.resources:
                self._apply_field_index_mappings_to_resource(
                    resource, field_index_mappings
                )

        return schema

    def _normalize_vertex_indexes(
        self,
        vertex_indexes: list[tuple[str, tuple[str, ...]]],
        relation: str | None,
        schema: Schema,
        field_index_mappings: dict[str, dict[str, str]],
        role: str,  # "source" or "target" for logging
    ) -> None:
        """Normalize vertex indexes to use the most popular index pattern.

        For vertices that don't match the most popular index, this method:
        1. Creates field mappings (old_field -> new_field)
        2. Updates vertex indexes to match the most popular pattern
        3. Adds new fields to vertices if needed
        4. Removes old fields that are being replaced

        Args:
            vertex_indexes: List of (vertex_name, index_fields_tuple) pairs
            relation: Relation name for logging
            schema: Schema to update
            field_index_mappings: Dictionary to update with field mappings
            role: "source" or "target" for logging purposes
        """
        if not vertex_indexes:
            return

        # Extract unique vertex/index pairs (a vertex might appear multiple times)
        vertex_index_dict: dict[str, tuple[str, ...]] = {}
        for vertex_name, index_fields in vertex_indexes:
            # Only store first occurrence - we'll normalize all vertices together
            if vertex_name not in vertex_index_dict:
                vertex_index_dict[vertex_name] = index_fields

        # Check if all indexes are consistent
        indexes_list = list(vertex_index_dict.values())
        indexes_set = set(indexes_list)
        indexes_consistent = len(indexes_set) == 1

        if indexes_consistent:
            # All indexes are the same, no normalization needed
            return

        # Find most popular index
        index_counter = Counter(indexes_list)
        most_popular_index = index_counter.most_common(1)[0][0]

        # Normalize vertices that don't match
        for vertex_name, index_fields in vertex_index_dict.items():
            if index_fields == most_popular_index:
                continue

            # Initialize mappings for this vertex if needed
            if vertex_name not in field_index_mappings:
                field_index_mappings[vertex_name] = {}

            # Map old fields to new fields
            old_fields = list(index_fields)
            new_fields = list(most_popular_index)

            # Create field-to-field mapping
            # If lengths match, map positionally; otherwise map first field to first field
            if len(old_fields) == len(new_fields):
                for old_field, new_field in zip(old_fields, new_fields):
                    if old_field != new_field:
                        # Update existing mapping if it exists, otherwise create new one
                        field_index_mappings[vertex_name][old_field] = new_field
            else:
                # If lengths don't match, map the first field
                if old_fields and new_fields:
                    if old_fields[0] != new_fields[0]:
                        field_index_mappings[vertex_name][old_fields[0]] = new_fields[0]

            # Update vertex index and fields
            vertex = schema.vertex_config[vertex_name]
            existing_field_names = {f.name for f in vertex.fields}

            # Add new fields that don't exist
            for new_field in most_popular_index:
                if new_field not in existing_field_names:
                    vertex.fields.append(Field(name=new_field, type=None))
                    existing_field_names.add(new_field)

            # Remove old fields that are being replaced (not in new index)
            fields_to_remove = [
                f
                for f in vertex.fields
                if f.name in old_fields and f.name not in new_fields
            ]
            for field_to_remove in fields_to_remove:
                vertex.fields.remove(field_to_remove)

            # Update vertex index to match the most popular one
            vertex.indexes[0].fields = list(most_popular_index)

            logger.debug(
                f"Normalizing {role} index for vertex '{vertex_name}' in relation '{relation}': "
                f"{old_fields} -> {new_fields}"
            )

    def _apply_field_index_mappings_to_resource(
        self, resource: Resource, field_index_mappings: dict[str, dict[str, str]]
    ) -> None:
        """Apply field index mappings to TransformActor instances in a resource.

        For vertices that had their indexes normalized, this method updates TransformActor
        instances to map old field names to new field names in their Transform.map attribute.
        Only updates TransformActors where the vertex is confirmed to be created at that level
        (via VertexActor).

        Args:
            resource: The resource to update
            field_index_mappings: Dictionary mapping vertex names to field mappings
                                 (old_field -> new_field)
        """
        from graflo.architecture.actor import (
            ActorWrapper,
            DescendActor,
            TransformActor,
            VertexActor,
        )

        def collect_vertices_at_level(wrappers: list[ActorWrapper]) -> set[str]:
            """Collect vertices created by VertexActor instances at the current level only.

            Does not recurse into nested structures - only collects vertices from
            the immediate level.

            Args:
                wrappers: List of ActorWrapper instances

            Returns:
                set[str]: Set of vertex names created at this level
            """
            vertices = set()
            for wrapper in wrappers:
                if isinstance(wrapper.actor, VertexActor):
                    vertices.add(wrapper.actor.name)
            return vertices

        def update_transform_actor_maps(
            wrapper: ActorWrapper, parent_vertices: set[str] | None = None
        ) -> set[str]:
            """Recursively update TransformActor instances with field index mappings.

            Args:
                wrapper: ActorWrapper instance to process
                parent_vertices: Set of vertices available from parent levels (for nested structures)

            Returns:
                set[str]: Set of all vertices available at this level (including parent)
            """
            if parent_vertices is None:
                parent_vertices = set()

            # Collect vertices created at this level
            current_level_vertices = set()
            if isinstance(wrapper.actor, VertexActor):
                current_level_vertices.add(wrapper.actor.name)

            # All available vertices = current level + parent levels
            all_available_vertices = current_level_vertices | parent_vertices

            # Process TransformActor if present
            if isinstance(wrapper.actor, TransformActor):
                transform_actor: TransformActor = wrapper.actor

                def apply_mappings_to_transform(
                    mappings: dict[str, str],
                    vertex_name: str,
                    actor: TransformActor,
                ) -> None:
                    """Apply field mappings to TransformActor's transform.map attribute.

                    Args:
                        mappings: Dictionary mapping old field names to new field names
                        vertex_name: Name of the vertex these mappings belong to (for logging)
                        actor: The TransformActor instance to update
                    """
                    transform = actor.t
                    if transform.map:
                        # Update existing map: replace values and keys that match old field names
                        # First, update values
                        for map_key, map_value in transform.map.items():
                            if isinstance(map_value, str) and map_value in mappings:
                                transform.map[map_key] = mappings[map_value]

                        # if the terminal attr not in the map - add it
                        for k, v in mappings.items():
                            if v not in transform.map.values():
                                transform.map[k] = v
                    else:
                        # Create new map with all mappings
                        transform.map = mappings.copy()

                    # Update Transform object IO to reflect map edits
                    actor.t._init_io_from_map(force_init=True)

                    logger.debug(
                        f"Updated TransformActor map in resource '{resource.resource_name}' "
                        f"for vertex '{vertex_name}': {mappings}"
                    )

                target_vertex = transform_actor.vertex

                if isinstance(target_vertex, str):
                    # TransformActor has explicit target_vertex
                    if (
                        target_vertex in field_index_mappings
                        and target_vertex in all_available_vertices
                    ):
                        mappings = field_index_mappings[target_vertex]
                        if mappings:
                            apply_mappings_to_transform(
                                mappings, target_vertex, transform_actor
                            )
                        else:
                            logger.debug(
                                f"Skipping TransformActor for vertex '{target_vertex}' "
                                f"in resource '{resource.resource_name}': no mappings needed"
                            )
                    else:
                        logger.debug(
                            f"Skipping TransformActor for vertex '{target_vertex}' "
                            f"in resource '{resource.resource_name}': vertex not created at this level"
                        )
                else:
                    # TransformActor has no target_vertex
                    # Apply mappings from all available vertices (parent and current level)
                    # since transformed fields will be attributed to those vertices
                    applied_any = False
                    for vertex in all_available_vertices:
                        if vertex in field_index_mappings:
                            mappings = field_index_mappings[vertex]
                            if mappings:
                                apply_mappings_to_transform(
                                    mappings, vertex, transform_actor
                                )
                                applied_any = True

                    if not applied_any:
                        logger.debug(
                            f"Skipping TransformActor without target_vertex "
                            f"in resource '{resource.resource_name}': "
                            f"no mappings found for available vertices {all_available_vertices}"
                        )

            # Recursively process nested structures (DescendActor)
            if isinstance(wrapper.actor, DescendActor):
                # Collect vertices from all descendants at this level
                descendant_vertices = collect_vertices_at_level(
                    wrapper.actor.descendants
                )
                all_available_vertices |= descendant_vertices

                # Recursively process each descendant
                for descendant_wrapper in wrapper.actor.descendants:
                    nested_vertices = update_transform_actor_maps(
                        descendant_wrapper, parent_vertices=all_available_vertices
                    )
                    # Merge nested vertices into available vertices
                    all_available_vertices |= nested_vertices

            return all_available_vertices

        # Process the root ActorWrapper if it exists
        if hasattr(resource, "root") and resource.root is not None:
            update_transform_actor_maps(resource.root)
        else:
            logger.warning(
                f"Resource '{resource.resource_name}' does not have a root ActorWrapper. "
                f"Skipping field index mapping updates."
            )

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

        # Create schema (resources will be created separately)
        schema = Schema(
            general=metadata,
            vertex_config=vertex_config,
            edge_config=edge_config,
            resources=[],  # Resources will be created separately
        )

        # Sanitize attribute names to avoid reserved words
        schema = self._sanitize_schema_attributes(schema)

        logger.info(
            f"Successfully inferred schema '{schema_name}' with "
            f"{len(vertex_config.vertices)} vertices and "
            f"{len(list(edge_config.edges_list()))} edges"
        )

        return schema
