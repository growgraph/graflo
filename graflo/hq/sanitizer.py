"""Schema sanitization for PostgreSQL schema inference.

This module provides functionality to sanitize schema attributes to avoid
reserved words and normalize vertex indexes for specific database flavors.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING
from collections import defaultdict

from graflo.architecture.edge import Edge
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import Field
from graflo.onto import DBType

from graflo.db.util import load_reserved_words, sanitize_attribute_name

if TYPE_CHECKING:
    from graflo.architecture.resource import Resource

logger = logging.getLogger(__name__)

VERTEX_SUFFIX = "vertex"
RELATION_SUFFIX = "relation"


class SchemaSanitizer:
    """Sanitizes schema attributes to avoid reserved words and normalize indexes.

    This class handles:
    - Sanitizing vertex names and field names to avoid reserved words
    - Normalizing vertex indexes for TigerGraph (ensuring consistent indexes
      for edges with the same relation)
    - Applying field index mappings to resources
    """

    def __init__(self, db_flavor: DBType):
        """Initialize the schema sanitizer.

        Args:
            db_flavor: Target database flavor to load reserved words for
        """
        self.db_flavor = db_flavor
        self.reserved_words = load_reserved_words(db_flavor)
        self.vertex_attribute_mappings: defaultdict[str, dict[str, str]] = defaultdict(
            dict
        )
        self.vertex_mappings: dict[str, str] = {}

    def sanitize(self, schema: Schema) -> Schema:
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

        # First pass: Sanitize vertex dbnames
        for vertex in schema.vertex_config.vertices:
            if vertex.dbname is None:
                continue
            dbname = vertex.dbname
            sanitized_vertex_name = sanitize_attribute_name(
                dbname, self.reserved_words, suffix=f"_{VERTEX_SUFFIX}"
            )
            if sanitized_vertex_name != dbname:
                logger.debug(
                    f"Sanitizing vertex name '{dbname}' -> '{sanitized_vertex_name}'"
                )
                self.vertex_mappings[dbname] = sanitized_vertex_name
                vertex.dbname = sanitized_vertex_name

        # Second pass: Sanitize vertex field names
        for vertex in schema.vertex_config.vertices:
            for field in vertex.fields:
                original_name = field.name
                sanitized_name = sanitize_attribute_name(
                    original_name, self.reserved_words
                )
                if sanitized_name != original_name:
                    self.vertex_attribute_mappings[vertex.name][original_name] = (
                        sanitized_name
                    )
                    logger.debug(
                        f"Sanitizing field name '{original_name}' -> '{sanitized_name}' "
                        f"in vertex '{vertex.name}'"
                    )
                    field.name = sanitized_name

            for index in vertex.indexes:
                index.fields = [
                    self.vertex_attribute_mappings[vertex.name].get(item, item)
                    for item in index.fields
                ]

        vertex_names = {vertex.dbname for vertex in schema.vertex_config.vertices}

        for edge in schema.edge_config.edges:
            if not edge.relation:
                continue

            original = edge.relation_dbname
            if original is None:
                continue

            # First pass: sanitize against reserved words
            sanitized = sanitize_attribute_name(
                original,
                self.reserved_words,
                suffix=f"_{RELATION_SUFFIX}",
            )

            # Second pass: avoid collision with vertex names
            if sanitized in vertex_names:
                base = f"{sanitized}_{RELATION_SUFFIX}"
                candidate = base
                counter = 1

                while candidate in vertex_names:
                    candidate = f"{base}_{counter}"
                    counter += 1

                sanitized = candidate

            # Update only if needed
            if sanitized != original:
                edge.relation_dbname = sanitized

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

        if schema.vertex_config.db_flavor == DBType.TIGERGRAPH:
            # Group edges by relation
            edges_by_relation: dict[str | None, list[Edge]] = {}
            for edge in schema.edge_config.edges:
                # Use sanitized dbname when grouping by relation for TigerGraph
                relation = (
                    edge.relation_dbname
                    if edge.relation_dbname is not None
                    else edge.relation
                )
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
