"""TigerGraph schema DDL statement builders and schema-change orchestration."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Literal

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema import Schema
from graflo.architecture.schema.db_aware import EdgeConfigDBAware
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import FieldType, Vertex
from graflo.db.field_type_support import tigergraph_type_for_field
from graflo.db.tigergraph.ddl_utils import (
    edge_identity_discriminator_fields,
    tigergraph_ddl_edge_projection,
)
from graflo.db.tigergraph.gsql_literals import gsql_default_literal
from graflo.db.tigergraph.name_validation import validate_tigergraph_schema_name
from graflo.db.tigergraph.onto import TIGERGRAPH_TYPE_ALIASES, VALID_TIGERGRAPH_TYPES
from graflo.onto import DBType

if TYPE_CHECKING:
    from graflo.db.tigergraph.conn import TigerGraphConnection

logger = logging.getLogger(__name__)


def _wrap_tg_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            raise

    return wrapper


class SchemaDdlBuilder:
    """Builds GSQL ADD VERTEX/EDGE statements and runs SCHEMA_CHANGE jobs."""

    def __init__(self, conn: TigerGraphConnection) -> None:
        self._conn = conn

    def _gsql_vertex_field_def(
        self,
        *,
        logical_vertex_name: str,
        field_name: str,
        tg_type: str,
        db_profile: DatabaseProfile | None,
    ) -> str:
        """Single attribute fragment: ``name TYPE`` or ``name TYPE DEFAULT ...``."""
        line = f"{field_name} {tg_type}"
        if db_profile is None or not db_profile.has_vertex_property_default(
            logical_vertex_name, field_name
        ):
            return line
        raw = db_profile.vertex_property_default(logical_vertex_name, field_name)
        if raw is None:
            return line
        lit = gsql_default_literal(raw)
        return f"{line} DEFAULT {lit}"

    def _get_vertex_add_statement(
        self,
        vertex: Vertex,
        vertex_config,
        *,
        db_profile: DatabaseProfile | None = None,
    ) -> str:
        """Generate ADD VERTEX statement for a schema change job.

        Args:
            vertex: Vertex object to generate statement for
            vertex_config: Vertex configuration
            db_profile: Optional profile for ``default_property_values`` (GSQL DEFAULT clauses).

        Returns:
            str: GSQL ADD VERTEX statement
        """
        profile = db_profile
        if profile is None and hasattr(vertex_config, "db_profile"):
            profile = getattr(vertex_config, "db_profile", None)

        vertex_dbname = vertex_config.vertex_dbname(vertex.name)
        logical = vertex.name
        index_fields = vertex_config.identity_fields(vertex.name)

        if len(index_fields) == 0:
            raise ValueError(
                f"Vertex '{vertex_dbname}' must have at least one index field"
            )

        # Get field type for primary key field(s) - convert FieldType enum to string
        field_type_map = {}
        for f in vertex.properties:
            field_type_map[f.name] = tigergraph_type_for_field(f)

        # Format all fields
        all_fields = []
        for field in vertex.properties:
            all_fields.append((field.name, tigergraph_type_for_field(field)))

        if len(index_fields) == 1:
            # Single field: PRIMARY_ID when no DEFAULT on the id; otherwise PRIMARY KEY
            # (GSQL does not allow DEFAULT on the PRIMARY_ID id_name id_type fragment).
            primary_field_name = index_fields[0]
            primary_field_type = field_type_map.get(
                primary_field_name, FieldType.STRING.value
            )

            other_fields = [
                (name, ftype)
                for name, ftype in all_fields
                if name != primary_field_name
            ]

            primary_attr = self._gsql_vertex_field_def(
                logical_vertex_name=logical,
                field_name=primary_field_name,
                tg_type=primary_field_type,
                db_profile=profile,
            )
            # PRIMARY_ID form is `PRIMARY_ID id_name id_type` — DEFAULT is only valid on
            # attributes in attribute_list, not on the PRIMARY_ID fragment (GSQL parse error).
            # When the identity field needs DEFAULT, use PRIMARY KEY form instead:
            # `name TYPE [DEFAULT ...] PRIMARY KEY`.
            primary_default_val = (
                profile.vertex_property_default(logical, primary_field_name)
                if profile is not None
                else None
            )
            if primary_default_val is not None:
                field_parts = [f"{primary_attr} PRIMARY KEY"]
                vertex_with = 'WITH STATS="OUTDEGREE_BY_EDGETYPE"'
            else:
                field_parts = [f"PRIMARY_ID {primary_attr}"]
                vertex_with = (
                    'WITH STATS="OUTDEGREE_BY_EDGETYPE", PRIMARY_ID_AS_ATTRIBUTE="true"'
                )
            for name, ftype in other_fields:
                field_parts.append(
                    self._gsql_vertex_field_def(
                        logical_vertex_name=logical,
                        field_name=name,
                        tg_type=ftype,
                        db_profile=profile,
                    )
                )

            field_definitions = ",\n        ".join(field_parts)

            return (
                f"ADD VERTEX {vertex_dbname} (\n"
                f"        {field_definitions}\n"
                f"    ) {vertex_with}"
            )
        else:
            # Composite key: use PRIMARY KEY syntax
            field_parts = [
                self._gsql_vertex_field_def(
                    logical_vertex_name=logical,
                    field_name=name,
                    tg_type=ftype,
                    db_profile=profile,
                )
                for name, ftype in all_fields
            ]
            vindex = "(" + ", ".join(index_fields) + ")"
            field_parts.append(f"PRIMARY KEY {vindex}")

            field_definitions = ",\n        ".join(field_parts)

            return (
                f"ADD VERTEX {vertex_dbname} (\n"
                f"        {field_definitions}\n"
                f'    ) WITH STATS="OUTDEGREE_BY_EDGETYPE"'
            )

    def _edge_for_tigergraph_ddl(self, edge: Edge, ec_db: EdgeConfigDBAware) -> Edge:
        """Deep-copy edge with TigerGraph-effective weights for GSQL (non-mutating on schema)."""
        return tigergraph_ddl_edge_projection(edge, ec_db)

    def _validate_tigergraph_vertex_properties(self, vertex: Vertex) -> None:
        """Reject reserved or invalid TigerGraph names on vertex attributes."""
        for field in vertex.properties:
            validate_tigergraph_schema_name(field.name, "vertex property")

    def _validate_tigergraph_edge_property_names(
        self, edge: Edge, edge_config_db: EdgeConfigDBAware
    ) -> None:
        """Reject reserved or invalid names on edge attributes and discriminators."""
        ddl_edge = self._edge_for_tigergraph_ddl(edge, edge_config_db)
        names: set[str] = {f.name for f in ddl_edge.properties}
        names.update(edge_identity_discriminator_fields(ddl_edge))
        for attr in names:
            validate_tigergraph_schema_name(attr, "edge attribute")

    def _format_edge_attributes(
        self,
        edge: Edge,
        exclude_fields: set[str] | None = None,
        *,
        db_profile: DatabaseProfile | None = None,
        edge_id: EdgeId | None = None,
    ) -> str:
        """Format edge attributes for GSQL ADD DIRECTED EDGE statement.

        Args:
            edge: Edge object to format attributes for
            exclude_fields: Optional set of field names to exclude from attributes
            db_profile: Optional profile for ``default_property_values`` (GSQL DEFAULT).
            edge_id: Logical edge identity; defaults to ``edge.edge_id``.

        Returns:
            str: Formatted attribute string (e.g., "    date STRING,\n    relation STRING")
        """
        if not edge.properties:
            return ""

        if exclude_fields is None:
            exclude_fields = set()

        eid = edge_id if edge_id is not None else edge.edge_id

        attr_parts = []
        for field in edge.properties:
            field_name = field.name
            if field_name not in exclude_fields:
                field_type = tigergraph_type_for_field(field)
                segment = f"{field_name} {field_type}"
                if db_profile is not None and db_profile.has_edge_property_default(
                    eid, field_name
                ):
                    raw = db_profile.edge_property_default(eid, field_name)
                    if raw is not None:
                        lit = gsql_default_literal(raw)
                        segment = f"{segment} DEFAULT {lit}"
                attr_parts.append(f"    {segment}")

        return ",\n".join(attr_parts)

    @staticmethod
    def _tigergraph_edge_ddl_kind(edge: Edge) -> Literal["directed", "undirected"]:
        return "undirected" if not edge.directed else "directed"

    @staticmethod
    def _edge_ddl_keyword(kind: Literal["directed", "undirected"]) -> str:
        return "UNDIRECTED" if kind == "undirected" else "DIRECTED"

    def _tigergraph_reverse_edge_name(
        self,
        edge: Edge,
        db_profile: DatabaseProfile | None,
    ) -> str | None:
        if db_profile is None or db_profile.db_flavor != DBType.TIGERGRAPH:
            return None
        return db_profile.edge_reverse_edge_name(edge.edge_id)

    def _validate_tigergraph_edge_ddl_config(
        self,
        edge: Edge,
        db_profile: DatabaseProfile | None,
    ) -> tuple[Literal["directed", "undirected"], str | None]:
        kind = self._tigergraph_edge_ddl_kind(edge)
        reverse_edge = self._tigergraph_reverse_edge_name(edge, db_profile)
        if reverse_edge is not None:
            if kind == "undirected":
                raise ValueError(
                    f"reverse_edge cannot be set for undirected edge {edge.edge_id!r}"
                )
            validate_tigergraph_schema_name(reverse_edge, "reverse_edge")
        return kind, reverse_edge

    def _get_edge_add_statement(
        self,
        edge: Edge,
        *,
        relation_name: str,
        source_vertex: str,
        target_vertex: str,
        db_profile: DatabaseProfile | None = None,
    ) -> str:
        """Generate ADD DIRECTED/UNDIRECTED EDGE statement for a schema change job.

        Args:
            edge: Edge object to generate statement for

        Returns:
            str: GSQL ADD edge statement (optionally with WITH REVERSE_EDGE)
        """
        # TigerGraph discriminators are derived from logical edge identity.
        indexed_field_names = edge_identity_discriminator_fields(edge)

        # IMPORTANT: In TigerGraph, discriminator fields MUST also be edge attributes.
        # If an indexed field is not in attributes, we need to add it.
        existing_weight_names = {f.name for f in edge.properties}

        # Add any indexed fields that are missing from attributes
        for field_name in indexed_field_names:
            if field_name not in existing_weight_names:
                from graflo.architecture.schema.edge import Field

                edge.properties.append(Field(name=field_name, type=FieldType.STRING))
                existing_weight_names.add(field_name)
                logger.info(
                    f"Added indexed field '{field_name}' to edge attributes for discriminator compatibility"
                )

        # Format edge attributes, excluding discriminator fields (they're in DISCRIMINATOR clause)
        edge_attrs = self._format_edge_attributes(
            edge,
            exclude_fields=indexed_field_names,
            db_profile=db_profile,
            edge_id=edge.edge_id,
        )

        # Build discriminator clause with all indexed fields
        # DISCRIMINATOR goes INSIDE parentheses, on same line as FROM/TO, with types
        # Format: FROM company, TO company, DISCRIMINATOR(relation STRING), date STRING, ...

        # Get field types for discriminator fields
        field_types = {}
        if edge.properties:
            for field in edge.properties:
                field_types[field.name] = tigergraph_type_for_field(field)

        # Use sanitized dbname for schema names when available
        relation_db = relation_name

        # Build FROM/TO line with discriminator
        from_to_parts = [
            f"        FROM {source_vertex}",
            f"        TO {target_vertex}",
        ]

        if indexed_field_names:
            # Format discriminator with types: DISCRIMINATOR(field1 TYPE1, field2 TYPE2)
            discriminator_parts = []
            for field_name in sorted(indexed_field_names):
                field_type = field_types.get(field_name, "STRING")  # Default to STRING
                discriminator_parts.append(f"{field_name} {field_type}")

            discriminator_str = f"DISCRIMINATOR({', '.join(discriminator_parts)})"
            from_to_parts.append(f"        {discriminator_str}")
            logger.info(
                f"Added discriminator for edge {relation_db}: {', '.join(discriminator_parts)}"
            )
        else:
            logger.debug(
                f"No identity discriminator fields found for edge {relation_db}. "
                f"Identities: {edge.identities}, relation: {edge.relation}"
            )

        # Combine FROM/TO and discriminator with commas
        from_to_line = ",\n".join(from_to_parts)

        ddl_kind, reverse_edge = self._validate_tigergraph_edge_ddl_config(
            edge, db_profile
        )
        ddl_keyword = self._edge_ddl_keyword(ddl_kind)
        reverse_suffix = (
            f' WITH REVERSE_EDGE="{reverse_edge}"' if reverse_edge is not None else ""
        )

        # Build the complete statement
        if edge_attrs:
            body = (
                f"ADD {ddl_keyword} EDGE {relation_db} (\n"
                f"{from_to_line},\n"
                f"{edge_attrs}\n"
                f"    ){reverse_suffix}"
            )
            return body
        body = f"ADD {ddl_keyword} EDGE {relation_db} (\n{from_to_line}\n    ){reverse_suffix}"
        return body

    def _get_edge_group_create_statement(
        self,
        edges: list[Edge],
        *,
        relation_name: str,
        source_vertices: dict[int, str],
        target_vertices: dict[int, str],
        db_profile: DatabaseProfile | None = None,
    ) -> str:
        """Generate ADD DIRECTED EDGE statement for a group of edges with the same relation.

        TigerGraph requires edges of the same type to be created in a single statement
        with multiple FROM/TO pairs separated by |.

        Args:
            edges: List of Edge objects with the same relation (edge type)

        Returns:
            str: GSQL ADD DIRECTED EDGE statement with multiple FROM/TO pairs
        """
        if not edges:
            raise ValueError("Cannot create edge statement from empty edge list")

        # Use the first edge to determine attributes and discriminator
        # (all edges of the same relation should have the same schema)
        first_edge = edges[0]
        relation = relation_name

        # Collect identity discriminator fields (same logic as _get_edge_add_statement)
        indexed_field_names = edge_identity_discriminator_fields(first_edge)

        # Ensure indexed fields are in attributes (same logic as _get_edge_add_statement)
        existing_weight_names = {f.name for f in first_edge.properties}

        for field_name in indexed_field_names:
            if field_name not in existing_weight_names:
                from graflo.architecture.schema.edge import Field

                first_edge.properties.append(
                    Field(name=field_name, type=FieldType.STRING)
                )
                existing_weight_names.add(field_name)

        # Format edge attributes, excluding discriminator fields
        edge_attrs = self._format_edge_attributes(
            first_edge,
            exclude_fields=indexed_field_names,
            db_profile=db_profile,
            edge_id=first_edge.edge_id,
        )

        # Get field types for discriminator fields
        field_types = {}
        if first_edge.properties:
            for field in first_edge.properties:
                field_types[field.name] = tigergraph_type_for_field(field)

        # Build FROM/TO pairs for all edges, separated by |
        from_to_lines = []
        for edge in edges:
            # Build FROM/TO line: "FROM A, TO B" or "FROM A, TO B, DISCRIMINATOR(...)"
            from_to_parts = [
                f"FROM {source_vertices[id(edge)]}",
                f"TO {target_vertices[id(edge)]}",
            ]

            # Add discriminator if needed (same for all edges of the same relation)
            if indexed_field_names:
                discriminator_parts = []
                for field_name in sorted(indexed_field_names):
                    field_type = field_types.get(field_name, "STRING")
                    discriminator_parts.append(f"{field_name} {field_type}")

                discriminator_str = f"DISCRIMINATOR({', '.join(discriminator_parts)})"
                from_to_parts.append(discriminator_str)

            # Combine FROM/TO and discriminator with commas on one line
            from_to_line = ", ".join(from_to_parts)
            from_to_lines.append(f"    {from_to_line}")

        # Join all FROM/TO pairs with |
        all_from_to = " |\n".join(from_to_lines)

        ddl_kind, reverse_edge = self._validate_tigergraph_edge_ddl_config(
            first_edge, db_profile
        )
        ddl_keyword = self._edge_ddl_keyword(ddl_kind)
        reverse_suffix = (
            f' WITH REVERSE_EDGE="{reverse_edge}"' if reverse_edge is not None else ""
        )

        # Build the complete statement
        if edge_attrs:
            return (
                f"ADD {ddl_keyword} EDGE {relation} (\n{all_from_to},\n{edge_attrs}\n    )"
                f"{reverse_suffix}"
            )
        return (
            f"ADD {ddl_keyword} EDGE {relation} (\n{all_from_to}\n    ){reverse_suffix}"
        )

    def _batch_schema_statements(
        self, schema_change_stmts: list[str], graph_name: str, max_job_size: int
    ) -> list[list[str]]:
        """Batch schema change statements into groups that fit within max_job_size.

        Preserves input order: statements are packed sequentially into batches
        without reordering. Callers must pass vertices before edges when both are
        required (see ``_define_schema_local``).

        Args:
            schema_change_stmts: List of schema change statements to batch
            graph_name: Name of the graph (used for size estimation)
            max_job_size: Maximum size in characters for a single job

        Returns:
            List of batches, where each batch is a list of statements
        """
        if not schema_change_stmts:
            return []

        # Calculate base overhead for a job
        # Use worst-case job name length (multi-batch format) for conservative estimation
        worst_case_job_name = (
            f"schema_change_{graph_name}_batch_999"  # Use large number for worst case
        )
        base_template = (
            f"USE GRAPH {graph_name}\n"
            f"CREATE SCHEMA_CHANGE JOB {worst_case_job_name} FOR GRAPH {graph_name} {{\n"
            f"}}\n"
            f"RUN SCHEMA_CHANGE JOB {worst_case_job_name}"
        )
        base_overhead = len(base_template)

        # Each statement adds 5 characters: first gets "    " (4) + ";" (1),
        # subsequent get ";\n    " (5) between statements, final ";" (1) is included
        # For N statements: 4 (first indent) + (N-1)*5 (separators) + 1 (final semicolon) = 5*N

        def estimate_batch_size(stmts: list[str]) -> int:
            """Estimate the total size of a batch of statements."""
            if not stmts:
                return base_overhead
            total_stmt_size = sum(len(stmt) for stmt in stmts)
            return base_overhead + total_stmt_size + 5 * len(stmts)

        # Calculate total estimated size for all statements
        num_statements = len(schema_change_stmts)
        total_stmt_size = sum(len(stmt) for stmt in schema_change_stmts)
        estimated_size = base_overhead + total_stmt_size + 5 * num_statements

        # If everything fits in one batch, return single batch
        if estimated_size <= max_job_size:
            logger.info(
                f"Applying schema change as single job (estimated size: {estimated_size} chars)"
            )
            return [schema_change_stmts]

        # Need to split into multiple batches while preserving statement order.
        batches: list[list[str]] = []
        current_batch: list[str] = []

        for stmt in schema_change_stmts:
            candidate = current_batch + [stmt]
            if estimate_batch_size(candidate) <= max_job_size:
                current_batch.append(stmt)
                continue

            if current_batch:
                batches.append(current_batch)

            single_stmt_size = estimate_batch_size([stmt])
            if single_stmt_size > max_job_size:
                logger.warning(
                    f"Statement exceeds max_job_size ({single_stmt_size} > {max_job_size}). "
                    f"Will attempt to execute anyway, but may fail."
                )
            current_batch = [stmt]

        if current_batch:
            batches.append(current_batch)

        logger.info(
            f"Large schema detected (estimated size: {estimated_size} chars). "
            f"Splitting into {len(batches)} batches."
        )

        return batches

    def _define_schema_local(self, schema: Schema) -> None:
        """Define TigerGraph schema locally for the current graph using a SCHEMA_CHANGE job.

        Args:
            schema: Schema definition
        """
        from graflo.db.field_type_support import assert_schema_field_types_supported

        assert_schema_field_types_supported(DBType.TIGERGRAPH, schema)
        graph_name = self._conn._require_configured_graph_name()

        # Validate graph name
        validate_tigergraph_schema_name(graph_name, "graph")

        vertex_config = schema.core_schema.vertex_config
        edge_config = schema.core_schema.edge_config
        db_schema = schema.resolve_db_aware(DBType.TIGERGRAPH)

        vertex_stmts = []
        edge_stmts = []

        # Vertices
        for vertex in vertex_config.vertices:
            # Validate vertex name
            vertex_dbname = db_schema.vertex_config.vertex_dbname(vertex.name)
            validate_tigergraph_schema_name(vertex_dbname, "vertex")
            self._validate_tigergraph_vertex_properties(vertex)
            stmt = self._get_vertex_add_statement(
                vertex,
                db_schema.vertex_config,
                db_profile=db_schema.db_profile,
            )
            vertex_stmts.append(stmt)

        # Edges - group by relation since TigerGraph requires edges of the same type
        # to be created in a single statement with multiple FROM/TO pairs
        edges_to_create = list(edge_config.values())
        source_vertices: dict[int, str] = {}
        target_vertices: dict[int, str] = {}
        relation_names: dict[int, str] = {}
        for edge in edges_to_create:
            runtime = db_schema.edge_config.runtime(edge)
            source_vertices[id(edge)] = runtime.source_storage
            target_vertices[id(edge)] = runtime.target_storage
            edge_dbname = runtime.relation_name or f"{edge.source}_{edge.target}"
            relation_names[id(edge)] = edge_dbname
            validate_tigergraph_schema_name(edge_dbname, "edge")
            self._validate_tigergraph_edge_property_names(edge, db_schema.edge_config)

        # Group edges by DDL kind, relation name, and reverse_edge pairing
        edges_by_group: dict[tuple[str, str, str | None], list[Edge]] = defaultdict(
            list
        )
        for edge in edges_to_create:
            ddl_kind = self._tigergraph_edge_ddl_kind(edge)
            reverse_edge = self._tigergraph_reverse_edge_name(
                edge, db_schema.db_profile
            )
            key = (ddl_kind, relation_names[id(edge)], reverse_edge)
            edges_by_group[key].append(edge)

        # Create one statement per group with all FROM/TO pairs
        for (_ddl_kind, relation, _reverse_edge), edge_group in edges_by_group.items():
            ddl_edges = [
                self._edge_for_tigergraph_ddl(e, db_schema.edge_config)
                for e in edge_group
            ]
            ddl_source_vertices = {
                id(de): source_vertices[id(og)]
                for de, og in zip(ddl_edges, edge_group, strict=True)
            }
            ddl_target_vertices = {
                id(de): target_vertices[id(og)]
                for de, og in zip(ddl_edges, edge_group, strict=True)
            }
            stmt = self._get_edge_group_create_statement(
                ddl_edges,
                relation_name=relation,
                source_vertices=ddl_source_vertices,
                target_vertices=ddl_target_vertices,
                db_profile=db_schema.db_profile,
            )
            edge_stmts.append(stmt)

        if not vertex_stmts and not edge_stmts:
            logger.debug(f"No schema changes to apply for graph '{graph_name}'")
            return

        # Estimate the size of the GSQL command to determine if we need to split it.
        # Large SCHEMA_CHANGE JOBs (>30k chars) can cause parser failures with misleading errors
        # like "Missing return statement" (which is actually a parser size limit issue).
        # Vertices and edges are batched separately so every vertex type exists before
        # any ADD EDGE statement runs.
        max_job_size = self._conn.config.max_job_size
        vertex_batches = self._batch_schema_statements(
            vertex_stmts, graph_name, max_job_size
        )
        edge_batches = self._batch_schema_statements(
            edge_stmts, graph_name, max_job_size
        )
        batches = vertex_batches + edge_batches

        # Execute batches sequentially
        for batch_idx, batch_stmts in enumerate(batches):
            job_name = (
                f"schema_change_{graph_name}_batch_{batch_idx}"
                if len(batches) > 1
                else f"schema_change_{graph_name}"
            )

            # First, try to drop the job if it exists (ignore errors if it doesn't)
            try:
                drop_job_cmd = f"USE GRAPH {graph_name}\nDROP JOB {job_name}"
                self._conn._execute_gsql(drop_job_cmd)
                logger.debug(f"Dropped existing schema change job '{job_name}'")
            except Exception as e:
                err_str = str(e).lower()
                # Ignore errors if job doesn't exist
                if "not found" in err_str or "could not be found" in err_str:
                    logger.debug(
                        f"Schema change job '{job_name}' does not exist, skipping drop"
                    )
                else:
                    logger.debug(f"Could not drop schema change job '{job_name}': {e}")

            # Create and run SCHEMA_CHANGE job for this batch
            gsql_commands = [
                f"USE GRAPH {graph_name}",
                f"CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {graph_name} {{",
                "    " + ";\n    ".join(batch_stmts) + ";",
                "}",
                f"RUN SCHEMA_CHANGE JOB {job_name}",
            ]

            full_gsql = "\n".join(gsql_commands)
            actual_size = len(full_gsql)

            # Safety check: warn if actual size exceeds limit (indicates estimation error)
            if actual_size > self._conn.config.max_job_size:
                logger.warning(
                    f"Batch {batch_idx + 1} actual size ({actual_size} chars) exceeds limit ({self._conn.config.max_job_size} chars). "
                    f"This may cause parser errors. Consider reducing max_job_size or improving estimation."
                )

            logger.info(
                f"Applying schema change batch {batch_idx + 1}/{len(batches)} for graph '{graph_name}' "
                f"({len(batch_stmts)} statements, {actual_size} chars)"
            )
            if actual_size < 5000:  # Only log full command if it's reasonably small
                logger.debug(f"GSQL command:\n{full_gsql}")
            else:
                logger.debug(f"GSQL command size: {actual_size} characters")

            try:
                result = self._conn._execute_gsql(full_gsql)
                logger.debug(f"Schema change batch {batch_idx + 1} result: {result}")

                # Check if result indicates success - should contain "Local schema change succeeded." near the end
                result_str = str(result) if result else ""
                if result_str:
                    # Check for success message near the end (last 500 characters to handle long outputs)
                    result_tail = (
                        result_str[-500:] if len(result_str) > 500 else result_str
                    )
                    if "Local schema change succeeded." not in result_tail:
                        error_msg = (
                            f"Schema change job batch {batch_idx + 1} did not report success. "
                            f"Expected 'Local schema change succeeded.' near the end of the result. "
                            f"Result (last 500 chars): {result_tail}"
                        )
                        logger.error(error_msg)
                        logger.error(f"Full result: {result_str}")
                        raise RuntimeError(error_msg)

                # Check if result indicates an error - be more lenient with error detection
                # Only treat as error if result explicitly contains error indicators
                if (
                    result
                    and result_str
                    and (
                        "Encountered" in result_str
                        or "syntax error" in result_str.lower()
                        or "parse error" in result_str.lower()
                        or "missing return statement" in result_str.lower()
                    )
                ):
                    # "Missing return statement" is a misleading error - it's actually a parser size limit
                    # SCHEMA_CHANGE JOB doesn't require RETURN statements, so this indicates parser failure
                    if "missing return statement" in result_str.lower():
                        error_msg = (
                            f"Schema change job batch {batch_idx + 1} failed with parser error. "
                            f"This is likely due to the GSQL command size ({actual_size} chars) exceeding "
                            f"TigerGraph's parser limit (~30-40K chars). The 'Missing return statement' error "
                            f"is misleading - SCHEMA_CHANGE JOB doesn't require RETURN statements. "
                            f"Original error: {result}"
                        )
                    else:
                        error_msg = f"Schema change job batch {batch_idx + 1} reported an error: {result}"

                    logger.error(error_msg)
                    logger.error(
                        f"GSQL command that failed (first 1000 chars):\n{full_gsql[:1000]}..."
                    )
                    raise RuntimeError(error_msg)
            except Exception as e:
                logger.error(
                    f"Failed to execute schema change batch {batch_idx + 1}: {e}"
                )
                raise

        # Verify that the schema was actually created by checking vertex and edge types
        # Wait a moment for schema changes to propagate (after all batches)

        time.sleep(1.0)  # Increased wait time

        with self._conn._ensure_graph_context(graph_name):
            vertex_types = self._conn._get_vertex_types()
            edge_types = self._conn._get_edge_types()

            # Use vertex_dbname instead of v.name to match what TigerGraph actually creates
            # vertex_dbname returns dbname if set, otherwise None - fallback to v.name if None
            expected_vertex_types = set()
            for v in vertex_config.vertices:
                try:
                    dbname = db_schema.vertex_config.vertex_dbname(v.name)
                    # If dbname is None, use vertex name
                    expected_name = dbname if dbname is not None else v.name
                except (KeyError, AttributeError):
                    # Fallback to vertex name if vertex_dbname fails
                    expected_name = v.name
                expected_vertex_types.add(expected_name)

            expected_edge_types = {
                relation_names[id(e)]
                for e in edges_to_create
                if relation_names.get(id(e))
            }

            # Convert to sets for case-insensitive comparison
            # TigerGraph may capitalize vertex names, so compare case-insensitively
            vertex_types_lower = {vt.lower() for vt in vertex_types}
            expected_vertex_types_lower = {evt.lower() for evt in expected_vertex_types}

            missing_vertices_lower = expected_vertex_types_lower - vertex_types_lower
            # Convert back to original case for error message
            missing_vertices = {
                evt
                for evt in expected_vertex_types
                if evt.lower() in missing_vertices_lower
            }

            missing_edges = expected_edge_types - set(edge_types)

            if missing_vertices or missing_edges:
                error_msg = (
                    f"Schema change job completed but types were not created correctly. "
                    f"Missing vertex types: {missing_vertices}, "
                    f"Missing edge types: {missing_edges}. "
                    f"Created vertex types: {vertex_types}, "
                    f"Created edge types: {edge_types}."
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            logger.info(
                f"Schema verified: {len(vertex_types)} vertex types, {len(edge_types)} edge types created"
            )

    def _format_vertex_fields(self, vertex: Vertex) -> str:
        """
        Format vertex fields for GSQL CREATE VERTEX statement.

        Uses Field objects with types, applying TigerGraph defaults (STRING for None types).
        Formats fields as: field_name TYPE

        Args:
            vertex: Vertex object with Field definitions

        Returns:
            str: Formatted field definitions for GSQL CREATE VERTEX statement
        """
        fields = vertex.properties

        if not fields:
            # Default fields if none specified
            return 'name STRING DEFAULT "",\n    properties MAP<STRING, STRING> DEFAULT (map())'

        field_list = []
        for field in fields:
            # Format as: field_name TYPE (DEFAULT clauses live in schema.db_profile.default_property_values)
            field_list.append(f"{field.name} {tigergraph_type_for_field(field)}")

        return ",\n    ".join(field_list)

    def _format_edge_attributes_for_create(self, edge: Edge) -> str:
        """
        Format edge attributes for GSQL CREATE EDGE statement.

        Edge properties come from edge.properties (list of Field objects).
        Each attribute field needs to be included in the CREATE EDGE statement with its type.
        """
        attrs = []

        if edge.properties:
            for field in edge.properties:
                field_name = field.name
                tg_type = tigergraph_type_for_field(field)
                attrs.append(f"{field_name} {tg_type}")

        return ",\n    " + ",\n    ".join(attrs) if attrs else ""

    def _get_tigergraph_type(self, field_type: FieldType | str | None) -> str:
        """
        Convert a scalar field type to TigerGraph type string.

        Prefer :func:`tigergraph_type_for_field` when a full ``Field`` is available
        (required for ``LIST<item>`` composition).
        """
        if field_type is None:
            return FieldType.STRING.value

        if isinstance(field_type, FieldType):
            if field_type == FieldType.LIST:
                raise ValueError(
                    "Bare LIST is not a TigerGraph type; use tigergraph_type_for_field "
                    "with item_type to emit LIST<item>"
                )
            return field_type.value

        if hasattr(field_type, "value"):
            enum_value_str = str(field_type.value).upper()
            if enum_value_str in VALID_TIGERGRAPH_TYPES:
                return enum_value_str
            return enum_value_str

        field_type_str = str(field_type).upper()
        if field_type_str in VALID_TIGERGRAPH_TYPES:
            return field_type_str

        return TIGERGRAPH_TYPE_ALIASES.get(field_type_str, FieldType.STRING.value)
