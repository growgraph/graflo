"""TigerGraph graph lifecycle and schema administration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.graph_types import Index
from graflo.architecture.schema import Schema
from graflo.architecture.schema.db_aware import VertexConfigDBAware
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import VertexConfig
from graflo.db.conn import NamespaceNotFoundError, SchemaExistsError
from graflo.db.tigergraph.gsql_parsers import (
    gsql_result_has_error,
    is_not_found_error,
    parse_show_edge_output,
    parse_show_graph_output,
    parse_show_vertex_output,
)
from graflo.db.tigergraph.name_validation import validate_tigergraph_schema_name
from graflo.onto import DBType

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _wrap_tg_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            raise

    return wrapper


class GraphAdmin:
    def __init__(self, conn) -> None:
        self._conn = conn

    def graph_exists(self, name: str) -> bool:
        """
        Check if a graph with the given name exists.

        Prefers `SHOW GRAPH *` parsing for deterministic existence checks,
        with a best-effort fallback to `USE GRAPH` output heuristics.

        Args:
            name: Name of the graph to check

        Returns:
            bool: True if the graph exists, False otherwise
        """
        normalized_name = name.strip().lower()
        if not normalized_name:
            return False

        try:
            result = self._conn._execute_gsql("USE GLOBAL\nSHOW GRAPH *")
            graph_names = parse_show_graph_output(str(result))
            if graph_names:
                return any(g.lower() == normalized_name for g in graph_names)
            logger.debug(
                "SHOW GRAPH * returned no parsed graphs; falling back to USE GRAPH check for '%s'",
                name,
            )
        except Exception as e:
            logger.debug(f"SHOW GRAPH check failed for graph '{name}': {e}")

        try:
            result = self._conn._execute_gsql(f"USE GRAPH {name}")
            result_str = str(result).lower()
            return (
                "does not exist" not in result_str and "doesn't exist" not in result_str
            )
        except Exception as e:
            logger.debug(f"Fallback USE GRAPH check failed for '{name}': {e}")
            error_str = str(e).lower()
            if "does not exist" in error_str or "doesn't exist" in error_str:
                return False
            return False

    def create_database(
        self,
        name: str,
        vertex_names: list[str] | None = None,
        edge_names: list[str] | None = None,
    ):
        """
        Create a TigerGraph database (graph) using GSQL commands.

        This method creates a graph with explicitly attached vertices and edges.
        Example: CREATE GRAPH researchGraph (author, paper, wrote)

        This method uses direct REST API calls to execute GSQL commands
        that create and use the graph. Supported in TigerGraph version 4.2.2+.

        Args:
            name: Name of the graph to create
            vertex_names: Optional list of vertex type names to attach to the graph
            edge_names: Optional list of edge type names to attach to the graph

        Raises:
            RuntimeError: If graph already exists or creation fails
        """
        # Check if graph already exists first
        if self._conn.graph_exists(name):
            raise RuntimeError(f"Graph '{name}' already exists")

        try:
            # Build the list of types to include in CREATE GRAPH
            all_types = []
            if vertex_names:
                all_types.extend(vertex_names)
            if edge_names:
                all_types.extend(edge_names)

            # Format the CREATE GRAPH command with types
            if all_types:
                types_str = ", ".join(all_types)
                gsql_commands = f"CREATE GRAPH {name} ({types_str})\nUSE GRAPH {name}"
            else:
                # Fallback to empty graph if no types provided
                gsql_commands = f"CREATE GRAPH {name}()\nUSE GRAPH {name}"

            # Execute using direct GSQL REST API which handles authentication
            logger.debug(f"Creating graph '{name}' via GSQL: {gsql_commands}")
            try:
                result = self._conn._execute_gsql(gsql_commands)
                result_str = str(result)
                result_lower = result_str.lower()

                # Check for explicit failure
                if gsql_result_has_error(result_str):
                    error_msg = f"Failed to create graph '{name}': {result_str}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                logger.info(
                    f"Successfully created graph '{name}' with types {all_types}: {result_str}"
                )
                # Verify the result doesn't indicate the graph already existed
                if (
                    "already exists" in result_lower
                    or "duplicate" in result_lower
                    or "graph already exists" in result_lower
                ):
                    raise RuntimeError(f"Graph '{name}' already exists")
                return result
            except RuntimeError:
                # Re-raise RuntimeError as-is (already handled)
                raise
            except Exception as e:
                error_msg = str(e).lower()
                # Check if graph already exists - raise exception in this case
                # TigerGraph may return various error messages for existing graphs
                if (
                    "already exists" in error_msg
                    or "duplicate" in error_msg
                    or "graph already exists" in error_msg
                    or "already exist" in error_msg
                ):
                    logger.warning(f"Graph '{name}' already exists: {e}")
                    raise RuntimeError(f"Graph '{name}' already exists") from e
                logger.error(f"Failed to create graph '{name}': {e}")
                raise

        except RuntimeError:
            # Re-raise RuntimeError as-is
            raise
        except Exception as e:
            logger.error(f"Error creating graph '{name}' via GSQL: {e}")
            raise

    def delete_database(self, name: str):
        """
        Delete a TigerGraph database (graph).

        Teardown sequence:
          1) Drop installed queries for the graph
          2) Drop jobs scoped to the graph
          3) DROP GRAPH

        The GSQL endpoint returns HTTP 200 even for logical failures, so we
        inspect the response text for GSQL-level error markers rather than
        relying on a follow-up graph_exists() call (which can produce false
        positives when SHOW GRAPH * is unavailable or slow to propagate).

        Args:
            name: Name of the graph to delete
        """
        logger.debug(f"Attempting to drop graph '{name}'")
        self._conn._drop_installed_queries_for_graph(name)
        self._conn._drop_jobs_for_graph(name)
        result = self._conn._execute_gsql(f"USE GLOBAL\nDROP GRAPH {name}")
        result_str = str(result) if result else ""
        result_lower = result_str.lower()

        # Treat "does not exist" as a success: graph is already gone.
        if (
            "does not exist" in result_lower
            or "doesn't exist" in result_lower
            or "could not be dropped" in result_lower
        ):
            logger.info(
                f"Graph '{name}' did not exist; treating as successful deletion"
            )
            return result

        if gsql_result_has_error(result_str):
            error_msg = f"DROP GRAPH '{name}' failed: {result_str}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.info(f"Successfully dropped graph '{name}'")
        return result

    def _resolve_graph_name(self, schema: Schema) -> str:
        graph_name = self._conn._configured_graph_name()
        if not graph_name:
            graph_name = schema.metadata.name
            self._conn.config.database = graph_name
            self._conn.config.schema_name = graph_name
            logger.info("Using schema name '%s' from schema.metadata.name", graph_name)
        validate_tigergraph_schema_name(graph_name, "graph")
        return graph_name

    def schema_has_artifacts(self, graph_name: str) -> bool:
        """Return True if the graph has any vertex or edge types defined."""
        vertex_names, edge_names = self._conn._get_graph_type_names(graph_name)
        return bool(vertex_names or edge_names)

    def ensure_target_namespace(self, schema: Schema, *, create: bool) -> None:
        """Ensure the TigerGraph graph (namespace) exists."""
        graph_name = self._resolve_graph_name(schema)
        if self._conn.graph_exists(graph_name):
            logger.debug("Graph '%s' already exists", graph_name)
            return
        if not create:
            raise NamespaceNotFoundError(
                f"TigerGraph graph '{graph_name}' does not exist. "
                "Create it manually or call with create_namespace=True."
            )
        logger.debug("Creating empty graph '%s'", graph_name)
        try:
            self._conn.create_database(graph_name)
            logger.info("Successfully created empty graph '%s'", graph_name)
        except Exception as create_error:
            logger.error(
                "Failed to create graph '%s': %s",
                graph_name,
                create_error,
                exc_info=True,
            )
            raise

    def apply_target_schema(
        self,
        schema: Schema,
        *,
        recreate: bool,
        create_namespace: bool = True,
    ) -> None:
        """Define local vertex/edge types and indexes for the current graph."""
        graph_name = self._resolve_graph_name(schema)
        graph_created = False

        try:
            if recreate:
                pre_query_snapshot = self._conn._snapshot_all_queries()
                logger.info(
                    "Pre-recreate installed-query snapshot for graph '%s': %s",
                    graph_name,
                    pre_query_snapshot,
                )
                try:
                    if create_namespace and self._conn.graph_exists(graph_name):
                        self._conn.delete_database(graph_name)
                        surviving_graphs = self._conn._get_all_graph_names()
                        normalized = graph_name.strip().lower()
                        surviving_graphs = [
                            g
                            for g in surviving_graphs
                            if g.strip().lower() != normalized
                        ]
                        logger.debug(
                            "Dropping global schema types for graph '%s' "
                            "(surviving graphs for orphan check: %s)",
                            graph_name,
                            surviving_graphs,
                        )
                        self._conn._drop_global_schema_types(schema, surviving_graphs)
                        logger.debug(
                            "Cleaned up graph '%s' for fresh start", graph_name
                        )
                    elif self._conn.graph_exists(graph_name):
                        surviving_graphs = self._conn._get_all_graph_names()
                        normalized = graph_name.strip().lower()
                        surviving_graphs = [
                            g
                            for g in surviving_graphs
                            if g.strip().lower() != normalized
                        ]
                        self._conn._drop_global_schema_types(schema, surviving_graphs)
                except Exception as clean_error:
                    error_msg = (
                        f"Error during recreate for graph '{graph_name}': {clean_error}"
                    )
                    logger.error(error_msg, exc_info=True)
                    raise RuntimeError(error_msg) from clean_error

                post_query_snapshot = self._conn._snapshot_all_queries()
                normalized_graph = graph_name.strip().lower()
                for other_graph, pre_queries in pre_query_snapshot.items():
                    if other_graph.strip().lower() == normalized_graph:
                        continue
                    post_queries = post_query_snapshot.get(other_graph, [])
                    lost = set(pre_queries) - set(post_queries)
                    if lost:
                        logger.error(
                            "QUERY LOSS DETECTED in graph '%s' after recreating '%s': %s",
                            other_graph,
                            graph_name,
                            sorted(lost),
                        )

            if (
                not recreate
                and self._conn.graph_exists(graph_name)
                and self.schema_has_artifacts(graph_name)
            ):
                raise SchemaExistsError(
                    f"Schema already exists in graph '{graph_name}'. "
                    "Set recreate_schema=True to replace, or use clear_data=True "
                    "before ingestion."
                )

            if not self._conn.graph_exists(graph_name):
                if not create_namespace:
                    raise NamespaceNotFoundError(
                        f"TigerGraph graph '{graph_name}' does not exist. "
                        "Create it manually or call with create_namespace=True."
                    )
                logger.debug("Recreating graph shell '%s' after recreate", graph_name)
                self._conn.create_database(graph_name)
                graph_created = True
                logger.info("Successfully created empty graph '%s'", graph_name)

            logger.info("Defining local schema for graph '%s'", graph_name)
            try:
                self._conn._define_schema_local(schema)
            except Exception as schema_error:
                logger.error(
                    "Failed to define local schema for graph '%s': %s",
                    graph_name,
                    schema_error,
                    exc_info=True,
                )
                raise

            try:
                self.define_indexes(schema)
                logger.info("Index definition completed for graph '%s'", graph_name)
            except Exception as index_error:
                logger.error(
                    "Failed to define indexes for graph '%s': %s",
                    graph_name,
                    index_error,
                    exc_info=True,
                )
                raise
        except Exception:
            if graph_created:
                try:
                    logger.info(
                        "Cleaning up graph '%s' after schema application failure",
                        graph_name,
                    )
                    self._conn.delete_database(graph_name)
                except Exception as cleanup_error:
                    logger.warning(
                        "Failed to clean up graph '%s': %s",
                        graph_name,
                        cleanup_error,
                    )
            raise

    def init_db(
        self,
        schema: Schema,
        recreate_schema: bool = False,
        *,
        create_namespace: bool = True,
    ) -> None:
        """Convenience wrapper: ensure graph namespace then apply schema."""
        self.ensure_target_namespace(schema, create=create_namespace)
        self.apply_target_schema(
            schema, recreate=recreate_schema, create_namespace=create_namespace
        )

    def define_schema(self, schema: Schema):
        """
        Define TigerGraph schema locally for the current graph.

        Assumes graph already exists (created in init_db).
        """
        try:
            self._conn._define_schema_local(schema)
        except Exception as e:
            logger.error(f"Error defining schema: {e}")
            raise

    def define_vertex_classes(self, vertex_config: VertexConfig) -> None:
        """Define TigerGraph vertex types locally for the current graph.

        Args:
            vertex_config: Vertex configuration containing vertices to create
        """
        graph_name = self._conn._require_configured_graph_name()

        schema_change_stmts = []
        db_vertex = (
            VertexConfigDBAware(
                vertex_config, DatabaseProfile(db_flavor=DBType.TIGERGRAPH)
            )
            if not isinstance(vertex_config, VertexConfigDBAware)
            else vertex_config
        )
        for vertex in vertex_config.vertices:
            stmt = self._conn._get_vertex_add_statement(vertex, db_vertex)
            schema_change_stmts.append(stmt)

        if not schema_change_stmts:
            return

        job_name = f"add_vertices_{graph_name}"
        gsql_commands = [
            f"USE GRAPH {graph_name}",
            f"DROP JOB {job_name}",
            f"CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {graph_name} {{",
            "    " + ";\n    ".join(schema_change_stmts) + ";",
            "}",
            f"RUN SCHEMA_CHANGE JOB {job_name}",
        ]

        logger.info(f"Adding vertices locally to graph '{graph_name}'")
        self._conn._execute_gsql("\n".join(gsql_commands))

    def define_edge_classes(self, edges: list[Edge]):
        """Define TigerGraph edge types locally for the current graph.

        Args:
            edges: List of edges to create
        """
        graph_name = self._conn._require_configured_graph_name()

        # Need vertex_config for dbname lookup if finish_init hasn't been called
        # But edges should ideally already be initialized.
        # If not, this might fail or needs a vertex_config.

        schema_change_stmts = []
        for edge in edges:
            stmt = self._conn._get_edge_add_statement(
                edge,
                relation_name=edge.relation or f"{edge.source}_{edge.target}",
                source_vertex=edge.source,
                target_vertex=edge.target,
            )
            schema_change_stmts.append(stmt)

        if not schema_change_stmts:
            return

        job_name = f"add_edges_{graph_name}"
        gsql_commands = [
            f"USE GRAPH {graph_name}",
            f"DROP JOB {job_name}",
            f"CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {graph_name} {{",
            "    " + ";\n    ".join(schema_change_stmts) + ";",
            "}",
            f"RUN SCHEMA_CHANGE JOB {job_name}",
        ]

        logger.info(f"Adding edges locally to graph '{graph_name}'")
        self._conn._execute_gsql("\n".join(gsql_commands))

    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ):
        """
        TigerGraph automatically indexes primary keys.
        Secondary indexes are less common but can be created.
        """
        db_vertex = (
            schema.resolve_db_aware(DBType.TIGERGRAPH).vertex_config
            if schema is not None
            else None
        )
        for vertex_class in vertex_config.vertex_set:
            vertex_dbname = (
                db_vertex.vertex_dbname(vertex_class) if db_vertex else vertex_class
            )
            index_list = (
                schema.db_profile.vertex_secondary_indexes(vertex_class)
                if schema is not None
                else []
            )
            for index_obj in index_list:
                self._conn._add_index(vertex_dbname, index_obj)

    def define_edge_indexes(self, edges: list[Edge], schema: Schema | None = None):
        """Define indexes for edges if specified.

        Note: TigerGraph does not support creating indexes on edge attributes.
        Edge indexes are skipped with a warning. Only vertex indexes are supported.
        """
        for edge in edges:
            index_list = (
                schema.db_profile.edge_secondary_indexes(edge.edge_id)
                if schema is not None
                else []
            )
            if index_list:
                edge_db = (
                    schema.resolve_db_aware(
                        DBType.TIGERGRAPH
                    ).edge_config.relation_dbname(edge)
                    if schema is not None
                    else (edge.relation or f"{edge.source}_{edge.target}")
                )
                logger.info(
                    f"Skipping {len(index_list)} index(es) on edge '{edge_db}': "
                    f"TigerGraph does not support indexes on edge attributes. "
                    f"Only vertex indexes are supported."
                )

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        """
        Create an index on a vertex type using GSQL schema change jobs.

        TigerGraph requires indexes to be created through schema change jobs.
        This implementation creates a local schema change job for the current graph.

        Note: TigerGraph only supports secondary indexes on vertex attributes, not on edge attributes.
        Indexes on edges are not supported and should be skipped.
        TigerGraph only supports indexes on a single field.
        Indexes with multiple fields will be skipped with a warning.

        Args:
            obj_name: Name of the vertex type
            index: Index configuration object
            is_vertex_index: Whether this is a vertex index (True) or edge index (False)
        """
        # TigerGraph does not support indexes on edge attributes
        if not is_vertex_index:
            logger.warning(
                f"Skipping index creation on edge '{obj_name}': "
                f"TigerGraph does not support indexes on edge attributes. "
                f"Only vertex indexes are supported."
            )
            return

        try:
            if not index.fields:
                logger.warning(f"No fields specified for index on {obj_name}, skipping")
                return

            # TigerGraph only supports secondary indexes on a single field
            if len(index.fields) > 1:
                logger.warning(
                    f"TigerGraph only supports indexes on a single field. "
                    f"Skipping multi-field index on {obj_name} with fields {index.fields}"
                )
                return

            # We have exactly one field - proceed with index creation
            field_name = index.fields[0]

            # Generate index name if not provided
            if index.name:
                index_name = index.name
            else:
                # Generate name from obj_name and field name
                index_name = f"{obj_name}_{field_name}_index"

            # Generate job name from obj_name and field name
            job_name = f"add_{obj_name}_{field_name}_index"

            # Build the ALTER command (single field only)
            graph_name = self._conn._configured_graph_name()

            if not graph_name:
                logger.warning(
                    f"No graph name configured, cannot create index on {obj_name}"
                )
                return

            # Build the ALTER statement inside the job
            # Note: For edges, use "EDGE" not "DIRECTED EDGE" in ALTER statements
            obj_type = "VERTEX" if is_vertex_index else "EDGE"
            alter_stmt = (
                f"ALTER {obj_type} {obj_name} ADD INDEX {index_name} ON ({field_name})"
            )

            # Step 1: Drop existing job if it exists (ignore errors)
            try:
                drop_job_cmd = f"USE GRAPH {graph_name}\nDROP JOB {job_name}"
                self._conn._execute_gsql(drop_job_cmd)
                logger.debug(f"Dropped existing job '{job_name}'")
            except Exception as e:
                err_str = str(e).lower()
                # Ignore errors if job doesn't exist
                if "not found" in err_str or "could not be found" in err_str:
                    logger.debug(f"Job '{job_name}' does not exist, skipping drop")
                else:
                    logger.debug(f"Could not drop job '{job_name}': {e}")

            # Step 2: Create the schema change job
            # Use local schema change for the graph
            create_job_cmd = (
                f"USE GRAPH {graph_name}\n"
                f"CREATE SCHEMA_CHANGE job {job_name} FOR GRAPH {graph_name} {{{alter_stmt};}}"
            )

            logger.debug(f"Executing GSQL (create job): {create_job_cmd}")
            try:
                result = self._conn._execute_gsql(create_job_cmd)
                logger.debug(f"Created schema change job '{job_name}': {result}")
            except Exception as e:
                err = str(e).lower()
                # Check if job already exists
                if (
                    "already exists" in err
                    or "duplicate" in err
                    or "used by another object" in err
                ):
                    logger.debug(f"Schema change job '{job_name}' already exists")
                else:
                    logger.error(
                        f"Failed to create schema change job '{job_name}': {e}"
                    )
                    raise

            # Step 2: Run the schema change job
            run_job_cmd = f"RUN SCHEMA_CHANGE job {job_name}"

            logger.debug(f"Executing GSQL (run job): {run_job_cmd}")
            try:
                result = self._conn._execute_gsql(run_job_cmd)
                logger.debug(
                    f"Ran schema change job '{job_name}', created index '{index_name}' on {obj_name}: {result}"
                )
            except Exception as e:
                err = str(e).lower()
                # Check if index already exists or job was already run
                if (
                    "already exists" in err
                    or "duplicate" in err
                    or "used by another object" in err
                    or "already applied" in err
                ):
                    logger.debug(
                        f"Index '{index_name}' on {obj_name} already exists or job already run, skipping"
                    )
                else:
                    logger.error(f"Failed to run schema change job '{job_name}': {e}")
                    raise
        except Exception as e:
            logger.warning(f"Could not create index for {obj_name}: {e}")

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
        *,
        confirm_global_teardown: bool = False,
    ) -> None:
        """
        Delete graph structure (graphs, vertex types, edge types) from TigerGraph.

        In TigerGraph:
        - Graph: Top-level container (functions like a database in ArangoDB)
        - Vertex Types: Global vertex type definitions (can be shared across graphs)
        - Edge Types: Global edge type definitions (can be shared across graphs)
        - Vertex and edge types are associated with graphs

        Teardown order (``delete_all=True``):
        1. Drop targeted graphs (``delete_database`` drops graph-scoped queries and jobs).
        2. Drop global edge types that are not still referenced by any surviving graph.
        3. Drop global vertex types that are not still referenced by any surviving graph.

        Global ``DROP VERTEX`` / ``DROP EDGE`` can silently invalidate installed queries
        in unrelated graphs. ``delete_all=True`` therefore requires
        ``confirm_global_teardown=True``.

        Args:
            vertex_types: Vertex type names to delete (not used in TigerGraph teardown)
            graph_names: Graph names to delete (if empty and delete_all=True, deletes all)
            delete_all: If True, perform full teardown of targeted graphs and orphaned
                global types.
            confirm_global_teardown: Must be True when ``delete_all=True``; otherwise
                this method raises without issuing GSQL.
        """
        cnames = vertex_types
        gnames = graph_names
        if delete_all and not confirm_global_teardown:
            raise ValueError(
                "delete_all=True requires confirm_global_teardown=True. "
                "Global teardown can silently drop installed queries in unrelated "
                "graphs when shared vertex/edge types are removed."
            )
        try:
            if delete_all:
                # Step 1: Drop all graphs
                graphs_to_drop = list(gnames) if gnames else []

                # Guardrail: never auto-discover and drop every graph by default.
                # If no explicit graph target is provided, constrain to current graph.
                if not graphs_to_drop:
                    current_graph = self._conn._configured_graph_name()
                    if current_graph:
                        graphs_to_drop = [current_graph]
                        logger.warning(
                            "delete_all=True without explicit graph_names: limiting TigerGraph teardown to current graph '%s'",
                            current_graph,
                        )
                    else:
                        raise ValueError(
                            "Refusing global TigerGraph teardown without explicit "
                            "graph_names or config.database/config.schema_name"
                        )

                # Drop each graph
                logger.info(
                    f"Found {len(graphs_to_drop)} graphs to drop: {graphs_to_drop}"
                )
                for graph_name in graphs_to_drop:
                    try:
                        self._conn.delete_database(graph_name)
                        logger.info(f"Successfully dropped graph '{graph_name}'")
                    except Exception as e:
                        if is_not_found_error(e):
                            logger.debug(
                                f"Graph '{graph_name}' already dropped or doesn't exist"
                            )
                        else:
                            logger.warning(f"Failed to drop graph '{graph_name}': {e}")
                            logger.warning(
                                f"Error details: {type(e).__name__}: {str(e)}"
                            )

                dropped_set = {g.strip().lower() for g in graphs_to_drop}
                surviving_graphs = [
                    g
                    for g in self._conn._get_all_graph_names()
                    if g.strip().lower() not in dropped_set
                ]
                in_use_vertices: set[str] = set()
                in_use_edges: set[str] = set()
                for g in surviving_graphs:
                    verts, edges = self._conn._get_graph_type_names(g)
                    in_use_vertices |= verts
                    in_use_edges |= edges

                # Step 2: Drop global edge types not still referenced by surviving graphs.
                # Edges before vertices (dependencies).
                try:
                    show_edges_cmd = "SHOW EDGE *"
                    result = self._conn._execute_gsql(show_edges_cmd)
                    result_str = str(result)
                    edge_types = parse_show_edge_output(result_str)

                    logger.info(
                        "Found %s edge types in global catalog: %s",
                        len(edge_types),
                        [name for name, _ in edge_types],
                    )
                    for e_type, _is_directed in edge_types:
                        if e_type in in_use_edges:
                            logger.warning(
                                "Skipping DROP EDGE '%s' — still referenced by surviving graphs",
                                e_type,
                            )
                            continue
                        try:
                            drop_edge_cmd = f"DROP EDGE {e_type}"
                            logger.debug(f"Executing: {drop_edge_cmd}")
                            result = self._conn._execute_gsql(drop_edge_cmd)
                            logger.info(
                                f"Successfully dropped edge type '{e_type}': {result}"
                            )
                        except Exception as e:
                            if is_not_found_error(e):
                                logger.debug(
                                    f"Edge type '{e_type}' already dropped or doesn't exist"
                                )
                            else:
                                logger.warning(
                                    f"Failed to drop edge type '{e_type}': {e}"
                                )
                                logger.warning(
                                    f"Error details: {type(e).__name__}: {str(e)}"
                                )
                except Exception as e:
                    logger.warning(f"Could not list or drop edge types: {e}")
                    logger.warning(f"Error details: {type(e).__name__}: {str(e)}")

                # Step 3: Drop global vertex types not still referenced by surviving graphs.
                try:
                    show_vertices_cmd = "SHOW VERTEX *"
                    result = self._conn._execute_gsql(show_vertices_cmd)
                    result_str = str(result)
                    listed_vertex_types = parse_show_vertex_output(result_str)

                    logger.info(
                        "Found %s vertex types in global catalog: %s",
                        len(listed_vertex_types),
                        listed_vertex_types,
                    )
                    for v_type in listed_vertex_types:
                        if v_type in in_use_vertices:
                            logger.warning(
                                "Skipping DROP VERTEX '%s' — still referenced by "
                                "surviving graphs",
                                v_type,
                            )
                            continue
                        try:
                            try:
                                result = self._conn._delete_vertices(v_type)
                                logger.debug(
                                    f"Cleared data from vertex type '{v_type}': {result}"
                                )
                            except Exception as clear_err:
                                logger.debug(
                                    f"Could not clear data from vertex type '{v_type}': {clear_err}"
                                )

                            drop_vertex_cmd = f"DROP VERTEX {v_type}"
                            logger.debug(f"Executing: {drop_vertex_cmd}")
                            result = self._conn._execute_gsql(drop_vertex_cmd)
                            logger.info(
                                f"Successfully dropped vertex type '{v_type}': {result}"
                            )
                        except Exception as e:
                            if is_not_found_error(e):
                                logger.debug(
                                    f"Vertex type '{v_type}' already dropped or doesn't exist"
                                )
                            else:
                                logger.warning(
                                    f"Failed to drop vertex type '{v_type}': {e}"
                                )
                                logger.warning(
                                    f"Error details: {type(e).__name__}: {str(e)}"
                                )
                except Exception as e:
                    logger.warning(f"Could not list or drop vertex types: {e}")
                    logger.warning(f"Error details: {type(e).__name__}: {str(e)}")

            elif gnames:
                # Drop specific graphs
                for graph_name in gnames:
                    try:
                        self._conn.delete_database(graph_name)
                    except Exception as e:
                        logger.error(f"Error deleting graph '{graph_name}': {e}")
            elif cnames:
                # Delete vertices from specific vertex types (data only, not schema)
                with self._conn._ensure_graph_context():
                    for class_name in cnames:
                        try:
                            result = self._conn._delete_vertices(class_name)
                            logger.debug(
                                f"Deleted vertices from {class_name}: {result}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error deleting vertices from {class_name}: {e}"
                            )

        except Exception as e:
            logger.error(f"Error in delete_graph_structure: {e}")

    def clear_data(self, schema: Schema) -> None:
        """Remove all data from the graph without dropping the schema.

        Deletes vertices (and their edges) for all vertex types in the schema.
        """
        vc = schema.resolve_db_aware(DBType.TIGERGRAPH).vertex_config
        graph_name = self._conn._configured_graph_name() or schema.metadata.name
        vertex_types = tuple(vc.vertex_dbname(v) for v in vc.vertex_set)
        if not vertex_types:
            return

        try:
            self._conn._clear_data_via_installed_query(
                graph_name=graph_name, vertex_types=vertex_types
            )
            remaining = [
                vertex_type
                for vertex_type in vertex_types
                if len(self._conn.fetch_docs(vertex_type, limit=1)) > 0
            ]
            if remaining:
                raise RuntimeError(
                    "Installed clear_data query completed but left data in: "
                    + ", ".join(remaining)
                )
            logger.info(
                "Cleared data via installed query for graph '%s' (%d vertex types)",
                graph_name,
                len(vertex_types),
            )
            return
        except Exception as query_error:
            logger.info(
                "Installed clear-data query path failed for graph '%s': %s. "
                "Falling back to GSQL vertex deletion.",
                graph_name,
                query_error,
            )

        gsql_failures: list[str] = []
        for vertex_type in vertex_types:
            try:
                result = self._conn._execute_gsql(
                    f"USE GRAPH {graph_name}\nDELETE FROM {vertex_type}"
                )
                if isinstance(result, dict) and result.get("error") is True:
                    raise RuntimeError(str(result))
                result_text = str(result).lower()
                if '"error": true' in result_text or "failed" in result_text:
                    raise RuntimeError(str(result))
                logger.debug(
                    "Deleted vertices via GSQL from %s in graph %s: %s",
                    vertex_type,
                    graph_name,
                    result,
                )
            except Exception as gsql_error:
                gsql_failures.append(f"{vertex_type}: {gsql_error}")

        if not gsql_failures:
            logger.info(
                "Cleared data via direct GSQL deletion for graph '%s' (%d vertex types)",
                graph_name,
                len(vertex_types),
            )
            return

        logger.warning(
            "Direct GSQL delete path failed for graph '%s': %s. "
            "Falling back to REST vertex deletion.",
            graph_name,
            "; ".join(gsql_failures),
        )

        failures: list[str] = []
        for vertex_type in vertex_types:
            try:
                result = self._conn._delete_vertices(
                    vertex_type=vertex_type,
                    graph_name=graph_name,
                )
                if isinstance(result, dict) and result.get("error") is True:
                    raise RuntimeError(
                        result.get("message", "Unknown TigerGraph error")
                    )
                logger.debug(
                    "Deleted vertices from %s in graph %s: %s",
                    vertex_type,
                    graph_name,
                    result,
                )
            except Exception as e:
                logger.error(
                    "Error deleting vertices from %s in graph %s: %s",
                    vertex_type,
                    graph_name,
                    e,
                )
                failures.append(f"{vertex_type}: {e}")

        if failures:
            raise RuntimeError(
                "TigerGraph clear_data failed for vertex types: " + "; ".join(failures)
            )

    def define_indexes(self, schema: Schema):
        """Define all indexes from schema."""
        try:
            self._conn.define_vertex_indexes(
                schema.core_schema.vertex_config, schema=schema
            )
            edges_for_indexes = list(schema.core_schema.edge_config.values())
            self._conn.define_edge_indexes(edges_for_indexes, schema=schema)
        except Exception as e:
            logger.error(f"Error defining indexes: {e}")

    def fetch_indexes(self, vertex_type: str | None = None):
        """
        Fetch indexes for vertex types using GSQL.

        In TigerGraph, indexes are associated with vertex types.
        Use DESCRIBE VERTEX to get index information.

        Args:
            vertex_type: Optional vertex type name to fetch indexes for.
                        If None, fetches indexes for all vertex types.

        Returns:
            dict: Mapping of vertex type names to their indexes.
                  Format: {vertex_type: [{"name": "index_name", "fields": ["field1", ...]}, ...]}
        """
        try:
            with self._conn._ensure_graph_context():
                result = {}

                if vertex_type:
                    vertex_types = [vertex_type]
                else:
                    vertex_types = self._conn._get_vertex_types()

                for v_type in vertex_types:
                    try:
                        # Parse indexes from the describe output
                        indexes = []
                        try:
                            indexes.append(
                                {"name": "stat_index", "source": "show_stat"}
                            )
                        except Exception:
                            # If SHOW STAT INDEX doesn't work, try alternative methods
                            pass

                        result[v_type] = indexes
                    except Exception as e:
                        logger.debug(
                            f"Could not fetch indexes for vertex type {v_type}: {e}"
                        )
                        result[v_type] = []

                return result
        except Exception as e:
            logger.error(f"Error fetching indexes: {e}")
            return {}

    def _get_all_graph_names(self) -> list[str]:
        """Return all graph names currently in TigerGraph (SHOW GRAPH * + parser)."""
        try:
            result = self._conn._execute_gsql("USE GLOBAL\nSHOW GRAPH *")
            return parse_show_graph_output(str(result))
        except Exception as e:
            logger.warning(f"Could not list graphs for orphan check: {e}")
            return []

    def _get_graph_type_names(self, graph_name: str) -> tuple[set[str], set[str]]:
        """Return ``(vertex_names, edge_names)`` visible in *graph_name* context."""
        vertex_names: set[str] = set()
        edge_names: set[str] = set()
        try:
            r = self._conn._execute_gsql(f"USE GRAPH {graph_name}\nSHOW VERTEX *")
            vertex_names = set(parse_show_vertex_output(str(r)))
        except Exception as e:
            logger.debug(f"Could not list vertices for graph '{graph_name}': {e}")
        try:
            r = self._conn._execute_gsql(f"USE GRAPH {graph_name}\nSHOW EDGE *")
            edge_names = {name for name, _ in parse_show_edge_output(str(r))}
        except Exception as e:
            logger.debug(f"Could not list edges for graph '{graph_name}': {e}")
        return vertex_names, edge_names

    def _snapshot_all_queries(self) -> dict[str, list[str]]:
        """Return ``{graph_name: [installed_query_names]}`` for every graph.

        Used before/after destructive operations to detect accidental query loss
        in graphs that were not the direct target of a recreate.
        """
        snapshot: dict[str, list[str]] = {}
        for graph_name in self._conn._get_all_graph_names():
            # Get installed queries via GSQL; returns None if discovery failed (e.g. auth/permission error)
            queries = self._conn._gsql._get_installed_queries_via_gsql(graph_name)
            if queries is not None:
                snapshot[graph_name] = queries
            else:
                logger.debug(
                    f"Skipping query snapshot for graph '{graph_name}' due to discovery failure"
                )
        return snapshot
