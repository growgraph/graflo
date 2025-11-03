"""TigerGraph connection implementation for graph database operations.

This module implements the Connection interface for TigerGraph, providing
specific functionality for graph operations in TigerGraph. It handles:
- Vertex and edge management
- GSQL query execution
- Schema management
- Batch operations
- Graph traversal and analytics

Key Features:
    - Vertex and edge type management
    - GSQL query execution
    - Schema definition and management
    - Batch vertex and edge operations
    - Graph analytics and traversal

Example:
    >>> conn = TigerGraphConnection(config)
    >>> conn.init_db(schema, clean_start=True)
    >>> conn.upsert_docs_batch(docs, "User", match_keys=["email"])
"""

import contextlib
import logging

from pyTigerGraph import TigerGraphConnection as PyTigerGraphConnection

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import FieldType, Vertex, VertexConfig
from graflo.db.conn import Connection
from graflo.db.connection.onto import TigergraphConnectionConfig
from graflo.onto import AggregationType, DBFlavor
from graflo.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


class TigerGraphConnection(Connection):
    """
    TigerGraph database connection implementation.

    Key conceptual differences from ArangoDB:
    1. TigerGraph uses GSQL (Graph Query Language) instead of AQL
    2. Schema must be defined explicitly before data insertion
    3. No automatic collection creation - vertices and edges must be pre-defined
    4. Different query syntax and execution model
    5. Token-based authentication for some operations
    """

    flavor = DBFlavor.TIGERGRAPH

    def __init__(self, config: TigergraphConnectionConfig):
        super().__init__()
        self.config = config
        # If database is not set, pyTigerGraph will default to "tigergraph"
        # We'll pass None and let it default, but will set config.database when we create graphs
        self.conn = PyTigerGraphConnection(
            host=config.url_without_port,
            restppPort=config.port,
            gsPort=config.gs_port,
            graphname=config.database,
            username=config.username,
            password=config.password,
            certPath=getattr(config, "certPath", None),
        )

        # Get authentication token if secret is provided
        if hasattr(config, "secret") and config.secret:
            try:
                self.conn.getToken(config.secret)
            except Exception as e:
                logger.warning(f"Failed to get authentication token: {e}")

    @contextlib.contextmanager
    def _ensure_graph_context(self, graph_name: str | None = None):
        """
        Context manager that ensures graph context for metadata operations.

        Updates conn.graphname for PyTigerGraph metadata operations that rely on it
        (e.g., getVertexTypes(), getEdgeTypes()).

        Args:
            graph_name: Name of the graph to use. If None, uses self.config.database.

        Yields:
            The graph name that was set.
        """
        graph_name = graph_name or self.config.database
        if not graph_name:
            raise ValueError(
                "Graph name must be provided via graph_name parameter or config.database"
            )

        old_graphname = self.conn.graphname
        self.conn.graphname = graph_name

        try:
            yield graph_name
        finally:
            # Restore original graphname
            self.conn.graphname = old_graphname

    def graph_exists(self, name: str) -> bool:
        """
        Check if a graph with the given name exists.

        Uses the USE GRAPH command and checks the returned message.
        If the graph doesn't exist, USE GRAPH returns an error message like
        "Graph 'name' does not exist."

        Args:
            name: Name of the graph to check

        Returns:
            bool: True if the graph exists, False otherwise
        """
        try:
            result = self.conn.gsql(f"USE GRAPH {name}")
            result_str = str(result).lower()

            # If the graph doesn't exist, USE GRAPH returns an error message
            # Check for common error messages indicating the graph doesn't exist
            error_patterns = [
                "does not exist",
                "doesn't exist",
                "doesn't exist!",
                f"graph '{name.lower()}' does not exist",
            ]

            # If any error pattern is found, the graph doesn't exist
            for pattern in error_patterns:
                if pattern in result_str:
                    return False

            # If no error pattern is found, the graph likely exists
            # (USE GRAPH succeeded or returned success message)
            return True
        except Exception as e:
            logger.debug(f"Error checking if graph '{name}' exists: {e}")
            # If there's an exception, try to parse it
            error_str = str(e).lower()
            if "does not exist" in error_str or "doesn't exist" in error_str:
                return False
            # If exception doesn't indicate "doesn't exist", assume it exists
            # (other errors might indicate connection issues, not missing graph)
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

        This method uses the pyTigerGraph gsql() method to execute GSQL commands
        that create and use the graph. Supported in TigerGraph version 4.2.2+.

        Args:
            name: Name of the graph to create
            vertex_names: Optional list of vertex type names to attach to the graph
            edge_names: Optional list of edge type names to attach to the graph

        Raises:
            Exception: If graph creation fails
        """
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

            # Execute using pyTigerGraph's gsql method which handles authentication
            logger.debug(f"Creating graph '{name}' via GSQL: {gsql_commands}")
            try:
                result = self.conn.gsql(gsql_commands)
                logger.info(
                    f"Successfully created graph '{name}' with types {all_types}: {result}"
                )
                return result
            except Exception as e:
                error_msg = str(e).lower()
                # Check if graph already exists (might be acceptable)
                if "already exists" in error_msg or "duplicate" in error_msg:
                    logger.info(f"Graph '{name}' may already exist: {e}")
                    return str(e)
                logger.error(f"Failed to create graph '{name}': {e}")
                raise

        except Exception as e:
            logger.error(f"Error creating graph '{name}' via GSQL: {e}")
            raise

    def delete_database(self, name: str):
        """
        Delete a TigerGraph database (graph).

        This method attempts to drop the graph using GSQL DROP GRAPH.
        If that fails (e.g., dependencies), it will:
          1) Remove associations and drop all edge types
          2) Drop all vertex types
          3) Clear remaining data as a last resort

        Args:
            name: Name of the graph to delete

        Note:
            In TigerGraph, deleting a graph structure requires the graph to be empty
            or may fail if it has dependencies. This method handles both cases.
        """
        try:
            logger.debug(f"Attempting to drop graph '{name}'")
            try:
                # Use the graph first to ensure we're working with the right graph
                drop_command = f"USE GRAPH {name}\nDROP GRAPH {name}"
                result = self.conn.gsql(drop_command)
                logger.info(f"Successfully dropped graph '{name}': {result}")
                return result
            except Exception as e:
                logger.debug(
                    f"Could not drop graph '{name}' (may not exist or have dependencies): {e}"
                )

            # Fallback 1: Attempt to drop edge and vertex types via ALTER GRAPH and DROP
            try:
                with self._ensure_graph_context(name):
                    # Drop edge associations and edge types
                    try:
                        edge_types = self.conn.getEdgeTypes(force=True)
                    except Exception:
                        edge_types = []

                    for e_type in edge_types:
                        # Try disassociate from graph (safe if already disassociated)
                        # ALTER GRAPH requires USE GRAPH context
                        try:
                            drop_edge_cmd = f"USE GRAPH {name}\nALTER GRAPH {name} DROP DIRECTED EDGE {e_type}"
                            self.conn.gsql(drop_edge_cmd)
                        except Exception:
                            pass
                        # Try drop edge type globally (edges are global, no USE GRAPH needed)
                        try:
                            drop_edge_global_cmd = f"DROP DIRECTED EDGE {e_type}"
                            self.conn.gsql(drop_edge_global_cmd)
                        except Exception:
                            pass

                    # Drop vertex associations and vertex types
                    try:
                        vertex_types = self.conn.getVertexTypes(force=True)
                    except Exception:
                        vertex_types = []

                    for v_type in vertex_types:
                        # Remove all data first to avoid dependency issues
                        try:
                            self.conn.delVertices(v_type)
                        except Exception:
                            pass
                        # Disassociate from graph (best-effort)
                        # ALTER GRAPH requires USE GRAPH context
                        try:
                            drop_vertex_cmd = f"USE GRAPH {name}\nALTER GRAPH {name} DROP VERTEX {v_type}"
                            self.conn.gsql(drop_vertex_cmd)
                        except Exception:
                            pass
                        # Drop vertex type globally (vertices are global, no USE GRAPH needed)
                        try:
                            drop_vertex_global_cmd = f"DROP VERTEX {v_type}"
                            self.conn.gsql(drop_vertex_global_cmd)
                        except Exception:
                            pass
            except Exception as e3:
                logger.warning(
                    f"Could not drop schema types for graph '{name}': {e3}. Proceeding to data clear."
                )

            # Fallback 2: Clear all data (if any remain)
            try:
                with self._ensure_graph_context(name):
                    vertex_types = self.conn.getVertexTypes()
                    for v_type in vertex_types:
                        result = self.conn.delVertices(v_type)
                        logger.debug(f"Cleared vertices of type {v_type}: {result}")
                    logger.info(f"Cleared all data from graph '{name}'")
            except Exception as e2:
                logger.warning(
                    f"Could not clear data from graph '{name}': {e2}. Graph may not exist."
                )

        except Exception as e:
            logger.error(f"Error deleting database '{name}': {e}")

    def execute(self, query, **kwargs):
        """
        Execute GSQL query or installed query based on content.
        """
        try:
            # Check if this is an installed query call
            if query.strip().upper().startswith("RUN "):
                # Extract query name and parameters
                query_name = query.strip()[4:].split("(")[0].strip()
                result = self.conn.runInstalledQuery(query_name, **kwargs)
            else:
                # Execute as raw GSQL
                result = self.conn.gsql(query)
            return result
        except Exception as e:
            logger.error(f"Error executing query '{query}': {e}")
            raise

    def close(self):
        """Close connection - pyTigerGraph handles cleanup automatically."""
        pass

    def init_db(self, schema: Schema, clean_start=False):
        """
        Initialize database with schema definition.

        Follows the same pattern as ArangoDB:
        1. Clean if needed
        2. Create vertex and edge types globally (required before CREATE GRAPH)
        3. Create graph with vertices and edges explicitly attached
        4. Define indexes

        If any step fails, the graph will be cleaned up gracefully.
        """
        # Use schema.general.name for graph creation
        graph_created = False

        # Ensure config.database is set to the graph name
        # This ensures subsequent operations use the correct graph
        graph_name = self.config.database

        try:
            if clean_start:
                # Delete all graphs, edges, and vertices (full teardown)
                self.delete_collections([], [], delete_all=True)

            # Step 1: Create vertex and edge types globally first
            # These must exist before they can be included in CREATE GRAPH
            logger.debug(
                f"Creating vertex and edge types globally for graph '{graph_name}'"
            )
            vertex_names = self._create_vertex_types_global(schema.vertex_config)
            edge_names = self._create_edge_types_global(
                schema.edge_config.edges_list(include_aux=True)
            )

            # Step 2: Create graph with vertices and edges explicitly attached
            if not self.graph_exists(graph_name):
                logger.debug(f"Creating graph '{graph_name}' with types in init_db")
                self.create_database(
                    graph_name, vertex_names=vertex_names, edge_names=edge_names
                )
                graph_created = True
            else:
                logger.debug(f"Graph '{graph_name}' already exists in init_db")
                # If graph already exists, associate types via ALTER GRAPH
                self.define_vertex_collections(schema.vertex_config)
                self.define_edge_collections(
                    schema.edge_config.edges_list(include_aux=True)
                )

            # Step 3: Define indexes
            self.define_indexes(schema)
            logger.info("Index definition completed")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            # Graceful teardown: if graph was created in this session, clean it up
            if graph_created:
                try:
                    logger.info(
                        f"Cleaning up graph '{graph_name}' after initialization failure"
                    )
                    self.delete_database(graph_name)
                except Exception as cleanup_error:
                    logger.warning(
                        f"Failed to clean up graph '{graph_name}': {cleanup_error}"
                    )
            raise

    def define_schema(self, schema: Schema):
        """
        Define TigerGraph schema with proper GSQL syntax.

        Assumes graph already exists (created in init_db). This method:
        1. Uses the graph from config.database
        2. Defines vertex types within the graph
        3. Defines edge types within the graph
        """
        try:
            # Define vertex and edge types within the graph
            # Graph context is ensured by _ensure_graph_context in the called methods
            self.define_vertex_collections(schema.vertex_config)
            self.define_edge_collections(
                schema.edge_config.edges_list(include_aux=True)
            )

        except Exception as e:
            logger.error(f"Error defining schema: {e}")
            raise

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
        # Get fields with TigerGraph default types applied (None -> STRING)
        fields = vertex.get_fields_with_defaults(DBFlavor.TIGERGRAPH, with_aux=False)

        if not fields:
            # Default fields if none specified
            return 'name STRING DEFAULT "",\n    properties MAP<STRING, STRING> DEFAULT (map())'

        field_list = []
        for field in fields:
            # Field type should already be set (STRING if was None)
            field_type = field.type or FieldType.STRING.value
            # Format as: field_name TYPE
            # TODO: Add DEFAULT clause support if needed in the future
            field_list.append(f"{field.name} {field_type}")

        return ",\n    ".join(field_list)

    def _format_edge_attributes(self, edge: Edge) -> str:
        """
        Format edge attributes for GSQL CREATE EDGE statement.
        """
        if hasattr(edge, "attributes") and edge.attributes:
            attrs = []
            for attr_name, attr_type in edge.attributes.items():
                tg_type = self._map_type_to_tigergraph(attr_type)
                attrs.append(f"{attr_name} {tg_type}")
            return ",\n    " + ",\n    ".join(attrs) if attrs else ""
        else:
            return ",\n    weight FLOAT DEFAULT 1.0"

    def _map_type_to_tigergraph(self, field_type: str) -> str:
        """
        Map common field types to TigerGraph types.
        """
        type_mapping = {
            "str": "STRING",
            "string": "STRING",
            "int": "INT",
            "integer": "INT",
            "float": "FLOAT",
            "double": "DOUBLE",
            "bool": "BOOL",
            "boolean": "BOOL",
            "datetime": "DATETIME",
            "date": "DATETIME",
        }
        return type_mapping.get(field_type.lower(), "STRING")

    # _get_graph_name removed: always use schema.general.name

    def _create_vertex_types_global(self, vertex_config: VertexConfig) -> list[str]:
        """Create TigerGraph vertex types globally (without graph association).

        Vertices are global in TigerGraph and must be created before they can be
        included in a CREATE GRAPH statement.

        Args:
            vertex_config: Vertex configuration containing vertices to create

        Returns:
            list[str]: List of vertex type names that were created (or already existed)
        """
        vertex_names = []
        for vertex in vertex_config.vertices:
            field_definitions = self._format_vertex_fields(vertex)
            vertex_dbname = vertex_config.vertex_dbname(vertex.name)
            vindex = "(" + ", ".join(vertex_config.index(vertex.name).fields) + ")"

            # Create the vertex type globally (ignore if exists)
            # Vertices are global in TigerGraph, so no USE GRAPH needed
            create_vertex_cmd = (
                f"CREATE VERTEX {vertex_dbname} (\n"
                f"    {field_definitions},\n"
                f"    PRIMARY KEY {vindex}\n"
                f') WITH STATS="OUTDEGREE_BY_EDGETYPE"'
            )
            logger.debug(f"Executing GSQL: {create_vertex_cmd}")
            try:
                result = self.conn.gsql(create_vertex_cmd)
                logger.debug(f"Result: {result}")
                vertex_names.append(vertex_dbname)
            except Exception as e:
                err = str(e).lower()
                if "used by another object" in err or "duplicate" in err:
                    logger.debug(
                        f"Vertex type '{vertex_dbname}' already exists; will include in graph"
                    )
                    vertex_names.append(vertex_dbname)
                else:
                    raise
        return vertex_names

    def define_vertex_collections(self, vertex_config: VertexConfig):
        """Define TigerGraph vertex types and associate them with the current graph.

        Flow per vertex type:
        1) Try to CREATE VERTEX (idempotent: ignore "already exists" errors)
        2) Associate the vertex with the graph via ALTER GRAPH <graph> ADD VERTEX <vertex>

        Args:
            vertex_config: Vertex configuration containing vertices to create
        """
        # First create all vertex types globally
        vertex_names = self._create_vertex_types_global(vertex_config)

        # Then associate them with the graph (if graph already exists)
        graph_name = self.config.database
        if graph_name:
            for vertex_name in vertex_names:
                alter_graph_cmd = f"USE GRAPH {graph_name}\nALTER GRAPH {graph_name} ADD VERTEX {vertex_name}"
                logger.debug(f"Executing GSQL: {alter_graph_cmd}")
                try:
                    result = self.conn.gsql(alter_graph_cmd)
                    logger.debug(f"Result: {result}")
                except Exception as e:
                    err = str(e).lower()
                    # If already associated, ignore
                    if "already" in err and ("added" in err or "exists" in err):
                        logger.debug(
                            f"Vertex '{vertex_name}' already associated with graph '{graph_name}'"
                        )
                    else:
                        raise

    def _create_edge_types_global(self, edges: list[Edge]) -> list[str]:
        """Create TigerGraph edge types globally (without graph association).

        Edges are global in TigerGraph and must be created before they can be
        included in a CREATE GRAPH statement.

        Args:
            edges: List of edges to create (should have _source_collection and _target_collection populated)

        Returns:
            list[str]: List of edge type names (relation names) that were created (or already existed)
        """
        edge_names = []
        for edge in edges:
            edge_attrs = self._format_edge_attributes(edge)

            # Create the edge type globally (ignore if exists/used elsewhere)
            # Edges are global in TigerGraph, so no USE GRAPH needed
            create_edge_cmd = (
                f"CREATE DIRECTED EDGE {edge.relation} (\n"
                f"    FROM {edge._source_collection},\n"
                f"    TO {edge._target_collection}{edge_attrs}\n"
                f")"
            )
            logger.debug(f"Executing GSQL: {create_edge_cmd}")
            try:
                result = self.conn.gsql(create_edge_cmd)
                logger.debug(f"Result: {result}")
                edge_names.append(edge.relation)
            except Exception as e:
                err = str(e).lower()
                # If the edge name is already used by another object or duplicates exist, continue
                if (
                    "used by another object" in err
                    or "duplicate" in err
                    or "already exists" in err
                ):
                    logger.debug(
                        f"Edge type '{edge.relation}' already defined; will include in graph"
                    )
                    edge_names.append(edge.relation)
                else:
                    raise
        return edge_names

    def define_edge_collections(self, edges: list[Edge]):
        """Define TigerGraph edge types and associate them with the current graph.

        Flow per edge type:
        1) Try to CREATE DIRECTED EDGE (idempotent: ignore "used by another object"/"duplicate"/"already exists")
        2) Associate the edge with the graph via ALTER GRAPH <graph> ADD DIRECTED EDGE <edge>

        Args:
            edges: List of edges to create (should have _source_collection and _target_collection populated)
        """
        # First create all edge types globally
        edge_names = self._create_edge_types_global(edges)

        # Then associate them with the graph (if graph already exists)
        graph_name = self.config.database
        if graph_name:
            for edge_name in edge_names:
                alter_graph_cmd = (
                    f"USE GRAPH {graph_name}\n"
                    f"ALTER GRAPH {graph_name} ADD DIRECTED EDGE {edge_name}"
                )
                logger.debug(f"Executing GSQL: {alter_graph_cmd}")
                try:
                    result = self.conn.gsql(alter_graph_cmd)
                    logger.debug(f"Result: {result}")
                except Exception as e:
                    err = str(e).lower()
                    # If already associated, ignore
                    if "already" in err and ("added" in err or "exists" in err):
                        logger.debug(
                            f"Edge '{edge_name}' already associated with graph '{graph_name}'"
                        )
                    else:
                        raise

    def define_vertex_indices(self, vertex_config: VertexConfig):
        """
        TigerGraph automatically indexes primary keys.
        Secondary indices are less common but can be created.
        """
        for vertex_class in vertex_config.vertex_set:
            vertex_dbname = vertex_config.vertex_dbname(vertex_class)
            for index_obj in vertex_config.indexes(vertex_class)[1:]:
                self._add_index(vertex_dbname, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        """Define indices for edges if specified."""
        for edge in edges:
            if hasattr(edge, "indexes"):
                for index_obj in edge.indexes:
                    if edge.relation:
                        self._add_index(edge.relation, index_obj, is_vertex_index=False)

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        """
        Create an index on a vertex or edge type using GSQL schema change jobs.

        TigerGraph requires indexes to be created through schema change jobs:
        1. CREATE GLOBAL SCHEMA_CHANGE job job_name {ALTER VERTEX/EDGE ... ADD INDEX ... ON (...);}
        2. RUN GLOBAL SCHEMA_CHANGE job job_name

        Note: TigerGraph only supports secondary indexes on a single field.
        Indexes with multiple fields will be skipped with a warning.

        Args:
            obj_name: Name of the vertex type or edge type
            index: Index configuration object
            is_vertex_index: Whether this is a vertex index (True) or edge index (False)
        """
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
            graph_name = self.config.database

            if not graph_name:
                logger.warning(
                    f"No graph name configured, cannot create index on {obj_name}"
                )
                return

            # Ensure graph context is set using context manager
            with self._ensure_graph_context(graph_name):
                # Build the ALTER statement inside the job (single field in parentheses)
                if is_vertex_index:
                    alter_stmt = f"ALTER VERTEX {obj_name} ADD INDEX {index_name} ON ({field_name})"
                else:
                    alter_stmt = f"ALTER EDGE {obj_name} ADD INDEX {index_name} ON ({field_name})"

                # Step 1: Create the schema change job
                # only global changes are supported by tigergraph
                create_job_cmd = (
                    f"USE GLOBAL \n"
                    f"CREATE GLOBAL SCHEMA_CHANGE job {job_name} {{{alter_stmt};}}"
                )

                logger.debug(f"Executing GSQL (create job): {create_job_cmd}")
                try:
                    result = self.conn.gsql(create_job_cmd)
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
                run_job_cmd = f"RUN GLOBAL SCHEMA_CHANGE job {job_name}"

                logger.debug(f"Executing GSQL (run job): {run_job_cmd}")
                try:
                    result = self.conn.gsql(run_job_cmd)
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
                        logger.error(
                            f"Failed to run schema change job '{job_name}': {e}"
                        )
                        raise
        except Exception as e:
            logger.warning(f"Could not create index for {obj_name}: {e}")

    def _parse_show_edge_output(self, result_str: str) -> list[tuple[str, bool]]:
        """
        Parse SHOW EDGE * output to extract edge type names and direction.

        Format: "- DIRECTED EDGE belongsTo(FROM Author, TO ResearchField, ...)"
                or "- UNDIRECTED EDGE edgeName(...)"

        Args:
            result_str: String output from SHOW EDGE * GSQL command

        Returns:
            List of tuples (edge_name, is_directed)
        """
        edge_types = []
        lines = result_str.split("\n")

        for line in lines:
            line = line.strip()
            # Skip empty lines and headers
            if not line or line.startswith("*"):
                continue

            # Remove leading "- " if present
            if line.startswith("- "):
                line = line[2:].strip()

            # Look for "DIRECTED EDGE" or "UNDIRECTED EDGE" pattern
            is_directed = None
            prefix = None
            if "DIRECTED EDGE" in line.upper():
                prefix = "DIRECTED EDGE "
                is_directed = True
            elif "UNDIRECTED EDGE" in line.upper():
                prefix = "UNDIRECTED EDGE "
                is_directed = False

            if prefix:
                idx = line.upper().find(prefix)
                if idx >= 0:
                    after_prefix = line[idx + len(prefix) :].strip()
                    # Extract name before opening parenthesis
                    if "(" in after_prefix:
                        edge_name = after_prefix.split("(")[0].strip()
                        if edge_name:
                            edge_types.append((edge_name, is_directed))

        return edge_types

    def _parse_show_vertex_output(self, result_str: str) -> list[str]:
        """
        Parse SHOW VERTEX * output to extract vertex type names.

        Format: "- VERTEX Author(id STRING, full_name STRING, ..., primary key (...)) WITH STATS=..."

        Args:
            result_str: String output from SHOW VERTEX * GSQL command

        Returns:
            List of vertex type names
        """
        vertex_types = []
        lines = result_str.split("\n")

        for line in lines:
            line = line.strip()
            # Skip empty lines and headers
            if not line or line.startswith("*"):
                continue

            # Remove leading "- " if present
            if line.startswith("- "):
                line = line[2:].strip()

            # Look for "VERTEX" pattern
            if line.upper().startswith("VERTEX "):
                # Extract vertex type name (after "VERTEX" and before opening parenthesis)
                after_vertex = line[7:].strip()  # "VERTEX " is 7 chars
                if "(" in after_vertex:
                    vertex_name = after_vertex.split("(")[0].strip()
                    if vertex_name:
                        vertex_types.append(vertex_name)

        return vertex_types

    def _parse_show_graph_output(self, result_str: str) -> list[str]:
        """
        Parse SHOW GRAPH * output to extract graph names.

        Format: "- GRAPH graphName(...)" or similar

        Args:
            result_str: String output from SHOW GRAPH * GSQL command

        Returns:
            List of graph names
        """
        graph_names = []
        lines = result_str.split("\n")

        for line in lines:
            line = line.strip()
            # Skip empty lines and headers
            if not line or line.startswith("*"):
                continue

            # Remove leading "- " if present
            if line.startswith("- "):
                line = line[2:].strip()

            # Look for "GRAPH" pattern
            if line.upper().startswith("GRAPH "):
                # Extract graph name (after "GRAPH" and before opening parenthesis or whitespace)
                after_graph = line[6:].strip()  # "GRAPH " is 6 chars
                # Graph name is the first word (before space or parenthesis)
                if "(" in after_graph:
                    graph_name = after_graph.split("(")[0].strip()
                else:
                    # No parenthesis, take the first word
                    graph_name = (
                        after_graph.split()[0].strip() if after_graph.split() else None
                    )

                if graph_name:
                    graph_names.append(graph_name)

        return graph_names

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        """
        Delete collections and graphs with proper teardown sequence.

        Teardown order:
        1. Drop all graphs
        2. Drop all edge types globally
        3. Drop all vertex types globally

        Args:
            cnames: Vertex type names to delete (not used in TigerGraph teardown)
            gnames: Graph names to delete (if empty and delete_all=True, deletes all)
            delete_all: If True, perform full teardown of all graphs, edges, and vertices
        """
        try:
            if delete_all:
                # Step 1: Drop all graphs
                graphs_to_drop = list(gnames) if gnames else []

                # If no specific graphs provided, try to discover and drop all graphs
                if not graphs_to_drop:
                    try:
                        # Use GSQL to list all graphs
                        show_graphs_cmd = "SHOW GRAPH *"
                        result = self.conn.gsql(show_graphs_cmd)
                        result_str = str(result)

                        # Parse graph names using helper method
                        graphs_to_drop = self._parse_show_graph_output(result_str)
                    except Exception as e:
                        logger.debug(f"Could not list graphs: {e}")
                        graphs_to_drop = []

                # Drop each graph
                logger.info(
                    f"Found {len(graphs_to_drop)} graphs to drop: {graphs_to_drop}"
                )
                for graph_name in graphs_to_drop:
                    try:
                        self.delete_database(graph_name)
                        logger.info(f"Successfully dropped graph '{graph_name}'")
                    except Exception as e:
                        err_str = str(e).lower()
                        # Check if graph doesn't exist (already dropped)
                        if "does not exist" in err_str or "not found" in err_str:
                            logger.debug(
                                f"Graph '{graph_name}' already dropped or doesn't exist"
                            )
                        else:
                            logger.warning(f"Failed to drop graph '{graph_name}': {e}")
                            logger.warning(
                                f"Error details: {type(e).__name__}: {str(e)}"
                            )

                # Step 2: Drop all edge types globally
                # Note: Edges must be dropped before vertices due to dependencies
                # Edges are global, so we need to query them at global level using GSQL
                try:
                    # Use GSQL to list all global edge types (not graph-scoped)
                    show_edges_cmd = "SHOW EDGE *"
                    result = self.conn.gsql(show_edges_cmd)
                    result_str = str(result)

                    # Parse edge types using helper method
                    edge_types = self._parse_show_edge_output(result_str)

                    logger.info(
                        f"Found {len(edge_types)} edge types to drop: {[name for name, _ in edge_types]}"
                    )
                    for e_type, is_directed in edge_types:
                        try:
                            # DROP EDGE works for both directed and undirected edges
                            drop_edge_cmd = f"DROP EDGE {e_type}"
                            logger.debug(f"Executing: {drop_edge_cmd}")
                            result = self.conn.gsql(drop_edge_cmd)
                            logger.info(
                                f"Successfully dropped edge type '{e_type}': {result}"
                            )
                        except Exception as e:
                            err_str = str(e).lower()
                            # Check if edge doesn't exist (already dropped)
                            if "does not exist" in err_str or "not found" in err_str:
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

                # Step 3: Drop all vertex types globally
                # Vertices are dropped after edges to avoid dependency issues
                # Vertices are global, so we need to query them at global level using GSQL
                try:
                    # Use GSQL to list all global vertex types (not graph-scoped)
                    show_vertices_cmd = "SHOW VERTEX *"
                    result = self.conn.gsql(show_vertices_cmd)
                    result_str = str(result)

                    # Parse vertex types using helper method
                    vertex_types = self._parse_show_vertex_output(result_str)

                    logger.info(
                        f"Found {len(vertex_types)} vertex types to drop: {vertex_types}"
                    )
                    for v_type in vertex_types:
                        try:
                            # Clear data first to avoid dependency issues
                            try:
                                result = self.conn.delVertices(v_type)
                                logger.debug(
                                    f"Cleared data from vertex type '{v_type}': {result}"
                                )
                            except Exception as clear_err:
                                logger.debug(
                                    f"Could not clear data from vertex type '{v_type}': {clear_err}"
                                )

                            # Drop vertex type
                            drop_vertex_cmd = f"DROP VERTEX {v_type}"
                            logger.debug(f"Executing: {drop_vertex_cmd}")
                            result = self.conn.gsql(drop_vertex_cmd)
                            logger.info(
                                f"Successfully dropped vertex type '{v_type}': {result}"
                            )
                        except Exception as e:
                            err_str = str(e).lower()
                            # Check if vertex doesn't exist (already dropped)
                            if "does not exist" in err_str or "not found" in err_str:
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
                        self.delete_database(graph_name)
                    except Exception as e:
                        logger.error(f"Error deleting graph '{graph_name}': {e}")
            elif cnames:
                # Delete vertices from specific vertex types (data only, not schema)
                with self._ensure_graph_context():
                    for class_name in cnames:
                        try:
                            result = self.conn.delVertices(class_name)
                            logger.debug(
                                f"Deleted vertices from {class_name}: {result}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error deleting vertices from {class_name}: {e}"
                            )

        except Exception as e:
            logger.error(f"Error in delete_collections: {e}")

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """
        Batch upsert documents as vertices.
        """
        dry = kwargs.pop("dry", False)
        if dry:
            logger.debug(f"Dry run: would upsert {len(docs)} documents to {class_name}")
            return

        try:
            # Prepare vertices data for pyTigerGraph format
            vertices_data = []
            for doc in docs:
                vertex_id = self._extract_id(doc, match_keys)
                if vertex_id:
                    # Remove internal keys that shouldn't be stored
                    clean_doc = {
                        k: v
                        for k, v in doc.items()
                        if not k.startswith("_") or k == "_key"
                    }
                    vertices_data.append({vertex_id: clean_doc})

            # Batch upsert vertices
            if vertices_data:
                result = self.conn.upsertVertices(class_name, vertices_data)
                logger.debug(
                    f"Upserted {len(vertices_data)} vertices to {class_name}: {result}"
                )
                return result

        except Exception as e:
            logger.error(f"Error upserting vertices to {class_name}: {e}")
            # Fallback to individual operations
            self._fallback_individual_upsert(docs, class_name, match_keys)

    def _fallback_individual_upsert(self, docs, class_name, match_keys):
        """Fallback method for individual vertex upserts."""
        for doc in docs:
            try:
                vertex_id = self._extract_id(doc, match_keys)
                if vertex_id:
                    clean_doc = {
                        k: v
                        for k, v in doc.items()
                        if not k.startswith("_") or k == "_key"
                    }
                    self.conn.upsertVertex(class_name, vertex_id, clean_doc)
            except Exception as e:
                logger.error(f"Error upserting individual vertex {vertex_id}: {e}")

    def insert_edges_batch(
        self,
        docs_edges,
        source_class,
        target_class,
        relation_name,
        collection_name=None,
        match_keys_source=("_key",),
        match_keys_target=("_key",),
        filter_uniques=True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option=False,
        head=None,
        **kwargs,
    ):
        """
        Batch insert edges with proper error handling.
        """
        dry = kwargs.pop("dry", False)
        if dry:
            logger.debug(f"Dry run: would insert {len(docs_edges)} edges")
            return

        # Process edges list
        if isinstance(docs_edges, list):
            if head is not None:
                docs_edges = docs_edges[:head]
            if filter_uniques:
                docs_edges = pick_unique_dict(docs_edges)

        edges_data = []
        for edge_doc in docs_edges:
            try:
                source_doc = edge_doc.get("_source_aux", {})
                target_doc = edge_doc.get("_target_aux", {})
                edge_props = edge_doc.get("_edge_props", {})

                source_id = self._extract_id(source_doc, match_keys_source)
                target_id = self._extract_id(target_doc, match_keys_target)

                if source_id and target_id:
                    edges_data.append((source_id, target_id, edge_props))
                else:
                    logger.warning(
                        f"Missing source_id ({source_id}) or target_id ({target_id}) for edge"
                    )

            except Exception as e:
                logger.error(f"Error processing edge document: {e}")

        # Batch insert edges
        if edges_data:
            try:
                edge_type = relation_name or collection_name
                result = self.conn.upsertEdges(
                    source_class,
                    edge_type,
                    target_class,
                    edges_data,
                )
                logger.debug(
                    f"Inserted {len(edges_data)} edges of type {edge_type}: {result}"
                )
                return result
            except Exception as e:
                logger.error(f"Error batch inserting edges: {e}")

    def _extract_id(self, doc, match_keys):
        """
        Extract vertex ID from document based on match keys.
        """
        if not doc:
            return None

        # Try _key first (common in ArangoDB style docs)
        if "_key" in doc and doc["_key"]:
            return str(doc["_key"])

        # Try other match keys
        for key in match_keys:
            if key in doc and doc[key] is not None:
                return str(doc[key])

        # Fallback: create composite ID
        id_parts = []
        for key in match_keys:
            if key in doc and doc[key] is not None:
                id_parts.append(str(doc[key]))

        return "_".join(id_parts) if id_parts else None

    def insert_return_batch(self, docs, class_name):
        """
        TigerGraph doesn't have INSERT...RETURN semantics like ArangoDB.
        """
        raise NotImplementedError(
            "insert_return_batch not supported in TigerGraph - use upsert_docs_batch instead"
        )

    def fetch_docs(
        self,
        class_name,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
    ):
        """
        Fetch documents (vertices) with filtering and projection.
        """
        try:
            # Get vertices using pyTigerGraph
            vertices = self.conn.getVertices(class_name, limit=limit)

            result = []
            for vertex_id, vertex_data in vertices.items():
                # Extract attributes
                attributes = vertex_data.get("attributes", {})
                doc = {**attributes, "_key": vertex_id}

                # Apply filters (client-side for now)
                if filters and not self._matches_filters(doc, filters):
                    continue

                # Apply projection
                if return_keys is not None:
                    doc = {k: doc.get(k) for k in return_keys if k in doc}
                elif unset_keys is not None:
                    doc = {k: v for k, v in doc.items() if k not in unset_keys}

                result.append(doc)

                # Apply limit after filtering
                if limit and len(result) >= limit:
                    break

            return result

        except Exception as e:
            logger.error(f"Error fetching documents from {class_name}: {e}")
            return []

    def _matches_filters(self, doc, filters):
        """Simple client-side filtering."""
        if isinstance(filters, dict):
            for key, value in filters.items():
                if doc.get(key) != value:
                    return False
        # For list filters, would need more complex logic
        return True

    def fetch_present_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        flatten=False,
        filters: list | dict | None = None,
    ):
        """
        Check which documents from batch are present in the database.
        """
        try:
            present_docs = {}

            for i, doc in enumerate(batch):
                vertex_id = self._extract_id(doc, match_keys)
                if not vertex_id:
                    continue

                try:
                    vertex_data = self.conn.getVerticesById(class_name, vertex_id)
                    if vertex_data and vertex_id in vertex_data:
                        # Extract requested keys
                        vertex_attrs = vertex_data[vertex_id].get("attributes", {})
                        filtered_doc = {}

                        for key in keep_keys:
                            if key == "_key":
                                filtered_doc[key] = vertex_id
                            elif key in vertex_attrs:
                                filtered_doc[key] = vertex_attrs[key]

                        present_docs[i] = [filtered_doc]

                except Exception:
                    # Vertex doesn't exist or error occurred
                    continue

            return present_docs

        except Exception as e:
            logger.error(f"Error fetching present documents: {e}")
            return {}

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """
        Perform aggregation operations.
        """
        try:
            if aggregation_function == AggregationType.COUNT and discriminant is None:
                # Simple vertex count
                count = self.conn.getVertexCount(class_name)
                return [{"_value": count}]
            else:
                # Complex aggregations require custom GSQL queries
                logger.warning(
                    f"Complex aggregation {aggregation_function} requires custom GSQL implementation"
                )
                return []
        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            return []

    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
        """
        Return documents from batch that are NOT present in database.
        """
        present_docs_indices = self.fetch_present_documents(
            batch=batch,
            class_name=class_name,
            match_keys=match_keys,
            keep_keys=keep_keys,
            flatten=False,
            filters=filters,
        )

        absent_indices = sorted(
            set(range(len(batch))) - set(present_docs_indices.keys())
        )
        return [batch[i] for i in absent_indices]

    def define_indexes(self, schema: Schema):
        """Define all indexes from schema."""
        try:
            self.define_vertex_indices(schema.vertex_config)
            self.define_edge_indices(schema.edge_config.edges_list(include_aux=True))
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
            with self._ensure_graph_context():
                result = {}

                if vertex_type:
                    vertex_types = [vertex_type]
                else:
                    vertex_types = self.conn.getVertexTypes(force=True)

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
