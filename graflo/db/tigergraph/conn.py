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
import json
import logging
from typing import Any, cast


import requests
from requests import exceptions as requests_exceptions

from pyTigerGraph import TigerGraphConnection as PyTigerGraphConnection

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import FieldType, Vertex, VertexConfig
from graflo.db.conn import Connection
from graflo.db.connection.onto import TigergraphConfig
from graflo.filter.onto import Clause, Expression
from graflo.onto import AggregationType, DBFlavor, ExpressionFlavor
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

    def __init__(self, config: TigergraphConfig):
        super().__init__()
        self.config = config
        # Store base URLs for REST++ and GSQL endpoints
        self.restpp_url = f"{config.url_without_port}:{config.port}"
        self.gsql_url = f"{config.url_without_port}:{config.gs_port}"

        # Initialize pyTigerGraph connection for most operations
        # Use type narrowing to help type checker understand non-None values
        # PyTigerGraphConnection has defaults for all parameters, so None values are acceptable
        restpp_port: int | str = config.port if config.port is not None else "9000"
        gs_port: int | str = config.gs_port if config.gs_port is not None else "14240"
        graphname: str = (
            config.database if config.database is not None else "DefaultGraph"
        )
        username: str = config.username if config.username is not None else "tigergraph"
        password: str = config.password if config.password is not None else "tigergraph"
        cert_path: str | None = getattr(config, "certPath", None)

        # Build connection kwargs, only include certPath if it's not None
        conn_kwargs: dict[str, Any] = {
            "host": config.url_without_port,
            "restppPort": restpp_port,
            "gsPort": gs_port,
            "graphname": graphname,
            "username": username,
            "password": password,
        }
        if cert_path is not None:
            conn_kwargs["certPath"] = cert_path

        self.conn = PyTigerGraphConnection(**conn_kwargs)

        # Get authentication token if secret is provided
        if config.secret:
            try:
                self.conn.getToken(config.secret)
            except Exception as e:
                logger.warning(f"Failed to get authentication token: {e}")

    def _get_auth_headers(self) -> dict[str, str]:
        """Get HTTP Basic Auth headers if credentials are available.

        Returns:
            Dictionary with Authorization header if credentials exist
        """
        headers = {}
        if self.config.username and self.config.password:
            import base64

            credentials = f"{self.config.username}:{self.config.password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_credentials}"
        return headers

    def _call_restpp_api(
        self,
        endpoint: str,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any] | list[dict]:
        """Call TigerGraph REST++ API endpoint.

        Args:
            endpoint: REST++ API endpoint (e.g., "/graph/{graph_name}/vertices/{vertex_type}")
            method: HTTP method (GET, POST, etc.)
            data: Optional data to send in request body (for POST)
            params: Optional query parameters

        Returns:
            Response data (dict or list)
        """
        url = f"{self.restpp_url}{endpoint}"

        headers = {
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }

        logger.debug(f"REST++ API call: {method} {url}")

        try:
            if method.upper() == "GET":
                response = requests.get(
                    url, headers=headers, params=params, timeout=120
                )
            elif method.upper() == "POST":
                response = requests.post(
                    url,
                    headers=headers,
                    data=json.dumps(data) if data else None,
                    params=params,
                    timeout=120,
                )
            elif method.upper() == "DELETE":
                response = requests.delete(
                    url, headers=headers, params=params, timeout=120
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests_exceptions.HTTPError as errh:
            logger.error(f"HTTP Error: {errh}")
            error_response = {"error": True, "message": str(errh)}
            try:
                # Try to parse error response for more details
                error_json = response.json()
                if isinstance(error_json, dict):
                    error_response.update(error_json)
                else:
                    error_response["details"] = response.text
            except Exception:
                error_response["details"] = response.text
            return error_response
        except requests_exceptions.ConnectionError as errc:
            logger.error(f"Error Connecting: {errc}")
            return {"error": True, "message": str(errc)}
        except requests_exceptions.Timeout as errt:
            logger.error(f"Timeout Error: {errt}")
            return {"error": True, "message": str(errt)}
        except requests_exceptions.RequestException as err:
            logger.error(f"An unexpected error occurred: {err}")
            return {"error": True, "message": str(err)}

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

        # Determine graph name: use config.database if set, otherwise use schema.general.name
        graph_name = self.config.database
        if not graph_name:
            graph_name = schema.general.name
            # Update config for subsequent operations
            self.config.database = graph_name
            logger.info(f"Using schema name '{graph_name}' from schema.general.name")

        try:
            if clean_start:
                try:
                    # Delete all graphs, edges, and vertices (full teardown)
                    self.delete_graph_structure([], [], delete_all=True)
                    logger.debug(f"Cleaned graph '{graph_name}' for fresh start")
                except Exception as clean_error:
                    logger.warning(
                        f"Error during clean_start for graph '{graph_name}': {clean_error}",
                        exc_info=True,
                    )
                    # Continue - may be first run or already clean

            # Step 1: Create vertex and edge types globally first
            # These must exist before they can be included in CREATE GRAPH
            logger.debug(
                f"Creating vertex and edge types globally for graph '{graph_name}'"
            )
            try:
                vertex_names = self._create_vertex_types_global(schema.vertex_config)
                edge_names = self._create_edge_types_global(
                    schema.edge_config.edges_list(include_aux=True)
                )
                logger.debug(
                    f"Created {len(vertex_names)} vertex types and {len(edge_names)} edge types"
                )
            except Exception as type_error:
                logger.error(
                    f"Failed to create vertex/edge types for graph '{graph_name}': {type_error}",
                    exc_info=True,
                )
                raise

            # Step 2: Create graph with vertices and edges explicitly attached
            try:
                if not self.graph_exists(graph_name):
                    logger.debug(f"Creating graph '{graph_name}' with types in init_db")
                    try:
                        self.create_database(
                            graph_name,
                            vertex_names=vertex_names,
                            edge_names=edge_names,
                        )
                        graph_created = True
                        logger.info(f"Successfully created graph '{graph_name}'")
                    except Exception as create_error:
                        logger.error(
                            f"Failed to create graph '{graph_name}': {create_error}",
                            exc_info=True,
                        )
                        raise
                else:
                    logger.debug(f"Graph '{graph_name}' already exists in init_db")
                    # If graph already exists, associate types via ALTER GRAPH
                    try:
                        self.define_vertex_collections(schema.vertex_config)
                        self.define_edge_collections(
                            schema.edge_config.edges_list(include_aux=True)
                        )
                    except Exception as define_error:
                        logger.warning(
                            f"Could not define collections for existing graph '{graph_name}': {define_error}",
                            exc_info=True,
                        )
                        # Continue - graph exists, collections may already be defined
            except Exception as graph_error:
                logger.error(
                    f"Error during graph creation/verification for '{graph_name}': {graph_error}",
                    exc_info=True,
                )
                raise

            # Step 3: Define indexes
            try:
                self.define_indexes(schema)
                logger.info(f"Index definition completed for graph '{graph_name}'")
            except Exception as index_error:
                logger.error(
                    f"Failed to define indexes for graph '{graph_name}': {index_error}",
                    exc_info=True,
                )
                raise
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

        Creates vertices with composite primary keys using PRIMARY KEY syntax.
        According to TigerGraph documentation, fields used in PRIMARY KEY must be
        defined as regular attributes first, and they remain accessible as attributes.

        Reference: https://docs.tigergraph.com/gsql-ref/4.2/ddl-and-loading/defining-a-graph-schema#_composite_key_using_primary_key

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
            # Note: Fields used in PRIMARY KEY must be defined as regular attributes first.
            # They remain accessible as attributes automatically (no primary_id_as_attribute needed).
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
        logger.warning("TigerGraph edge indices not implemented yet [version 4.2.2]")

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        """
        Create an index on a vertex or edge type using GSQL schema change jobs.

        TigerGraph requires indexes to be created through schema change jobs:
        1. CREATE GLOBAL SCHEMA_CHANGE job job_name {ALTER VERTEX ... ADD INDEX ... ON (...);}
        2. RUN GLOBAL SCHEMA_CHANGE job job_name

        Note: TigerGraph only supports secondary indexes on a single field.
        Indexes with multiple fields will be skipped with a warning.
        Edge indexes are not supported in TigerGraph and will be skipped with a warning.

        Args:
            obj_name: Name of the vertex type or edge type
            index: Index configuration object
            is_vertex_index: Whether this is a vertex index (True) or edge index (False)
        """
        try:
            # TigerGraph doesn't support indexes on edges
            if not is_vertex_index:
                logger.warning(
                    f"Edge indexes are not supported in TigerGraph [current version 4.2.2]"
                    f"Skipping index creation for edge '{obj_name}' on field(s) '{index.fields}'"
                )
                return

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

            # Build the ALTER statement inside the job (single field in parentheses)
            # Note: Only vertex indexes are supported - edge indexes are handled earlier
            alter_stmt = (
                f"ALTER VERTEX {obj_name} ADD INDEX {index_name} ON ({field_name})"
            )

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
                    logger.error(f"Failed to run schema change job '{job_name}': {e}")
                    raise
        except Exception as e:
            logger.warning(f"Could not create index for {obj_name}: {e}")

    def _parse_show_output(self, result_str: str, prefix: str) -> list[str]:
        """
        Generic parser for SHOW * output commands.

        Extracts names from lines matching the pattern: "- PREFIX name(...)"

        Args:
            result_str: String output from SHOW * GSQL command
            prefix: The prefix to look for (e.g., "VERTEX", "GRAPH", "JOB")

        Returns:
            List of extracted names
        """
        names = []
        lines = result_str.split("\n")

        for line in lines:
            line = line.strip()
            # Skip empty lines and headers
            if not line or line.startswith("*"):
                continue

            # Remove leading "- " if present
            if line.startswith("- "):
                line = line[2:].strip()

            # Look for prefix pattern
            prefix_upper = prefix.upper()
            if line.upper().startswith(f"{prefix_upper} "):
                # Extract name (after prefix and before opening parenthesis or whitespace)
                after_prefix = line[len(prefix_upper) + 1 :].strip()
                # Name is the first word (before space or parenthesis)
                if "(" in after_prefix:
                    name = after_prefix.split("(")[0].strip()
                else:
                    # No parenthesis, take the first word
                    name = (
                        after_prefix.split()[0].strip()
                        if after_prefix.split()
                        else None
                    )

                if name:
                    names.append(name)

        return names

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

    def _is_not_found_error(self, error: Exception | str) -> bool:
        """
        Check if an error indicates that an object doesn't exist.

        Args:
            error: Exception object or error string

        Returns:
            True if the error indicates "not found" or "does not exist"
        """
        err_str = str(error).lower()
        return "does not exist" in err_str or "not found" in err_str

    def _clean_document(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Remove internal keys that shouldn't be stored in the database.

        Removes keys starting with "_" except "_key".

        Args:
            doc: Document dictionary to clean

        Returns:
            Cleaned document dictionary
        """
        return {k: v for k, v in doc.items() if not k.startswith("_") or k == "_key"}

    def _parse_show_vertex_output(self, result_str: str) -> list[str]:
        """Parse SHOW VERTEX * output to extract vertex type names."""
        return self._parse_show_output(result_str, "VERTEX")

    def _parse_show_graph_output(self, result_str: str) -> list[str]:
        """Parse SHOW GRAPH * output to extract graph names."""
        return self._parse_show_output(result_str, "GRAPH")

    def _parse_show_job_output(self, result_str: str) -> list[str]:
        """Parse SHOW JOB * output to extract job names."""
        return self._parse_show_output(result_str, "JOB")

    def delete_graph_structure(self, vertex_types=(), graph_names=(), delete_all=False):
        """
        Delete graph structure (graphs, vertex types, edge types) from TigerGraph.

        In TigerGraph:
        - Graph: Top-level container (functions like a database in ArangoDB)
        - Vertex Types: Global vertex type definitions (can be shared across graphs)
        - Edge Types: Global edge type definitions (can be shared across graphs)
        - Vertex and edge types are associated with graphs

        Teardown order:
        1. Drop all graphs
        2. Drop all edge types globally
        3. Drop all vertex types globally
        4. Drop all jobs globally

        Args:
            vertex_types: Vertex type names to delete (not used in TigerGraph teardown)
            graph_names: Graph names to delete (if empty and delete_all=True, deletes all)
            delete_all: If True, perform full teardown of all graphs, edges, vertices, and jobs
        """
        cnames = vertex_types
        gnames = graph_names
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
                        if self._is_not_found_error(e):
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
                            if self._is_not_found_error(e):
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
                            if self._is_not_found_error(e):
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

                # Step 4: Drop all jobs globally
                # Jobs are dropped last since they may reference schema objects
                try:
                    # Use GSQL to list all global jobs
                    show_jobs_cmd = "SHOW JOB *"
                    result = self.conn.gsql(show_jobs_cmd)
                    result_str = str(result)

                    # Parse job names using helper method
                    job_names = self._parse_show_job_output(result_str)

                    logger.info(f"Found {len(job_names)} jobs to drop: {job_names}")
                    for job_name in job_names:
                        try:
                            # Drop job
                            # Jobs can be of different types (SCHEMA_CHANGE, LOADING, etc.)
                            # DROP JOB works for all job types
                            drop_job_cmd = f"DROP JOB {job_name}"
                            logger.debug(f"Executing: {drop_job_cmd}")
                            result = self.conn.gsql(drop_job_cmd)
                            logger.info(
                                f"Successfully dropped job '{job_name}': {result}"
                            )
                        except Exception as e:
                            if self._is_not_found_error(e):
                                logger.debug(
                                    f"Job '{job_name}' already dropped or doesn't exist"
                                )
                            else:
                                logger.warning(f"Failed to drop job '{job_name}': {e}")
                                logger.warning(
                                    f"Error details: {type(e).__name__}: {str(e)}"
                                )
                except Exception as e:
                    logger.warning(f"Could not list or drop jobs: {e}")
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
            logger.error(f"Error in delete_graph_structure: {e}")

    def _generate_upsert_payload(
        self, data: list[dict[str, Any]], vname: str, vindex: tuple[str, ...]
    ) -> dict[str, Any]:
        """
        Transforms a list of dictionaries into the TigerGraph REST++ batch upsert JSON format.

        The composite Primary ID is created by concatenating the values of the fields
        specified in vindex with an underscore '_'. Index fields are included in the
        vertex attributes since PRIMARY KEY fields are automatically accessible as
        attributes in TigerGraph queries.

        Attribute values are wrapped in {"value": ...} format as required by TigerGraph REST++ API.

        Args:
            data: List of document dictionaries to upsert
            vname: Target vertex name
            vindex: Tuple of index fields used to create the composite Primary ID

        Returns:
            Dictionary in TigerGraph REST++ batch upsert format:
            {"vertices": {vname: {vertex_id: {attr_name: {"value": attr_value}, ...}}}}
        """
        # Initialize the required JSON structure for vertices
        payload: dict[str, Any] = {"vertices": {vname: {}}}
        vertex_map = payload["vertices"][vname]

        for record in data:
            try:
                # 1. Calculate the Composite Primary ID
                # Assumes all index keys exist in the record
                primary_id_components = [str(record[key]) for key in vindex]
                vertex_id = "_".join(primary_id_components)

                # 2. Clean the record (remove internal keys that shouldn't be stored)
                clean_record = self._clean_document(record)

                # 3. Keep index fields in attributes
                # When using PRIMARY KEY (composite keys), the key fields are automatically
                # accessible as attributes in queries, so we include them in the payload

                # 4. Format attributes for TigerGraph REST++ API
                # TigerGraph requires attribute values to be wrapped in {"value": ...}
                formatted_attributes = {
                    k: {"value": v} for k, v in clean_record.items()
                }

                # 5. Add the record attributes to the map using the composite ID as the key
                vertex_map[vertex_id] = formatted_attributes

            except KeyError as e:
                logger.warning(
                    f"Record is missing a required index field: {e}. Skipping record: {record}"
                )
                continue

        return payload

    def _upsert_data(
        self,
        payload: dict[str, Any],
        host: str,
        graph_name: str,
        username: str | None = None,
        password: str | None = None,
    ) -> dict[str, Any]:
        """
        Sends the generated JSON payload to the TigerGraph REST++ upsert endpoint.

        Args:
            payload: The JSON payload in TigerGraph REST++ format
            host: Base host URL (e.g., "http://localhost:9000")
            graph_name: Name of the graph
            username: Optional username for authentication
            password: Optional password for authentication

        Returns:
            Dictionary containing the response from TigerGraph
        """
        url = f"{host}/graph/{graph_name}"

        headers = {
            "Content-Type": "application/json",
        }

        logger.debug(f"Attempting batch upsert to: {url}")

        try:
            # Use HTTP Basic Auth if username and password are provided
            auth = None
            if username and password:
                auth = (username, password)

            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                auth=auth,
                # Increase timeout for large batches
                timeout=120,
            )
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            # TigerGraph response is a JSON object
            return response.json()

        except requests_exceptions.HTTPError as errh:
            logger.error(f"HTTP Error: {errh}")
            error_details = ""
            try:
                error_details = response.text
            except Exception:
                pass
            return {"error": True, "message": str(errh), "details": error_details}
        except requests_exceptions.ConnectionError as errc:
            logger.error(f"Error Connecting: {errc}")
            return {"error": True, "message": str(errc)}
        except requests_exceptions.Timeout as errt:
            logger.error(f"Timeout Error: {errt}")
            return {"error": True, "message": str(errt)}
        except requests_exceptions.RequestException as err:
            logger.error(f"An unexpected error occurred: {err}")
            return {"error": True, "message": str(err)}

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """
        Batch upsert documents as vertices using TigerGraph REST++ API.

        Creates a GSQL job and formats the payload for batch upsert operations.
        Uses composite Primary IDs constructed from match_keys.
        """
        dry = kwargs.pop("dry", False)
        if dry:
            logger.debug(f"Dry run: would upsert {len(docs)} documents to {class_name}")
            return

        try:
            # Convert match_keys to tuple if it's a list
            vindex = tuple(match_keys) if isinstance(match_keys, list) else match_keys

            # Generate the upsert payload
            payload = self._generate_upsert_payload(docs, class_name, vindex)

            # Check if payload has any vertices
            if not payload.get("vertices", {}).get(class_name):
                logger.warning(f"No valid vertices to upsert for {class_name}")
                return

            # Build REST++ endpoint URL
            host = f"{self.config.url_without_port}:{self.config.port}"
            graph_name = self.config.database
            if not graph_name:
                raise ValueError("Graph name (database) must be configured")

            # Send the upsert request with username/password authentication
            result = self._upsert_data(
                payload,
                host,
                graph_name,
                username=self.config.username,
                password=self.config.password,
            )

            if result.get("error"):
                logger.error(
                    f"Error upserting vertices to {class_name}: {result.get('message')}"
                )
                # Fallback to individual operations
                self._fallback_individual_upsert(docs, class_name, match_keys)
            else:
                num_vertices = len(payload["vertices"][class_name])
                logger.debug(
                    f"Upserted {num_vertices} vertices to {class_name}: {result}"
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
                    clean_doc = self._clean_document(doc)
                    self.conn.upsertVertex(class_name, vertex_id, clean_doc)
            except Exception as e:
                logger.error(f"Error upserting individual vertex {vertex_id}: {e}")

    def _generate_edge_upsert_payload(
        self,
        edges_data: list[tuple[dict, dict, dict]],
        source_class: str,
        target_class: str,
        edge_type: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
    ) -> dict[str, Any]:
        """
        Transforms edge data into the TigerGraph REST++ batch upsert JSON format.

        Args:
            edges_data: List of tuples (source_doc, target_doc, edge_props)
            source_class: Source vertex type name
            target_class: Target vertex type name
            edge_type: Edge type/relation name
            match_keys_source: Tuple of index fields for source vertex
            match_keys_target: Tuple of index fields for target vertex

        Returns:
            Dictionary in TigerGraph REST++ batch upsert format for edges
        """
        # Initialize the required JSON structure for edges
        payload: dict[str, Any] = {"edges": {source_class: {}}}
        source_map = payload["edges"][source_class]

        for source_doc, target_doc, edge_props in edges_data:
            try:
                # Extract source ID (composite if needed)
                if isinstance(match_keys_source, tuple) and len(match_keys_source) > 1:
                    source_id_components = [
                        str(source_doc[key]) for key in match_keys_source
                    ]
                    source_id = "_".join(source_id_components)
                else:
                    source_id = self._extract_id(source_doc, match_keys_source)

                # Extract target ID (composite if needed)
                if isinstance(match_keys_target, tuple) and len(match_keys_target) > 1:
                    target_id_components = [
                        str(target_doc[key]) for key in match_keys_target
                    ]
                    target_id = "_".join(target_id_components)
                else:
                    target_id = self._extract_id(target_doc, match_keys_target)

                if not source_id or not target_id:
                    logger.warning(
                        f"Missing source_id ({source_id}) or target_id ({target_id}) for edge"
                    )
                    continue

                # Initialize source vertex entry if not exists
                if source_id not in source_map:
                    source_map[source_id] = {edge_type: {}}

                # Initialize edge type entry if not exists
                if edge_type not in source_map[source_id]:
                    source_map[source_id][edge_type] = {}

                # Initialize target vertex type entry if not exists
                if target_class not in source_map[source_id][edge_type]:
                    source_map[source_id][edge_type][target_class] = {}

                # Format edge attributes for TigerGraph REST++ API
                # Clean edge properties (remove internal keys)
                clean_edge_props = self._clean_document(edge_props)

                # Format attributes with {"value": ...} wrapper
                formatted_attributes = {
                    k: {"value": v} for k, v in clean_edge_props.items()
                }

                # Add target vertex with edge attributes under target vertex type
                source_map[source_id][edge_type][target_class][target_id] = (
                    formatted_attributes
                )

            except KeyError as e:
                logger.warning(
                    f"Edge is missing a required field: {e}. Skipping edge: {source_doc}, {target_doc}"
                )
                continue
            except Exception as e:
                logger.error(f"Error processing edge: {e}")
                continue

        return payload

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
        Batch insert/upsert edges using TigerGraph REST++ API.

        Handles edge data in tuple format: [(source_doc, target_doc, edge_props), ...]
        or dict format: [{"_source_aux": {...}, "_target_aux": {...}, "_edge_props": {...}}, ...]

        Args:
            docs_edges: List of edge documents (tuples or dicts)
            source_class: Source vertex type name
            target_class: Target vertex type name
            relation_name: Edge type/relation name
            collection_name: Alternative edge collection name (used if relation_name is None)
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: If True, filter duplicate edges
            uniq_weight_fields: Fields to consider for uniqueness (not used in TigerGraph)
            uniq_weight_collections: Collections to consider for uniqueness (not used in TigerGraph)
            upsert_option: If True, use upsert (default behavior in TigerGraph)
            head: Optional limit on number of edges to insert
            **kwargs: Additional options:
                - dry: If True, don't execute the query
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

        # Normalize edge data format - handle both tuple and dict formats
        normalized_edges = []
        for edge_item in docs_edges:
            try:
                if isinstance(edge_item, tuple) and len(edge_item) == 3:
                    # Tuple format: (source_doc, target_doc, edge_props)
                    source_doc, target_doc, edge_props = edge_item
                    normalized_edges.append((source_doc, target_doc, edge_props))
                elif isinstance(edge_item, dict):
                    # Dict format: {"_source_aux": {...}, "_target_aux": {...}, "_edge_props": {...}}
                    source_doc = edge_item.get("_source_aux", {})
                    target_doc = edge_item.get("_target_aux", {})
                    edge_props = edge_item.get("_edge_props", {})
                    normalized_edges.append((source_doc, target_doc, edge_props))
                else:
                    logger.warning(f"Unexpected edge format: {edge_item}")
            except Exception as e:
                logger.error(f"Error normalizing edge item: {e}")
                continue

        if not normalized_edges:
            logger.warning("No valid edges to insert")
            return

        try:
            # Convert match_keys to tuples if they're lists
            match_keys_src = (
                tuple(match_keys_source)
                if isinstance(match_keys_source, list)
                else match_keys_source
            )
            match_keys_tgt = (
                tuple(match_keys_target)
                if isinstance(match_keys_target, list)
                else match_keys_target
            )

            edge_type = relation_name or collection_name
            if not edge_type:
                logger.error(
                    "Edge type must be specified via relation_name or collection_name"
                )
                return

            # Generate the edge upsert payload
            payload = self._generate_edge_upsert_payload(
                normalized_edges,
                source_class,
                target_class,
                edge_type,
                match_keys_src,
                match_keys_tgt,
            )

            # Check if payload has any edges
            source_vertices = payload.get("edges", {}).get(source_class, {})
            if not source_vertices:
                logger.warning(f"No valid edges to upsert for edge type {edge_type}")
                return

            # Build REST++ endpoint URL
            host = f"{self.config.url_without_port}:{self.config.port}"
            graph_name = self.config.database
            if not graph_name:
                raise ValueError("Graph name (database) must be configured")

            # Send the upsert request with username/password authentication
            result = self._upsert_data(
                payload,
                host,
                graph_name,
                username=self.config.username,
                password=self.config.password,
            )

            if result.get("error"):
                logger.error(
                    f"Error upserting edges of type {edge_type}: {result.get('message')}"
                )
            else:
                # Count edges in payload
                edge_count = 0
                for source_edges in source_vertices.values():
                    if edge_type in source_edges:
                        if target_class in source_edges[edge_type]:
                            edge_count += len(source_edges[edge_type][target_class])
                logger.debug(
                    f"Upserted {edge_count} edges of type {edge_type}: {result}"
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

    def _render_rest_filter(
        self,
        filters: list | dict | Clause | None,
        field_types: dict[str, FieldType] | None = None,
    ) -> str:
        """Convert filter expressions to REST++ filter format.

        REST++ filter format: "field=value" or "field>value" etc.
        Format: fieldoperatorvalue (no spaces, quotes for string values)
        Example: "hindex=10" or "hindex>20" or 'name="John"'

        Args:
            filters: Filter expression to convert
            field_types: Optional mapping of field names to FieldType enum values

        Returns:
            str: REST++ filter string (empty if no filters)
        """
        if filters is not None:
            if not isinstance(filters, Clause):
                ff = Expression.from_dict(filters)
            else:
                ff = filters

            # Use ExpressionFlavor.TIGERGRAPH with empty doc_name to trigger REST++ format
            # Pass field_types to help with proper value quoting
            filter_str = ff(
                doc_name="",
                kind=ExpressionFlavor.TIGERGRAPH,
                field_types=field_types,
            )
            return filter_str
        else:
            return ""

    def _get_field_types_for_vertex(
        self, vertex_name: str, vertex_config: None | VertexConfig = None
    ) -> dict[str, FieldType] | None:
        """Get field types for a vertex from vertex config.

        Args:
            vertex_name: Name of the vertex type (or dbname)
            vertex_config: Vertex configuration to use for lookup

        Returns:
            dict[str, FieldType]: Mapping of field names to their FieldType enum values, or None if not available
        """
        if vertex_config is None:
            return None

        try:
            # Get fields with TigerGraph defaults applied
            fields = vertex_config.fields(
                vertex_name,
                with_aux=False,
                as_names=False,
                db_flavor=DBFlavor.TIGERGRAPH,
            )
            # Build field_types dict: {field_name: FieldType}
            field_types = {}
            for field in fields:
                if field.type:
                    # Convert string type to FieldType enum
                    field_types[field.name] = FieldType(field.type)
            return field_types if field_types else None
        except (KeyError, ValueError):
            # Vertex not found in config
            return None

    def fetch_docs(
        self,
        class_name,
        filters: list | dict | Clause | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
        **kwargs,
    ):
        """
        Fetch documents (vertices) with filtering and projection using REST++ API.

        Args:
            class_name: Vertex type name (or dbname)
            filters: Filter expression (list, dict, or Clause)
            limit: Maximum number of documents to return
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection)
            **kwargs: Additional parameters
                field_types: Optional mapping of field names to FieldType enum values
                           Used to properly quote string values in filters
                           If not provided and vertex_config is provided, will be auto-detected
                vertex_config: Optional VertexConfig object to use for field type lookup

        Returns:
            list: List of fetched documents
        """
        try:
            graph_name = self.config.database
            if not graph_name:
                raise ValueError("Graph name (database) must be configured")

            # Get field_types from kwargs or auto-detect from vertex_config
            field_types = kwargs.get("field_types")
            if field_types is None:
                vertex_config = kwargs.get("vertex_config")
                field_types = self._get_field_types_for_vertex(
                    class_name, vertex_config
                )

            # Build REST++ filter string with field type information
            filter_str = self._render_rest_filter(filters, field_types=field_types)

            # Build REST++ API endpoint with query parameters manually
            # Format: /graph/{graph_name}/vertices/{vertex_type}?filter=...&limit=...
            # Example: /graph/g22c97325/vertices/Author?filter=hindex>20&limit=10
            from urllib.parse import quote

            endpoint = f"/graph/{graph_name}/vertices/{class_name}"
            query_parts = []

            if filter_str:
                # URL-encode the filter string to handle special characters
                encoded_filter = quote(filter_str, safe="=<>!&|")
                query_parts.append(f"filter={encoded_filter}")
            if limit is not None:
                query_parts.append(f"limit={limit}")

            if query_parts:
                endpoint = f"{endpoint}?{'&'.join(query_parts)}"

            logger.debug(f"Calling REST++ API: {endpoint}")

            # Call REST++ API directly (no params dict, we built the URL ourselves)
            response = self._call_restpp_api(endpoint)

            # Parse REST++ response (vertices only)
            result: list[dict[str, Any]] = self._parse_restpp_response(
                response, is_edge=False
            )

            # Check for errors
            if isinstance(response, dict) and response.get("error"):
                raise Exception(
                    f"REST++ API error: {response.get('message', response)}"
                )

            # Apply projection (client-side projection is acceptable for result formatting)
            if return_keys is not None:
                result = [
                    {k: doc.get(k) for k in return_keys if k in doc}
                    for doc in result
                    if isinstance(doc, dict)
                ]
            elif unset_keys is not None:
                result = [
                    {k: v for k, v in doc.items() if k not in unset_keys}
                    for doc in result
                    if isinstance(doc, dict)
                ]

            return result

        except Exception as e:
            logger.error(f"Error fetching documents from {class_name} via REST++: {e}")
            raise

    def fetch_edges(
        self,
        from_type: str,
        from_id: str,
        edge_type: str | None = None,
        to_type: str | None = None,
        to_id: str | None = None,
        filters: list | dict | Clause | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
        **kwargs,
    ):
        """
        Fetch edges from TigerGraph using pyTigerGraph's getEdges method.

        In TigerGraph, you must know at least one vertex ID before you can fetch edges.
        Uses pyTigerGraph's getEdges method which handles special characters in vertex IDs.

        Args:
            from_type: Source vertex type (required)
            from_id: Source vertex ID (required)
            edge_type: Optional edge type to filter by
            to_type: Optional target vertex type to filter by (not used in pyTigerGraph)
            to_id: Optional target vertex ID to filter by (not used in pyTigerGraph)
            filters: Additional query filters (not supported by pyTigerGraph getEdges)
            limit: Maximum number of edges to return (not supported by pyTigerGraph getEdges)
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection)
            **kwargs: Additional parameters

        Returns:
            list: List of fetched edges
        """
        try:
            if not from_type or not from_id:
                raise ValueError(
                    "from_type and from_id are required for fetching edges in TigerGraph"
                )

            # Use pyTigerGraph's getEdges method
            # Signature: getEdges(sourceVertexType, sourceVertexId, edgeType=None)
            # Returns: list of edge dictionaries
            logger.debug(
                f"Fetching edges using pyTigerGraph: from_type={from_type}, from_id={from_id}, edge_type={edge_type}"
            )

            # Handle None edge_type by passing empty string (default behavior)
            edge_type_str = edge_type if edge_type is not None else ""
            edges = self.conn.getEdges(from_type, from_id, edge_type_str, fmt="py")

            # Parse pyTigerGraph response format
            # getEdges returns list of dicts with format like:
            # [{"e_type": "...", "from": {...}, "to": {...}, "attributes": {...}}, ...]
            # Type annotation: result is list[dict[str, Any]]
            # getEdges can return dict, str, or DataFrame, but with fmt="py" it returns dict
            if isinstance(edges, list):
                # Type narrowing: after isinstance check, we know it's a list
                # Use cast to help type checker understand the elements are dicts
                result = cast(list[dict[str, Any]], edges)
            elif isinstance(edges, dict):
                # If it's a single dict, wrap it in a list
                result = [cast(dict[str, Any], edges)]
            else:
                # Fallback for unexpected types
                result: list[dict[str, Any]] = []

            # Apply limit if specified (client-side since pyTigerGraph doesn't support it)
            if limit is not None and limit > 0:
                result = result[:limit]

            # Apply projection (client-side projection is acceptable for result formatting)
            if return_keys is not None:
                result = [
                    {k: doc.get(k) for k in return_keys if k in doc}
                    for doc in result
                    if isinstance(doc, dict)
                ]
            elif unset_keys is not None:
                result = [
                    {k: v for k, v in doc.items() if k not in unset_keys}
                    for doc in result
                    if isinstance(doc, dict)
                ]

            return result

        except Exception as e:
            logger.error(f"Error fetching edges via pyTigerGraph: {e}")
            raise

    def _parse_restpp_response(
        self, response: dict | list, is_edge: bool = False
    ) -> list[dict]:
        """Parse REST++ API response into list of documents.

        Args:
            response: REST++ API response (dict or list)
            is_edge: Whether this is an edge response (default: False for vertices)

        Returns:
            list: List of parsed documents
        """
        result = []
        if isinstance(response, dict):
            if "results" in response:
                for data in response["results"]:
                    if is_edge:
                        # Edge response format: {"e_type": "...", "from_id": "...", "to_id": "...", "attributes": {...}}
                        edge_type = data.get("e_type", "")
                        from_id = data.get("from_id", data.get("from", ""))
                        to_id = data.get("to_id", data.get("to", ""))
                        attributes = data.get("attributes", {})
                        doc = {
                            **attributes,
                            "edge_type": edge_type,
                            "from_id": from_id,
                            "to_id": to_id,
                        }
                    else:
                        # Vertex response format: {"v_id": "...", "attributes": {...}}
                        vertex_id = data.get("v_id", data.get("id"))
                        attributes = data.get("attributes", {})
                        doc = {**attributes, "id": vertex_id}
                    result.append(doc)
        elif isinstance(response, list):
            # Direct list response
            for data in response:
                if isinstance(data, dict):
                    if is_edge:
                        edge_type = data.get("e_type", "")
                        from_id = data.get("from_id", data.get("from", ""))
                        to_id = data.get("to_id", data.get("to", ""))
                        attributes = data.get("attributes", data)
                        doc = {
                            **attributes,
                            "edge_type": edge_type,
                            "from_id": from_id,
                            "to_id": to_id,
                        }
                    else:
                        vertex_id = data.get("v_id", data.get("id"))
                        attributes = data.get("attributes", data)
                        doc = {**attributes, "id": vertex_id}
                    result.append(doc)
        return result

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
                            if key == "id":
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
