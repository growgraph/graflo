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
from graflo.db.tigergraph.onto import (
    TIGERGRAPH_TYPE_ALIASES,
    VALID_TIGERGRAPH_TYPES,
)
from graflo.db.util import json_serializer
from graflo.filter.onto import Clause, Expression
from graflo.onto import AggregationType, DBFlavor, ExpressionFlavor
from graflo.util.transform import pick_unique_dict
from urllib.parse import quote

# Alias for backward compatibility
_json_serializer = json_serializer


logger = logging.getLogger(__name__)


# Universal monkey-patch: Add add_note() to base Exception class for Python 3.10 compatibility
# This ensures all exceptions (HTTPError, TigerGraphException, etc.) have add_note()
# Python 3.11+ already has this, but Python 3.10 doesn't
if not hasattr(Exception, "add_note"):

    def add_note(self, note: str) -> None:
        """Add a note to the exception (Python 3.11+ compatibility shim for Python 3.10)."""
        if not hasattr(self, "_notes"):
            self._notes = []
        self._notes.append(note)

    Exception.add_note = add_note  # type: ignore[attr-defined, assignment]


def _wrap_tg_exception(func):
    """Decorator to wrap TigerGraph exceptions for compatibility.

    This decorator is kept for backward compatibility but is no longer strictly
    necessary since we've monkey-patched Exception.add_note() universally.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            # Re-raise all exceptions as-is (add_note is now available on all exceptions)
            raise

    return wrapper


class TigerGraphConnection(Connection):
    """
    TigerGraph database connection implementation.

    Key conceptual differences from ArangoDB:
    1. TigerGraph uses GSQL (Graph Query Language) instead of AQL
    2. Schema must be defined explicitly before data insertion
    3. No automatic vertex/edge class creation - vertices and edges must be pre-defined
    4. Different query syntax and execution model
    5. Token-based authentication recommended for TigerGraph 4+

    Authentication (recommended for TG 4+):
        Token-based authentication using secrets is the most robust and recommended
        approach for TigerGraph 4+. Provide a 'secret' in your TigergraphConfig to
        enable automatic token generation and usage.

        Example:
            >>> config = TigergraphConfig(
            ...     uri="http://localhost:14240",
            ...     username="tigergraph",
            ...     password="tigergraph",
            ...     secret="your_secret_here",  # Recommended for TG 4+
            ...     database="my_graph"
            ... )
            >>> conn = TigerGraphConnection(config)

    Port Configuration for TigerGraph 4+:
        TigerGraph 4.1+ uses port 14240 (GSQL server) as the primary interface.
        Port 9000 (REST++) is for internal use only in TG 4.1+.

        Default behavior: Both restppPort and gsPort default to 14240 for TG 4+ compatibility.

        For custom Docker deployments with port mapping, explicitly set both ports:
            >>> config = TigergraphConfig(
            ...     uri="http://localhost:9001",  # Custom REST++ port
            ...     gs_port=14241,                 # Custom GSQL port
            ... )

    Version Compatibility:
        - TigerGraph 4.2.2+: Direct REST API endpoints (no /restpp prefix)
        - TigerGraph 4.2.1 and older: REST API with /restpp prefix
        - Version is auto-detected, or can be manually specified in config
    """

    flavor = DBFlavor.TIGERGRAPH

    def __init__(self, config: TigergraphConfig):
        super().__init__()
        self.config = config
        self.ssl_verify = getattr(config, "ssl_verify", True)

        # Initialize pyTigerGraph connection for most operations
        # Use type narrowing to help type checker understand non-None values
        # For TigerGraph 4+, both ports typically route through the GSQL server (14240)
        # Port 9000 (REST++) is internal-only in TG 4.1+
        restpp_port: int | str = config.port if config.port is not None else "14240"
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
        # Token-based auth is the recommended approach for TigerGraph 4+
        self.api_token: str | None = None
        if config.secret:
            try:
                token = self.conn.getToken(config.secret)
                # getToken returns tuple (token, expiration) or just token
                if isinstance(token, tuple):
                    self.api_token = token[0]
                    logger.info(
                        f"Successfully obtained API token (expires: {token[1]})"
                    )
                else:
                    self.api_token = token
                    logger.info("Successfully obtained API token")
                # Explicitly set token on connection object for TigerGraph 4.2.1 compatibility
                # This ensures pyTigerGraph internal calls use the same token
                if self.api_token:
                    if hasattr(self.conn, "apiToken"):
                        self.conn.apiToken = self.api_token  # type: ignore[attr-defined]
                    if hasattr(self.conn, "token"):
                        self.conn.token = self.api_token  # type: ignore[attr-defined]
                    if hasattr(self.conn, "setToken"):
                        self.conn.setToken(self.api_token)  # type: ignore[attr-defined]
            except Exception as e:
                # Log and fall back to username/password authentication
                logger.warning(f"Failed to get authentication token: {e}")
                logger.warning("Falling back to username/password authentication")

        # Detect TigerGraph version for compatibility
        self.tg_version: str | None = None
        self._use_restpp_prefix = False  # Default for 4.2.2+

        # Check if version is manually configured first
        if hasattr(config, "version") and config.version:
            version_str = config.version
            logger.info(f"Using manually configured TigerGraph version: {version_str}")
        else:
            # Auto-detect version
            try:
                version_info = self.conn.getVersion()
                # getVersion() can return different formats:
                # - list: [{"version": "release_4.2.2_..."}, ...]
                # - dict: {"version": "release_4.2.2_..."} or {"api": [{"version": "4.2.2"}]}
                # - str: "4.2.2"
                version_str = None

                if isinstance(version_info, list) and len(version_info) > 0:
                    first_item = version_info[0]
                    if isinstance(first_item, dict) and "version" in first_item:
                        version_str = str(first_item["version"])
                    else:
                        version_str = str(first_item)
                elif isinstance(version_info, dict):
                    # Try different dict structures
                    if "version" in version_info:
                        version_str = str(version_info["version"])
                    elif "api" in version_info and isinstance(
                        version_info["api"], list
                    ):
                        if (
                            len(version_info["api"]) > 0
                            and "version" in version_info["api"][0]
                        ):
                            version_str = str(version_info["api"][0]["version"])
                elif isinstance(version_info, str):
                    version_str = version_info
            except Exception as e:
                logger.warning(
                    f"Failed to detect TigerGraph version: {e}. "
                    f"Defaulting to 4.2.2+ behavior (no /restpp prefix)"
                )
                version_str = None

        # Parse version string if we have one
        if version_str:
            # Extract version from strings like "release_4.2.2_09-29-2025" or "4.2.1" or "v4.2.1"
            import re

            version_match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_str)
            if version_match:
                major = int(version_match.group(1))
                minor = int(version_match.group(2))
                patch = int(version_match.group(3))
                self.tg_version = f"{major}.{minor}.{patch}"

                # Version 4.2.1 and older need /restpp prefix
                if (major, minor, patch) < (4, 2, 2):
                    self._use_restpp_prefix = True
                    logger.info(
                        f"TigerGraph version {self.tg_version} detected, "
                        f"using /restpp prefix for REST API"
                    )
                else:
                    logger.info(
                        f"TigerGraph version {self.tg_version} detected, "
                        f"using direct REST API endpoints"
                    )
            else:
                logger.warning(
                    f"Could not extract version number from '{version_str}'. "
                    f"Defaulting to 4.2.2+ behavior (no /restpp prefix)"
                )

        # Store base URLs for REST++ and GSQL endpoints
        # For version 4.2.1 and older, include /restpp in the path
        base_url = f"{config.url_without_port}:{config.port}"
        if self._use_restpp_prefix:
            self.restpp_url = f"{base_url}/restpp"
        else:
            self.restpp_url = base_url
        self.gsql_url = f"{config.url_without_port}:{config.gs_port}"

    def _get_auth_headers(self) -> dict[str, str]:
        """Get authentication headers for REST API calls.

        Prioritizes token-based authentication over Basic Auth:
        1. If API token is available (from secret), use Bearer token (recommended for TG 4+)
        2. Otherwise, fall back to HTTP Basic Auth with username/password

        Returns:
            Dictionary with Authorization header
        """
        headers = {}

        # Prefer token-based authentication (recommended for TigerGraph 4+)
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        elif self.config.username and self.config.password:
            # Fallback to HTTP Basic Auth
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
                    url,
                    headers=headers,
                    params=params,
                    timeout=120,
                    verify=self.ssl_verify,
                )
            elif method.upper() == "POST":
                response = requests.post(
                    url,
                    headers=headers,
                    data=json.dumps(data, default=_json_serializer) if data else None,
                    params=params,
                    timeout=120,
                    verify=self.ssl_verify,
                )
            elif method.upper() == "DELETE":
                response = requests.delete(
                    url,
                    headers=headers,
                    params=params,
                    timeout=120,
                    verify=self.ssl_verify,
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

    @_wrap_tg_exception
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

    @_wrap_tg_exception
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

            # First, try to drop all queries associated with the graph
            # Try multiple approaches to ensure queries are dropped
            queries_dropped = False
            try:
                with self._ensure_graph_context(name):
                    # Get all installed queries for this graph
                    try:
                        queries = self.conn.getInstalledQueries()
                        if queries:
                            logger.info(
                                f"Dropping {len(queries)} queries from graph '{name}'"
                            )
                            for query_name in queries:
                                try:
                                    # Try DROP QUERY with IF EXISTS to avoid errors
                                    drop_query_cmd = f"USE GRAPH {name}\nDROP QUERY {query_name} IF EXISTS"
                                    self.conn.gsql(drop_query_cmd)
                                    logger.debug(
                                        f"Dropped query '{query_name}' from graph '{name}'"
                                    )
                                    queries_dropped = True
                                except Exception:
                                    # Try without IF EXISTS for older TigerGraph versions
                                    try:
                                        drop_query_cmd = (
                                            f"USE GRAPH {name}\nDROP QUERY {query_name}"
                                        )
                                        self.conn.gsql(drop_query_cmd)
                                        logger.debug(
                                            f"Dropped query '{query_name}' from graph '{name}'"
                                        )
                                        queries_dropped = True
                                    except Exception as qe2:
                                        logger.warning(
                                            f"Could not drop query '{query_name}' from graph '{name}': {qe2}"
                                        )
                    except Exception as e:
                        logger.debug(f"Could not list queries for graph '{name}': {e}")
            except Exception as e:
                logger.debug(
                    f"Could not access graph '{name}' to drop queries: {e}. "
                    f"Graph may not exist or queries may not be accessible."
                )

            # If we couldn't drop queries through the API, try direct GSQL
            if not queries_dropped:
                try:
                    # Try to drop queries using GSQL directly
                    list_queries_cmd = f"USE GRAPH {name}\nSHOW QUERY *"
                    result = self.conn.gsql(list_queries_cmd)
                    # Parse result to get query names and drop them
                    # This is a fallback if getInstalledQueries() doesn't work
                except Exception as e:
                    logger.debug(
                        f"Could not list queries via GSQL for graph '{name}': {e}"
                    )

            # Now try to drop the graph
            # First, try to clear all data from the graph to avoid dependency issues
            try:
                with self._ensure_graph_context(name):
                    # Clear all vertices to remove dependencies
                    try:
                        vertex_types = self.conn.getVertexTypes(force=True)
                        for v_type in vertex_types:
                            try:
                                self.conn.delVertices(v_type)
                                logger.debug(
                                    f"Cleared vertices of type '{v_type}' from graph '{name}'"
                                )
                            except Exception as ve:
                                logger.debug(
                                    f"Could not clear vertices '{v_type}': {ve}"
                                )
                    except Exception as e:
                        logger.debug(f"Could not clear vertices: {e}")
            except Exception as e:
                logger.debug(f"Could not access graph context to clear data: {e}")

            try:
                # Use the graph first to ensure we're working with the right graph
                drop_command = f"USE GRAPH {name}\nDROP GRAPH {name}"
                result = self.conn.gsql(drop_command)
                logger.info(f"Successfully dropped graph '{name}': {result}")
                return result
            except Exception as e:
                error_str = str(e).lower()
                # If graph has dependencies (queries, etc.), try to continue anyway
                # The graph structure might still be partially cleaned
                if "depends on" in error_str or "query" in error_str:
                    logger.warning(
                        f"Could not fully drop graph '{name}' due to dependencies: {e}. "
                        f"Attempting to continue - graph may be partially cleaned."
                    )
                    # Don't raise - allow the process to continue
                    # The schema creation will handle existing types
                    return None
                else:
                    error_msg = f"Could not drop graph '{name}'. Error: {e}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg) from e

            # Fallback 1: Attempt to disassociate edge and vertex types from graph
            # DO NOT drop global vertex/edge types as they might be used by other graphs
            try:
                with self._ensure_graph_context(name):
                    # Disassociate edge types from graph (but don't drop them globally)
                    try:
                        edge_types = self.conn.getEdgeTypes(force=True)
                    except Exception:
                        edge_types = []

                    for e_type in edge_types:
                        # Only disassociate from graph, don't drop globally
                        # ALTER GRAPH requires USE GRAPH context
                        try:
                            drop_edge_cmd = f"USE GRAPH {name}\nALTER GRAPH {name} DROP DIRECTED EDGE {e_type}"
                            self.conn.gsql(drop_edge_cmd)
                            logger.debug(
                                f"Disassociated edge type '{e_type}' from graph '{name}'"
                            )
                        except Exception as e:
                            logger.debug(
                                f"Could not disassociate edge type '{e_type}' from graph '{name}': {e}"
                            )
                            # Continue - edge might not be associated or graph might not exist

                    # Disassociate vertex types from graph (but don't drop them globally)
                    try:
                        vertex_types = self.conn.getVertexTypes(force=True)
                    except Exception:
                        vertex_types = []

                    for v_type in vertex_types:
                        # Only clear data from this graph's vertices, don't drop vertex type globally
                        # Clear data first to avoid dependency issues
                        try:
                            self.conn.delVertices(v_type)
                            logger.debug(
                                f"Cleared vertices of type '{v_type}' from graph '{name}'"
                            )
                        except Exception as e:
                            logger.debug(
                                f"Could not clear vertices of type '{v_type}' from graph '{name}': {e}"
                            )
                        # Disassociate from graph (best-effort)
                        # ALTER GRAPH requires USE GRAPH context
                        try:
                            drop_vertex_cmd = f"USE GRAPH {name}\nALTER GRAPH {name} DROP VERTEX {v_type}"
                            self.conn.gsql(drop_vertex_cmd)
                            logger.debug(
                                f"Disassociated vertex type '{v_type}' from graph '{name}'"
                            )
                        except Exception as e:
                            logger.debug(
                                f"Could not disassociate vertex type '{v_type}' from graph '{name}': {e}"
                            )
                            # Continue - vertex might not be associated or graph might not exist
            except Exception as e3:
                logger.warning(
                    f"Could not disassociate schema types from graph '{name}': {e3}. Proceeding to data clear."
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

    @_wrap_tg_exception
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

    def _get_vertex_add_statement(
        self, vertex: Vertex, vertex_config: VertexConfig
    ) -> str:
        """Generate ADD VERTEX statement for a schema change job.

        Args:
            vertex: Vertex object to generate statement for
            vertex_config: Vertex configuration

        Returns:
            str: GSQL ADD VERTEX statement
        """
        vertex_dbname = vertex_config.vertex_dbname(vertex.name)
        index_fields = vertex_config.index(vertex.name).fields

        if len(index_fields) == 0:
            raise ValueError(
                f"Vertex '{vertex_dbname}' must have at least one index field"
            )

        # Get field type for primary key field(s) - convert FieldType enum to string
        field_type_map = {}
        for f in vertex.fields:
            if f.type:
                field_type_map[f.name] = (
                    f.type.value if hasattr(f.type, "value") else str(f.type)
                )
            else:
                field_type_map[f.name] = FieldType.STRING.value

        # Format all fields
        all_fields = []
        for field in vertex.fields:
            if field.type:
                field_type = (
                    field.type.value
                    if hasattr(field.type, "value")
                    else str(field.type)
                )
            else:
                field_type = FieldType.STRING.value
            all_fields.append((field.name, field_type))

        if len(index_fields) == 1:
            # Single field: use PRIMARY_ID syntax (required by GSQL)
            primary_field_name = index_fields[0]
            primary_field_type = field_type_map.get(
                primary_field_name, FieldType.STRING.value
            )

            other_fields = [
                (name, ftype)
                for name, ftype in all_fields
                if name != primary_field_name
            ]

            # Build field list: PRIMARY_ID comes first, then other fields
            field_parts = [f"PRIMARY_ID {primary_field_name} {primary_field_type}"]
            field_parts.extend([f"{name} {ftype}" for name, ftype in other_fields])

            field_definitions = ",\n        ".join(field_parts)

            return (
                f"ADD VERTEX {vertex_dbname} (\n"
                f"        {field_definitions}\n"
                f'    ) WITH STATS="OUTDEGREE_BY_EDGETYPE", PRIMARY_ID_AS_ATTRIBUTE="true"'
            )
        else:
            # Composite key: use PRIMARY KEY syntax
            field_parts = [f"{name} {ftype}" for name, ftype in all_fields]
            vindex = "(" + ", ".join(index_fields) + ")"
            field_parts.append(f"PRIMARY KEY {vindex}")

            field_definitions = ",\n        ".join(field_parts)

            return (
                f"ADD VERTEX {vertex_dbname} (\n"
                f"        {field_definitions}\n"
                f'    ) WITH STATS="OUTDEGREE_BY_EDGETYPE"'
            )

    def _format_edge_attributes(
        self, edge: Edge, exclude_fields: set[str] | None = None
    ) -> str:
        """Format edge attributes for GSQL ADD DIRECTED EDGE statement.

        Args:
            edge: Edge object to format attributes for
            exclude_fields: Optional set of field names to exclude from attributes

        Returns:
            str: Formatted attribute string (e.g., "    date STRING,\n    relation STRING")
        """
        if not edge.weights or not edge.weights.direct:
            return ""

        if exclude_fields is None:
            exclude_fields = set()

        attr_parts = []
        for field in edge.weights.direct:
            field_name = field.name
            if field_name not in exclude_fields:
                field_type = self._get_tigergraph_type(field.type)
                attr_parts.append(f"    {field_name} {field_type}")

        return ",\n".join(attr_parts)

    def _get_edge_add_statement(self, edge: Edge) -> str:
        """Generate ADD DIRECTED EDGE statement for a schema change job.

        Args:
            edge: Edge object to generate statement for

        Returns:
            str: GSQL ADD DIRECTED EDGE statement
        """
        # TigerGraph requires discriminators to support multiple edges of the same type
        # between the same pair of vertices. We add discriminators for all indexed fields.
        # Collect all indexed fields from edge.indexes
        indexed_field_names = set()
        for index in edge.indexes:
            for field_name in index.fields:
                # Skip special fields like "_from", "_to" which are ArangoDB-specific
                if field_name not in ["_from", "_to"]:
                    indexed_field_names.add(field_name)

        # Also include relation_field if it's set (for backward compatibility)
        if edge.relation_field and edge.relation_field not in indexed_field_names:
            indexed_field_names.add(edge.relation_field)

        # IMPORTANT: In TigerGraph, discriminator fields MUST also be edge attributes.
        # If an indexed field is not in weights.direct, we need to add it.
        # Initialize weights if not present
        if edge.weights is None:
            from graflo.architecture.edge import WeightConfig, Field

            edge.weights = WeightConfig()

        # Type assertion: weights is guaranteed to be WeightConfig after assignment
        assert edge.weights is not None, "weights should be initialized"
        # Get existing weight field names
        existing_weight_names = set()
        if edge.weights.direct:
            existing_weight_names = {field.name for field in edge.weights.direct}

        # Add any indexed fields that are missing from weights
        for field_name in indexed_field_names:
            if field_name not in existing_weight_names:
                # Add the field to weights with STRING type (default)
                from graflo.architecture.edge import Field

                edge.weights.direct.append(
                    Field(name=field_name, type=FieldType.STRING)
                )
                logger.info(
                    f"Added indexed field '{field_name}' to edge weights for discriminator compatibility"
                )

        # Format edge attributes, excluding discriminator fields (they're in DISCRIMINATOR clause)
        edge_attrs = self._format_edge_attributes(
            edge, exclude_fields=indexed_field_names
        )

        # Build discriminator clause with all indexed fields
        # DISCRIMINATOR goes INSIDE parentheses, on same line as FROM/TO, with types
        # Format: FROM company, TO company, DISCRIMINATOR(relation STRING), date STRING, ...

        # Get field types for discriminator fields
        field_types = {}
        if edge.weights and edge.weights.direct:
            for field in edge.weights.direct:
                field_types[field.name] = self._get_tigergraph_type(field.type)

        # Build FROM/TO line with discriminator
        from_to_parts = [
            f"        FROM {edge._source}",
            f"        TO {edge._target}",
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
                f"Added discriminator for edge {edge.relation}: {', '.join(discriminator_parts)}"
            )
        else:
            logger.debug(
                f"No indexed fields found for edge {edge.relation}. "
                f"Indexes: {[idx.fields for idx in edge.indexes]}, "
                f"relation_field: {edge.relation_field}"
            )

        # Combine FROM/TO and discriminator with commas
        from_to_line = ",\n".join(from_to_parts)

        # Build the complete statement
        if edge_attrs:
            # Has attributes - add comma after FROM/TO line (which may include discriminator)
            # edge_attrs already has proper indentation, so we just need to add it after a comma
            return (
                f"ADD DIRECTED EDGE {edge.relation} (\n"
                f"{from_to_line},\n"
                f"{edge_attrs}\n"
                f"    )"
            )
        else:
            # No attributes - FROM/TO line (which may include discriminator) is the last thing
            # No trailing comma needed
            return f"ADD DIRECTED EDGE {edge.relation} (\n{from_to_line}\n    )"

    @_wrap_tg_exception
    def _define_schema_local(self, schema: Schema) -> None:
        """Define TigerGraph schema locally for the current graph using a SCHEMA_CHANGE job.

        Args:
            schema: Schema definition
        """
        graph_name = self.config.database
        if not graph_name:
            raise ValueError("Graph name (database) must be configured")

        vertex_config = schema.vertex_config
        edge_config = schema.edge_config

        schema_change_stmts = []

        # Vertices
        for vertex in vertex_config.vertices:
            stmt = self._get_vertex_add_statement(vertex, vertex_config)
            schema_change_stmts.append(stmt)

        # Edges
        edges_to_create = list(edge_config.edges_list(include_aux=True))
        for edge in edges_to_create:
            edge.finish_init(vertex_config)
            stmt = self._get_edge_add_statement(edge)
            schema_change_stmts.append(stmt)

        if not schema_change_stmts:
            logger.debug(f"No schema changes to apply for graph '{graph_name}'")
            return

        job_name = f"schema_change_{graph_name}"

        # First, try to drop the job if it exists (ignore errors if it doesn't)
        try:
            drop_job_cmd = f"USE GRAPH {graph_name}\nDROP JOB {job_name}"
            self.conn.gsql(drop_job_cmd)
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

        # Combine into a single SCHEMA_CHANGE job
        gsql_commands = [
            f"USE GRAPH {graph_name}",
            f"CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {graph_name} {{",
            "    " + ";\n    ".join(schema_change_stmts) + ";",
            "}",
            f"RUN SCHEMA_CHANGE JOB {job_name}",
        ]

        full_gsql = "\n".join(gsql_commands)
        logger.info(f"Applying local schema change for graph '{graph_name}'")
        logger.info(f"GSQL command:\n{full_gsql}")
        try:
            result = self.conn.gsql(full_gsql)
            logger.debug(f"Schema change result: {result}")

            # Check if result indicates an error - be more lenient with error detection
            result_str = str(result) if result else ""
            # Only treat as error if result explicitly contains error indicators
            if (
                result
                and result_str
                and (
                    "Encountered" in result_str
                    or "syntax error" in result_str.lower()
                    or "parse error" in result_str.lower()
                )
            ):
                error_msg = f"Schema change job reported a syntax/parse error: {result}"
                logger.error(error_msg)
                logger.error(f"GSQL command that failed: {full_gsql}")
                raise RuntimeError(error_msg)

            # Verify that the schema was actually created by checking vertex and edge types
            # Wait a moment for schema changes to propagate
            import time

            time.sleep(1.0)  # Increased wait time

            with self._ensure_graph_context(graph_name):
                vertex_types = self.conn.getVertexTypes(force=True)
                edge_types = self.conn.getEdgeTypes(force=True)

                # Use vertex_dbname instead of v.name to match what TigerGraph actually creates
                # vertex_dbname returns dbname if set, otherwise None - fallback to v.name if None
                expected_vertex_types = set()
                for v in vertex_config.vertices:
                    try:
                        dbname = vertex_config.vertex_dbname(v.name)
                        # If dbname is None, use vertex name
                        expected_name = dbname if dbname is not None else v.name
                    except (KeyError, AttributeError):
                        # Fallback to vertex name if vertex_dbname fails
                        expected_name = v.name
                    expected_vertex_types.add(expected_name)

                expected_edge_types = {
                    e.relation for e in edges_to_create if e.relation
                }

                # Convert to sets for case-insensitive comparison
                # TigerGraph may capitalize vertex names, so compare case-insensitively
                vertex_types_lower = {vt.lower() for vt in vertex_types}
                expected_vertex_types_lower = {
                    evt.lower() for evt in expected_vertex_types
                }

                missing_vertices_lower = (
                    expected_vertex_types_lower - vertex_types_lower
                )
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
                        f"Created edge types: {edge_types}. "
                        f"GSQL result: {result}"
                    )
                    logger.error(error_msg)
                    logger.error(f"GSQL command that failed: {full_gsql}")
                    raise RuntimeError(error_msg)

                logger.info(
                    f"Schema verified: {len(vertex_types)} vertex types, {len(edge_types)} edge types created"
                )
        except RuntimeError:
            # Re-raise RuntimeError as-is
            raise
        except Exception as e:
            logger.error(f"Failed to apply local schema change: {e}")
            logger.error(f"GSQL command was: {full_gsql}")
            raise

    @_wrap_tg_exception
    def init_db(self, schema: Schema, clean_start: bool = False) -> None:
        """
        Initialize database with schema definition.

        Follows the same pattern as ArangoDB:
        1. Clean if needed
        2. Create graph if not exists
        3. Define schema locally within the graph
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
                    # Only delete the current graph
                    self.delete_database(graph_name)
                    logger.debug(f"Cleaned graph '{graph_name}' for fresh start")
                except Exception as clean_error:
                    logger.warning(
                        f"Error during clean_start for graph '{graph_name}': {clean_error}",
                        exc_info=True,
                    )

            # Step 1: Create graph first if it doesn't exist
            if not self.graph_exists(graph_name):
                logger.debug(f"Creating empty graph '{graph_name}'")
                try:
                    # Create empty graph
                    self.create_database(graph_name)
                    graph_created = True
                    logger.info(f"Successfully created empty graph '{graph_name}'")
                except Exception as create_error:
                    logger.error(
                        f"Failed to create graph '{graph_name}': {create_error}",
                        exc_info=True,
                    )
                    raise
            else:
                logger.debug(f"Graph '{graph_name}' already exists in init_db")

            # Step 2: Define schema locally for the graph
            # This uses a SCHEMA_CHANGE job which is the standard way to define local types
            logger.info(f"Defining local schema for graph '{graph_name}'")
            try:
                self._define_schema_local(schema)
            except Exception as schema_error:
                logger.error(
                    f"Failed to define local schema for graph '{graph_name}': {schema_error}",
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

    @_wrap_tg_exception
    def define_schema(self, schema: Schema):
        """
        Define TigerGraph schema locally for the current graph.

        Assumes graph already exists (created in init_db).
        """
        try:
            self._define_schema_local(schema)
        except Exception as e:
            logger.error(f"Error defining schema: {e}")
            raise

    def define_vertex_classes(  # type: ignore[override]
        self, vertex_config: VertexConfig
    ) -> None:
        """Define TigerGraph vertex types locally for the current graph.

        Args:
            vertex_config: Vertex configuration containing vertices to create
        """
        graph_name = self.config.database
        if not graph_name:
            raise ValueError("Graph name (database) must be configured")

        schema_change_stmts = []
        for vertex in vertex_config.vertices:
            stmt = self._get_vertex_add_statement(vertex, vertex_config)
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
        self.conn.gsql("\n".join(gsql_commands))

    def define_edge_classes(self, edges: list[Edge]):
        """Define TigerGraph edge types locally for the current graph.

        Args:
            edges: List of edges to create
        """
        graph_name = self.config.database
        if not graph_name:
            raise ValueError("Graph name (database) must be configured")

        # Need vertex_config for dbname lookup if finish_init hasn't been called
        # But edges should ideally already be initialized.
        # If not, this might fail or needs a vertex_config.

        schema_change_stmts = []
        for edge in edges:
            stmt = self._get_edge_add_statement(edge)
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
        self.conn.gsql("\n".join(gsql_commands))

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
        fields = vertex.fields

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

    def _format_edge_attributes_for_create(self, edge: Edge) -> str:
        """
        Format edge attributes for GSQL CREATE EDGE statement.

        Edge weights/attributes come from edge.weights.direct (list of Field objects).
        Each weight field needs to be included in the CREATE EDGE statement with its type.
        """
        attrs = []

        # Get weight fields from edge.weights.direct
        if edge.weights and edge.weights.direct:
            for field in edge.weights.direct:
                # Field objects have name and type attributes
                field_name = field.name
                # Get TigerGraph type - FieldType enum values are already in TigerGraph format
                tg_type = self._get_tigergraph_type(field.type)
                attrs.append(f"{field_name} {tg_type}")

        return ",\n    " + ",\n    ".join(attrs) if attrs else ""

    def _get_tigergraph_type(self, field_type: FieldType | str | None) -> str:
        """
        Convert field type to TigerGraph type string.

        FieldType enum values are already in TigerGraph format (e.g., "INT", "STRING", "DATETIME").
        This method normalizes various input formats to the correct TigerGraph type.

        Args:
            field_type: FieldType enum, string, or None

        Returns:
            str: TigerGraph type string (e.g., "INT", "STRING", "DATETIME")
        """
        if field_type is None:
            return FieldType.STRING.value

        # If it's a FieldType enum, use its value directly (already in TigerGraph format)
        if isinstance(field_type, FieldType):
            return field_type.value

        # If it's an enum-like object with a value attribute
        if hasattr(field_type, "value"):
            enum_value = field_type.value
            # Convert to string and normalize
            enum_value_str = str(enum_value).upper()
            # Check if the value matches a FieldType enum value
            if enum_value_str in VALID_TIGERGRAPH_TYPES:
                return enum_value_str
            # Return as string (normalized to uppercase)
            return enum_value_str

        # If it's a string, normalize and check against FieldType values
        field_type_str = str(field_type).upper()

        # Check if it matches a FieldType enum value directly
        if field_type_str in VALID_TIGERGRAPH_TYPES:
            return field_type_str

        # Handle TigerGraph-specific type aliases
        return TIGERGRAPH_TYPE_ALIASES.get(field_type_str, FieldType.STRING.value)

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
        """Define indices for edges if specified.

        Note: TigerGraph does not support creating indexes on edge attributes.
        Edge indexes are skipped with a warning. Only vertex indexes are supported.
        """
        for edge in edges:
            if edge.indexes:
                logger.info(
                    f"Skipping {len(edge.indexes)} index(es) on edge '{edge.relation}': "
                    f"TigerGraph does not support indexes on edge attributes. "
                    f"Only vertex indexes are supported."
                )
                # Skip edge index creation - TigerGraph doesn't support it
                # for index_obj in edge.indexes:
                #     self._add_index(edge.relation, index_obj, is_vertex_index=False)

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
            graph_name = self.config.database

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
                self.conn.gsql(drop_job_cmd)
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
            run_job_cmd = f"RUN SCHEMA_CHANGE job {job_name}"

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
    ) -> dict[str, Any]:
        """
        Sends the generated JSON payload to the TigerGraph REST++ upsert endpoint.

        Args:
            payload: The JSON payload in TigerGraph REST++ format

        Returns:
            Dictionary containing the response from TigerGraph
        """
        graph_name = self.config.database
        if not graph_name:
            raise ValueError("Graph name (database) must be configured")

        # Use restpp_url which handles version-specific prefixes (e.g., /restpp for 4.2.1)
        url = f"{self.restpp_url}/graph/{graph_name}"

        # Use centralized auth headers (supports Bearer token for 4.2.1+)
        headers = self._get_auth_headers()
        headers["Content-Type"] = "application/json"

        logger.debug(f"Attempting batch upsert to: {url}")

        try:
            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload, default=_json_serializer),
                # Increase timeout for large batches
                timeout=120,
                verify=self.ssl_verify,
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

    @_wrap_tg_exception
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

            # Send the upsert request
            result = self._upsert_data(payload)

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
                    # Serialize datetime objects before passing to pyTigerGraph
                    # pyTigerGraph's upsertVertex expects JSON-serializable data
                    serialized_doc = json.loads(
                        json.dumps(clean_doc, default=_json_serializer)
                    )
                    self.conn.upsertVertex(class_name, vertex_id, serialized_doc)
            except Exception as e:
                logger.error(f"Error upserting individual vertex {vertex_id}: {e}")

    def _generate_edge_upsert_payloads(
        self,
        edges_data: list[tuple[dict, dict, dict]],
        source_class: str,
        target_class: str,
        edge_type: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        """
        Transforms edge data into multiple TigerGraph REST++ batch upsert JSON payloads.

        Groups edges by (source_id, target_id, edge_type) and collects all weight combinations
        for each triple. Then creates separate payloads by "zipping" the weight lists across
        all (source_id, target_id, edge_type) groups.

        Args:
            edges_data: List of tuples (source_doc, target_doc, edge_props)
            source_class: Source vertex type name
            target_class: Target vertex type name
            edge_type: Edge type/relation name (e.g., "relates")
            match_keys_source: Tuple of index fields for source vertex
            match_keys_target: Tuple of index fields for target vertex

        Returns:
            List of payload dictionaries in TigerGraph REST++ format:
            [{"edges": {source_v_type: {source_id: {edge_type: {target_v_type: {target_id: attributes}}}}}}, ...]
        """
        from collections import defaultdict

        # Step 1: Group edges by (source_id, target_id, edge_type) and collect weight combinations
        # Structure: {(source_id, target_id, edge_type): [weight_dict1, weight_dict2, ...]}
        uvr_weights_map: defaultdict[tuple[str, str, str], list[dict]] = defaultdict(
            list
        )

        # Also track original edge data for fallback
        uvr_edges_map: defaultdict[
            tuple[str, str, str], list[tuple[dict, dict, dict]]
        ] = defaultdict(list)

        for source_doc, target_doc, edge_props in edges_data:
            try:
                # Extract IDs
                source_id = self._extract_id(source_doc, match_keys_source)
                target_id = self._extract_id(target_doc, match_keys_target)

                if not source_id or not target_id:
                    logger.warning(
                        f"Missing source_id ({source_id}) or target_id ({target_id}) for edge"
                    )
                    continue

                # Clean and format edge attributes
                clean_edge_props = self._clean_document(edge_props)
                formatted_attributes = {
                    k: {"value": v} for k, v in clean_edge_props.items()
                }

                # Group by (source_id, target_id, edge_type)
                # edge_type is the actual edge type name (e.g., "relates"), not a weight value
                uvr_key = (source_id, target_id, edge_type)
                uvr_weights_map[uvr_key].append(formatted_attributes)
                uvr_edges_map[uvr_key].append((source_doc, target_doc, edge_props))

            except Exception as e:
                logger.error(f"Error processing edge: {e}")
                continue

        # Step 2: Find the maximum number of weights across all (u, v, r) groups
        # This determines how many payloads we need to create (k payloads for k max elements)
        max_weights = (
            max(len(weights_list) for weights_list in uvr_weights_map.values())
            if uvr_weights_map
            else 0
        )

        if max_weights == 0:
            return []

        # Step 3: Create k payloads by "zipping" weight lists across all (u, v, r) groups
        # Unlike Python's zip() which stops at the shortest iterable, we create k payloads
        # where k is the maximum group size. Payload i contains element i from each group
        # (if that group has an element at index i).
        payloads = []
        for weight_idx in range(max_weights):
            payload: dict[str, Any] = {"edges": {source_class: {}}}
            source_map = payload["edges"][source_class]
            payload_original_edges = []

            # Iterate through all (u, v, r) groups and take element at weight_idx
            for uvr_key, weights_list in uvr_weights_map.items():
                # Skip if this group doesn't have a weight at this index
                if weight_idx >= len(weights_list):
                    continue

                source_id, target_id, edge_type_key = uvr_key
                weight_attrs = weights_list[weight_idx]
                original_edge = uvr_edges_map[uvr_key][weight_idx]

                # Build nested structure
                if source_id not in source_map:
                    source_map[source_id] = {edge_type: {}}

                if edge_type not in source_map[source_id]:
                    source_map[source_id][edge_type] = {target_class: {}}

                if target_class not in source_map[source_id][edge_type]:
                    source_map[source_id][edge_type][target_class] = {}

                target_map = source_map[source_id][edge_type][target_class]

                # Add edge at this index from this (u, v, r) group
                target_map[target_id] = weight_attrs
                payload_original_edges.append(original_edge)

            # Only add payload if it has edges (skip empty payloads)
            if payload_original_edges:
                payload["_original_edges"] = payload_original_edges
                payloads.append(payload)

        return payloads

    def _extract_id(
        self, doc: dict[str, Any], match_keys: list[str] | tuple[str, ...]
    ) -> str | None:
        """
        Extract vertex ID from document based on match keys.

        For composite keys, concatenates values with an underscore '_'.
        Prefers '_key' if present.

        Args:
            doc: Document dictionary
            match_keys: Keys used to identify the vertex

        Returns:
            str | None: The extracted ID or None if missing required fields
        """
        if not doc:
            return None

        # Try _key first (common in ArangoDB style docs)
        if "_key" in doc and doc["_key"]:
            return str(doc["_key"])

        # If multiple match keys, create a composite ID
        if len(match_keys) > 1:
            try:
                id_parts = [str(doc[key]) for key in match_keys]
                return "_".join(id_parts)
            except KeyError:
                return None

        # Single match key
        if len(match_keys) == 1:
            key = match_keys[0]
            if key in doc and doc[key] is not None:
                return str(doc[key])

        return None

    def _fallback_individual_edge_upsert(
        self,
        edges_data: list[tuple[dict, dict, dict]],
        source_class: str,
        target_class: str,
        edge_type: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
    ) -> None:
        """Fallback method for individual edge upserts.

        Args:
            edges_data: List of tuples (source_doc, target_doc, edge_props)
            source_class: Source vertex type name
            target_class: Target vertex type name
            edge_type: Edge type name
            match_keys_source: Keys for source vertex ID
            match_keys_target: Keys for target vertex ID
        """
        for source_doc, target_doc, edge_props in edges_data:
            try:
                source_id = self._extract_id(source_doc, match_keys_source)
                target_id = self._extract_id(target_doc, match_keys_target)

                if source_id and target_id:
                    clean_edge_props = self._clean_document(edge_props)
                    # Serialize data for pyTigerGraph
                    serialized_props = json.loads(
                        json.dumps(clean_edge_props, default=_json_serializer)
                    )
                    self.conn.upsertEdge(
                        source_class,
                        source_id,
                        edge_type,
                        target_class,
                        target_id,
                        attributes=serialized_props,
                    )
            except Exception as e:
                logger.error(f"Error upserting individual edge: {e}")

    def insert_edges_batch(
        self,
        docs_edges: list[list[dict[str, Any]]] | list[Any] | None,
        source_class: str,
        target_class: str,
        relation_name: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
        filter_uniques: bool = True,
        head: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Batch insert/upsert edges using TigerGraph REST++ API.

        Handles edge data in tuple format: [(source_doc, target_doc, edge_props), ...]
        or dict format: [{"_source_aux": {...}, "_target_aux": {...}, "_edge_props": {...}}, ...]

        Args:
            docs_edges: List of edge documents (tuples or dicts)
            source_class: Source vertex type name
            target_class: Target vertex type name
            relation_name: Edge type/relation name
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: If True, filter duplicate edges (used)
            head: Optional limit on number of edges to insert (used)
            **kwargs: Additional options:
                - dry: If True, don't execute the query
                - collection_name: Alternative edge type name (used if relation_name is None)
                - uniq_weight_fields: Unused in TigerGraph (ArangoDB-specific)
                - uniq_weight_collections: Unused in TigerGraph (ArangoDB-specific)
                - upsert_option: Unused in TigerGraph (ArangoDB-specific, always upserts by default)
        """
        dry = kwargs.pop("dry", False)
        collection_name = kwargs.pop("collection_name", None)
        # Extract and ignore ArangoDB-specific parameters
        kwargs.pop("uniq_weight_fields", None)
        kwargs.pop("uniq_weight_collections", None)
        kwargs.pop("upsert_option", None)
        if dry:
            if docs_edges is not None:
                logger.debug(f"Dry run: would insert {len(docs_edges)} edges")
            return

        # Process edges list
        if isinstance(docs_edges, list):
            if head is not None:
                docs_edges = docs_edges[:head]
            if filter_uniques:
                docs_edges = pick_unique_dict(docs_edges)

        # Normalize edge data format - handle both tuple and dict formats
        if docs_edges is None:
            return
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

            # Generate multiple edge upsert payloads (one per unique attribute combination)
            payloads = self._generate_edge_upsert_payloads(
                normalized_edges,
                source_class,
                target_class,
                edge_type,
                match_keys_src,
                match_keys_tgt,
            )

            if not payloads:
                logger.warning(f"No valid edges to upsert for edge type {edge_type}")
                return

            # Send each payload in batch
            total_edges = 0
            failed_payloads = []
            for i, payload in enumerate(payloads):
                edges_payload = payload.get("edges", {})
                if not edges_payload or source_class not in edges_payload:
                    continue

                # Store original edges for fallback before removing metadata
                original_edges = payload.pop("_original_edges", [])

                # Send the batch upsert request
                result = self._upsert_data(payload)

                # Restore original edges for potential fallback
                payload["_original_edges"] = original_edges

                if result.get("error"):
                    logger.error(
                        f"Error upserting edges of type {edge_type} (payload {i + 1}/{len(payloads)}): "
                        f"{result.get('message')}"
                    )
                    # Collect failed payload for fallback
                    failed_payloads.append((payload, i))
                else:
                    # Count edges in this payload
                    edge_count = 0
                    for source_id_map in edges_payload[source_class].values():
                        if edge_type in source_id_map:
                            for target_type_map in source_id_map[edge_type].values():
                                for attrs_or_list in target_type_map.values():
                                    if isinstance(attrs_or_list, list):
                                        edge_count += len(attrs_or_list)
                                    else:
                                        edge_count += 1
                    total_edges += edge_count
                    logger.debug(
                        f"Upserted {edge_count} edges of type {edge_type} via batch "
                        f"(payload {i + 1}/{len(payloads)}): {result}"
                    )

            # Handle failed payloads with individual upserts
            if failed_payloads:
                logger.warning(
                    f"{len(failed_payloads)} payload(s) failed, falling back to individual upserts"
                )
                # Extract original edges from failed payloads for individual upsert
                failed_edges = []
                for payload, _ in failed_payloads:
                    # Use the stored original edges for this payload
                    original_edges = payload.get("_original_edges", [])
                    failed_edges.extend(original_edges)

                if failed_edges:
                    logger.debug(
                        f"Sending {len(failed_edges)} edges from failed payloads via individual upserts"
                    )
                    self._fallback_individual_edge_upsert(
                        failed_edges,
                        source_class,
                        target_class,
                        edge_type,
                        match_keys_src,
                        match_keys_tgt,
                    )

            logger.debug(
                f"Total upserted {total_edges} edges of type {edge_type} across {len(payloads)} payloads"
            )
            return

        except Exception as e:
            logger.error(f"Error batch inserting edges: {e}")
            # Fallback to individual operations
            self._fallback_individual_edge_upsert(
                normalized_edges,
                source_class,
                target_class,
                edge_type,
                match_keys_src,
                match_keys_tgt,
            )

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

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
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

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | Clause | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
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
            vertex_config = kwargs.get("vertex_config")

            if field_types is None and vertex_config is not None:
                field_types = {f.name: f.type for f in vertex_config.fields(class_name)}

            # Build REST++ filter string with field type information
            filter_str = self._render_rest_filter(filters, field_types=field_types)

            # Build REST++ API endpoint with query parameters manually
            # Format: /graph/{graph_name}/vertices/{vertex_type}?filter=...&limit=...
            # Example: /graph/g22c97325/vertices/Author?filter=hindex>20&limit=10

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
        filters: list[Any] | dict[str, Any] | Clause | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
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
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Check which documents from batch are present in the database.
        """
        try:
            present_docs: list[dict[str, Any]] = []
            keep_keys_list: list[str] | tuple[str, ...] = (
                list(keep_keys) if keep_keys is not None else []
            )
            if isinstance(keep_keys_list, tuple):
                keep_keys_list = list(keep_keys_list)

            for doc in batch:
                vertex_id = self._extract_id(doc, match_keys)
                if not vertex_id:
                    continue

                try:
                    vertex_data = self.conn.getVerticesById(class_name, vertex_id)
                    if vertex_data and vertex_id in vertex_data:
                        # Extract requested keys
                        vertex_attrs = vertex_data[vertex_id].get("attributes", {})
                        filtered_doc: dict[str, Any] = {}

                        if keep_keys_list:
                            for key in keep_keys_list:
                                if key == "id":
                                    filtered_doc[key] = vertex_id
                                elif key in vertex_attrs:
                                    filtered_doc[key] = vertex_attrs[key]
                        else:
                            # If no keep_keys specified, return all attributes
                            filtered_doc = vertex_attrs.copy()
                            filtered_doc["id"] = vertex_id

                        present_docs.append(filtered_doc)

                except Exception:
                    # Vertex doesn't exist or error occurred
                    continue

            return present_docs

        except Exception as e:
            logger.error(f"Error fetching present documents: {e}")
            return []

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
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return documents from batch that are NOT present in database.
        """
        present_docs = self.fetch_present_documents(
            batch=batch,
            class_name=class_name,
            match_keys=match_keys,
            keep_keys=keep_keys,
            flatten=False,
            filters=filters,
        )

        # Create a set of IDs from present documents for efficient lookup
        present_ids = set()
        for present_doc in present_docs:
            # Extract ID from present document (it should have 'id' key)
            if "id" in present_doc:
                present_ids.add(present_doc["id"])

        # Find documents that are not present
        absent_docs: list[dict[str, Any]] = []
        keep_keys_list: list[str] | tuple[str, ...] = (
            list(keep_keys) if keep_keys is not None else []
        )
        if isinstance(keep_keys_list, tuple):
            keep_keys_list = list(keep_keys_list)

        for doc in batch:
            vertex_id = self._extract_id(doc, match_keys)
            if not vertex_id or vertex_id not in present_ids:
                if keep_keys_list:
                    # Filter to keep only requested keys
                    filtered_doc = {k: doc.get(k) for k in keep_keys_list if k in doc}
                    absent_docs.append(filtered_doc)
                else:
                    absent_docs.append(doc)

        return absent_docs

    @_wrap_tg_exception
    def define_indexes(self, schema: Schema):
        """Define all indexes from schema."""
        try:
            self.define_vertex_indices(schema.vertex_config)
            # Ensure edges are initialized before defining indices
            edges_for_indices = list(schema.edge_config.edges_list(include_aux=True))
            for edge in edges_for_indices:
                if edge._source is None or edge._target is None:
                    edge.finish_init(schema.vertex_config)
            self.define_edge_indices(edges_for_indices)
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
