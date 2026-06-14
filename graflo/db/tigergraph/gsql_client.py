"""TigerGraph GSQL execution and catalog operations."""

from __future__ import annotations

import hashlib
import logging
import re
from typing import TYPE_CHECKING, Any

import requests
from requests import exceptions as requests_exceptions

from graflo.architecture.schema import Schema
from graflo.db.tigergraph.gsql_parsers import (
    gsql_result_has_error,
    is_missing_query_endpoint_error,
    parse_installed_queries_from_ls,
    parse_installed_queries_from_show_query,
    parse_show_edge_output_with_vertices,
    parse_show_job_output,
    parse_show_output,
)
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


class TigerGraphGsqlClient:
    def __init__(self, conn: TigerGraphConnection) -> None:
        self._conn = conn

    def _execute_gsql(self, gsql_command: str) -> str:
        """
        Execute GSQL command using REST API.

        For TigerGraph 4.0-4.2.1, uses POST /gsql/v1/statements endpoint.

        Note: GSQL endpoints require Basic Auth (username/password), not Bearer tokens.

        Args:
            gsql_command: GSQL command string to execute

        Returns:
            Response string from GSQL execution
        """
        url = f"{self._conn.gsql_url}/gsql/v1/statements"
        auth_headers = self._conn._get_auth_headers(use_basic_auth=True)
        headers = {
            "Content-Type": "text/plain",
            **auth_headers,
        }

        # Debug: Log if Authorization header is missing
        if "Authorization" not in headers:
            logger.error(
                f"No Authorization header generated. "
                f"Username: {self._conn.config.username}, Password: {'***' if self._conn.config.password else None}"
            )

        try:
            response = requests.post(
                url,
                headers=headers,
                data=gsql_command,
                timeout=120,
                verify=self._conn.ssl_verify,
            )
            response.raise_for_status()

            # Try to parse JSON response, fallback to text
            try:
                result = response.json()
                # Extract message or result from JSON response
                if isinstance(result, dict):
                    return result.get("message", str(result))
                return str(result)
            except ValueError:
                # Not JSON, return text
                return response.text
        except requests_exceptions.HTTPError as e:
            error_msg = str(e)
            # Try to extract error message from response
            try:
                error_details = e.response.json() if e.response else {}
                error_msg = error_details.get("message", error_msg)
            except Exception:
                pass
            raise RuntimeError(f"GSQL execution failed: {error_msg}") from e

    def _get_vertex_types(self, graph_name: str | None = None) -> list[str]:
        """
        Get list of vertex types using GSQL.

        Args:
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            List of vertex type names
        """
        graph_name = graph_name or self._conn.graphname
        try:
            result = self._conn._execute_gsql(f"USE GRAPH {graph_name}\nSHOW VERTEX *")
            # Parse GSQL output using the proper parser
            if isinstance(result, str):
                return parse_show_output(result, "VERTEX")
            return []
        except Exception as e:
            logger.debug(f"Failed to get vertex types via GSQL: {e}")
            return []

    def _get_edge_types(
        self, graph_name: str | None = None
    ) -> dict[str, list[tuple[str, str]]]:
        """
        Get edge types and their (source, target) vertex pairs using GSQL.

        Args:
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            Dict mapping edge_type -> list of (source_vertex, target_vertex)
        """
        graph_name = graph_name or self._conn.graphname
        try:
            result = self._conn._execute_gsql(f"USE GRAPH {graph_name}\nSHOW EDGE *")

            if isinstance(result, str):
                return parse_show_edge_output_with_vertices(result)

            return {}

        except Exception as e:
            logger.error(f"Failed to get edge types via GSQL: {e}")
            return {}

    def _get_installed_queries_via_gsql(self, graph_name: str) -> list[str] | None:
        """Discover installed queries via GSQL catalog commands.

        Returns:
            List of installed query names, or ``None`` when GSQL discovery fails.
        """
        try:
            ls_output = self._conn._execute_gsql(f"USE GRAPH {graph_name}\nls")
            ls_output_str = str(ls_output)
            if gsql_result_has_error(ls_output_str):
                logger.debug(
                    f"GSQL ls failed for graph '{graph_name}': {ls_output_str}"
                )
                return None

            queries = parse_installed_queries_from_ls(ls_output_str)
            if queries:
                return queries
        except Exception as e:
            logger.debug(f"GSQL ls failed for graph '{graph_name}': {e}")
            return None

        try:
            show_output = self._conn._execute_gsql(
                f"USE GRAPH {graph_name}\nSHOW QUERY *"
            )
            show_output_str = str(show_output)
            if gsql_result_has_error(show_output_str):
                logger.debug(
                    f"GSQL SHOW QUERY * failed for graph '{graph_name}': {show_output_str}"
                )
                return None

            return parse_installed_queries_from_show_query(show_output_str)
        except Exception as e:
            logger.debug(f"GSQL SHOW QUERY * failed for graph '{graph_name}': {e}")
            return None

    def _get_installed_queries(self, graph_name: str | None = None) -> list[str]:
        """Return installed query names for a graph.

        Uses GSQL catalog commands (same auth path as schema operations).

        Args:
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            List of installed query names (empty when none are installed or discovery fails)
        """
        graph_name = graph_name or self._conn.graphname
        gsql_queries = self._get_installed_queries_via_gsql(graph_name)
        if gsql_queries is not None:
            return gsql_queries

        logger.warning(
            "Could not discover installed queries for graph '%s' via GSQL; "
            "treating as empty",
            graph_name,
        )
        return []

    def _drop_installed_queries_for_graph(self, graph_name: str) -> None:
        """Drop all installed queries that belong to the provided graph.

        Uses GSQL ``DROP QUERY *`` as the primary mechanism — this removes every
        installed query in the graph in one shot and does not require prior
        discovery.  The REST-API-based individual-drop path runs afterwards as
        a best-effort cleanup for any stragglers.

        TigerGraph will not DROP GRAPH while installed queries exist; this step
        must succeed before the graph can be removed.
        """
        # Primary: bulk drop via GSQL — works regardless of what the REST API reports.
        try:
            self._conn._execute_gsql(f"USE GRAPH {graph_name}\nDROP QUERY *")
            logger.debug(f"Bulk-dropped all queries from graph '{graph_name}'")
        except Exception as e:
            logger.debug(
                f"Bulk DROP QUERY * for graph '{graph_name}' failed (may have no queries): {e}"
            )

        # Secondary: REST-API discovery + individual drops for any stragglers.
        queries = self._conn._get_installed_queries(graph_name=graph_name)
        if queries:
            logger.info(
                f"Dropping {len(queries)} remaining queries from graph '{graph_name}'"
            )
            for query_name in queries:
                try:
                    self._conn._execute_gsql(
                        f"USE GRAPH {graph_name}\nDROP QUERY {query_name} IF EXISTS"
                    )
                    logger.debug(
                        f"Dropped query '{query_name}' from graph '{graph_name}'"
                    )
                except Exception:
                    try:
                        self._conn._execute_gsql(
                            f"USE GRAPH {graph_name}\nDROP QUERY {query_name}"
                        )
                    except Exception as query_error:
                        logger.debug(
                            f"Could not drop query '{query_name}' from graph "
                            f"'{graph_name}': {query_error}"
                        )

        self._conn._installed_clear_data_queries.pop(graph_name, None)

    def _drop_global_schema_types(
        self, schema: "Schema", surviving_graphs: list[str]
    ) -> None:
        """Drop global vertex and edge types that belong to *schema*.

        TigerGraph stores vertex/edge types globally.  When a graph is dropped
        the types may linger as orphans and block subsequent schema creation for
        a graph with the same name.  This method cleans them up idempotently.

        Types that still appear in *surviving_graphs* (other graphs on the
        server) are **not** dropped: a global ``DROP VERTEX`` / ``DROP EDGE``
        can cascade-invalidate installed queries in unrelated graphs.

        Order: edges first (they depend on vertices), then vertices.
        """
        in_use_vertices: set[str] = set()
        in_use_edges: set[str] = set()
        for g in surviving_graphs:
            verts, edges = self._conn._get_graph_type_names(g)
            in_use_vertices |= verts
            in_use_edges |= edges

        db_schema = schema.resolve_db_aware(DBType.TIGERGRAPH)
        edge_config = schema.core_schema.edge_config

        # Collect unique edge relation names
        edge_names: set[str] = set()
        for edge in edge_config.values():
            runtime = db_schema.edge_config.runtime(edge)
            rel = runtime.relation_name or f"{edge.source}_{edge.target}"
            if rel:
                edge_names.add(rel)

        for edge_name in edge_names:
            if edge_name in in_use_edges:
                logger.warning(
                    f"Skipping DROP EDGE '{edge_name}' — still referenced by "
                    "surviving graphs"
                )
                continue
            try:
                result = self._conn._execute_gsql(f"DROP EDGE {edge_name}")
                logger.warning(f"Dropped global edge type '{edge_name}': {result}")
            except Exception as e:
                logger.debug(
                    f"Could not drop global edge type '{edge_name}' "
                    f"(may not exist or still referenced): {e}"
                )

        # Collect unique vertex db-names
        vertex_names: set[str] = set()
        for vertex in schema.core_schema.vertex_config.vertices:
            try:
                dbname = db_schema.vertex_config.vertex_dbname(vertex.name)
                vertex_names.add(dbname if dbname else vertex.name)
            except Exception:
                vertex_names.add(vertex.name)

        for vertex_name in vertex_names:
            if vertex_name in in_use_vertices:
                logger.warning(
                    f"Skipping DROP VERTEX '{vertex_name}' — still referenced by "
                    "surviving graphs"
                )
                continue
            try:
                result = self._conn._execute_gsql(f"DROP VERTEX {vertex_name}")
                logger.warning(f"Dropped global vertex type '{vertex_name}': {result}")
            except Exception as e:
                logger.debug(
                    f"Could not drop global vertex type '{vertex_name}' "
                    f"(may not exist or still referenced): {e}"
                )

    def _drop_jobs_for_graph(self, graph_name: str) -> None:
        """Drop jobs visible in the given graph context."""
        try:
            result = self._conn._execute_gsql(f"USE GRAPH {graph_name}\nSHOW JOB *")
        except Exception as e:
            logger.debug(f"Could not list jobs for graph '{graph_name}': {e}")
            return

        job_names = parse_show_job_output(str(result))
        if not job_names:
            logger.debug(f"No jobs found for graph '{graph_name}'")
            return

        logger.info(f"Dropping {len(job_names)} jobs from graph '{graph_name}'")
        for job_name in job_names:
            try:
                self._conn._execute_gsql(f"USE GRAPH {graph_name}\nDROP JOB {job_name}")
                logger.debug(f"Dropped job '{job_name}' from graph '{graph_name}'")
            except Exception as e:
                logger.debug(
                    f"Could not drop job '{job_name}' from graph '{graph_name}': {e}"
                )

    def _run_installed_query(
        self, query_name: str, graph_name: str | None = None, **kwargs: Any
    ) -> dict[str, Any] | list[dict]:
        """
        Run an installed query using REST API.

        Args:
            query_name: Name of the installed query
            graph_name: Name of the graph (defaults to self._conn.graphname)
            **kwargs: Query parameters

        Returns:
            Query result (dict or list)
        """
        graph_name = graph_name or self._conn.graphname
        endpoint = f"/query/{graph_name}/{query_name}"
        result = self._conn._call_restpp_api(endpoint, method="POST", data=kwargs)
        if (
            isinstance(result, dict)
            and result.get("error") is True
            and is_missing_query_endpoint_error(result)
        ):
            # Some TigerGraph environments expose installed query endpoints as GET-only.
            return self._conn._call_restpp_api(endpoint, method="GET", params=kwargs)
        return result

    def _build_clear_data_query_name(self, vertex_types: tuple[str, ...]) -> str:
        """Build a stable, schema-aware query name for clear-data operations."""
        signature = "|".join(sorted(vertex_types))
        digest = hashlib.sha1(signature.encode("utf-8")).hexdigest()[:12]
        return f"graflo_clear_data_{digest}"

    def _install_clear_data_query(
        self, graph_name: str, query_name: str, vertex_types: tuple[str, ...]
    ) -> None:
        """Create and install a pre-compiled query that deletes all schema vertex types."""
        delete_stmts = "\n".join(
            f"  DELETE FROM {vertex_type};" for vertex_type in sorted(vertex_types)
        )
        create_query = "\n".join(
            [
                f"USE GRAPH {graph_name}",
                f"CREATE QUERY {query_name}() FOR GRAPH {graph_name} {{",
                delete_stmts,
                "}",
            ]
        )
        install_query = "\n".join(
            [
                f"USE GRAPH {graph_name}",
                f"INSTALL QUERY {query_name}",
            ]
        )
        self._conn._execute_gsql(create_query)
        self._conn._execute_gsql(install_query)

    def _clear_data_via_installed_query(
        self, graph_name: str, vertex_types: tuple[str, ...]
    ) -> None:
        """Run clear-data through an installed GSQL query for faster cluster cleanup."""
        query_name = self._build_clear_data_query_name(vertex_types)
        installed_queries = self._conn._installed_clear_data_queries.get(graph_name)
        if installed_queries is None:
            installed_queries = set(
                self._conn._get_installed_queries(graph_name=graph_name)
            )
            self._conn._installed_clear_data_queries[graph_name] = installed_queries
        if query_name not in installed_queries:
            self._install_clear_data_query(
                graph_name=graph_name,
                query_name=query_name,
                vertex_types=vertex_types,
            )
            installed_queries.add(query_name)

        try:
            result = self._conn._execute_gsql(
                f"USE GRAPH {graph_name}\nRUN QUERY {query_name}()"
            )
        except Exception as run_error:
            raise RuntimeError(
                f"Installed clear_data query '{query_name}' failed: {run_error}"
            ) from run_error

        result_text = str(result).lower()
        if "error" in result_text or "failed" in result_text:
            raise RuntimeError(
                f"Installed clear_data query '{query_name}' failed: {result}"
            )

    def _get_version(self) -> str | None:
        """
        Get TigerGraph version using REST API.

        Tries multiple endpoints in order:
        1. GET /gsql/v1/version (GSQL server, port 14240) - primary for TG 4+
        2. GET /version (REST++ server, port 9000) - fallback for older versions

        Note: The /version endpoint does NOT exist on GSQL port (14240).
        It only exists on REST++ port (9000) for older versions.

        Returns:
            Version string (e.g., "4.2.1") or None if detection fails
        """

        if self._conn.config.gs_port is None:
            raise ValueError("gs_port must be set in config for version detection")

        # Try GSQL endpoint first (primary for TigerGraph 4+)
        # Note: /gsql/v1/version exists on GSQL port, but /version does NOT
        # Response format: plain text like "GSQL version: 4.2.2\n"
        gsql_url = f"{self._conn.gsql_url}/gsql/v1/version"
        headers = self._conn._get_auth_headers(use_basic_auth=True)

        try:
            response = requests.get(
                gsql_url, headers=headers, timeout=10, verify=self._conn.ssl_verify
            )
            response.raise_for_status()

            if not response.text.strip():
                # Empty response
                logger.debug("GSQL version endpoint returned empty response")
                raise ValueError("Empty response from GSQL version endpoint")

            # GSQL /gsql/v1/version returns plain text, not JSON
            # Format: "GSQL version: 4.2.2\n" or similar
            response_text = response.text.strip()

            # Try to parse version from text response
            # Format: "GSQL version: 4.2.2" or "version: 4.2.2" or "4.2.2"
            version_match = re.search(
                r"version:\s*(\d+)\.(\d+)\.(\d+)", response_text, re.IGNORECASE
            )
            if version_match:
                version_str = f"{version_match.group(1)}.{version_match.group(2)}.{version_match.group(3)}"
                logger.debug(
                    f"Detected TigerGraph version: {version_str} from GSQL endpoint (text format)"
                )
                return version_str

            # Try alternative: just look for version number pattern
            version_match = re.search(r"(\d+)\.(\d+)\.(\d+)", response_text)
            if version_match:
                version_str = f"{version_match.group(1)}.{version_match.group(2)}.{version_match.group(3)}"
                logger.debug(
                    f"Detected TigerGraph version: {version_str} from GSQL endpoint (text format)"
                )
                return version_str

            # If text parsing failed, try JSON as fallback (some versions might return JSON)
            try:
                result = response.json()
                message = result.get("message", "")
                if message:
                    version_match = re.search(r"release_(\d+)\.(\d+)\.(\d+)", message)
                    if version_match:
                        version_str = f"{version_match.group(1)}.{version_match.group(2)}.{version_match.group(3)}"
                        logger.debug(
                            f"Detected TigerGraph version: {version_str} from GSQL endpoint (JSON format)"
                        )
                        return version_str
            except ValueError:
                # Not JSON, that's fine - we already tried text parsing
                pass

        except Exception as e:
            logger.debug(f"Failed to get version from GSQL endpoint: {e}")

        # Fallback: Try REST++ /version endpoint (for older versions or if GSQL endpoint fails)
        # Note: /version only exists on REST++ port (9000), not GSQL port (14240)
        try:
            # Use REST++ port if different from GSQL port
            restpp_port = (
                self._conn.config.port
                if self._conn.config.port
                else self._conn.config.gs_port
            )
            if restpp_port is None:
                return None

            restpp_url = f"{self._conn.config.url_without_port}:{restpp_port}/version"
            headers = self._conn._get_auth_headers(use_basic_auth=True)

            response = requests.get(
                restpp_url, headers=headers, timeout=10, verify=self._conn.ssl_verify
            )
            response.raise_for_status()

            # Check content type and response
            if not response.text.strip():
                logger.debug("REST++ version endpoint returned empty response")
                return None

            try:
                result = response.json()
            except ValueError:
                logger.debug(
                    f"REST++ version endpoint returned non-JSON response: "
                    f"status={response.status_code}, text={response.text[:200]}"
                )
                return None

            # Parse version from REST++ response
            message = result.get("message", "")
            if message:
                version_match = re.search(r"release_(\d+)\.(\d+)\.(\d+)", message)
                if version_match:
                    version_str = f"{version_match.group(1)}.{version_match.group(2)}.{version_match.group(3)}"
                    logger.debug(
                        f"Detected TigerGraph version: {version_str} from REST++ endpoint"
                    )
                    return version_str

        except Exception as e:
            logger.debug(f"Failed to get version from REST++ endpoint: {e}")

        return None
