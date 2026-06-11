"""TigerGraph REST++ API client."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import requests
from requests import exceptions as requests_exceptions

from graflo.db.tigergraph.document_utils import (
    json_serializer_alias as _json_serializer,
)
from graflo.db.tigergraph.token_cache import _TigerGraphTokenCache

if TYPE_CHECKING:
    from graflo.db.tigergraph.conn import TigerGraphConnection

logger = logging.getLogger(__name__)


class TigerGraphRestClient:
    def __init__(self, conn: TigerGraphConnection) -> None:
        self._conn = conn

    def _upsert_vertex(
        self,
        vertex_type: str,
        vertex_id: str,
        attributes: dict[str, Any],
        graph_name: str | None = None,
    ) -> dict[str, Any] | list[dict]:
        """
        Upsert a single vertex using REST API.

        Args:
            vertex_type: Vertex type name
            vertex_id: Vertex ID
            attributes: Vertex attributes
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            Response from API
        """
        graph_name = graph_name or self._conn.graphname
        endpoint = f"/graph/{graph_name}/vertices/{vertex_type}/{quote(str(vertex_id))}"
        return self._call_restpp_api(endpoint, method="POST", data=attributes)

    def _upsert_edge(
        self,
        source_type: str,
        source_id: str,
        edge_type: str,
        target_type: str,
        target_id: str,
        attributes: dict[str, Any] | None = None,
        graph_name: str | None = None,
    ) -> dict[str, Any] | list[dict]:
        """
        Upsert a single edge using REST API.

        Args:
            source_type: Source vertex type
            source_id: Source vertex ID
            edge_type: Edge type name
            target_type: Target vertex type
            target_id: Target vertex ID
            attributes: Edge attributes (optional)
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            Response from API
        """
        graph_name = graph_name or self._conn.graphname
        # TigerGraph 4.2+: .../edges/{source_type}/{source_id}/{edge_type}/{target_type}/{target_id}
        endpoint = (
            f"/graph/{graph_name}/edges/"
            f"{source_type}/{quote(str(source_id))}/"
            f"{edge_type}/"
            f"{target_type}/{quote(str(target_id))}"
        )
        data = attributes if attributes else {}
        return self._call_restpp_api(endpoint, method="POST", data=data)

    def _get_edges(
        self,
        source_type: str,
        source_id: str,
        edge_type: str | None = None,
        graph_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get edges from a vertex using REST API.

        Based on pyTigerGraph's getEdges() implementation.
        Uses GET /graph/{graph}/edges/{source_vertex_type}/{source_vertex_id} endpoint.

        Args:
            source_type: Source vertex type
            source_id: Source vertex ID
            edge_type: Edge type to filter by (optional, filtered client-side)
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            List of edge dictionaries
        """
        graph_name = graph_name or self._conn.graphname

        # Use the correct endpoint format matching pyTigerGraph's _prep_get_edges:
        # GET /graph/{graph}/edges/{source_type}/{source_id}
        # If edge_type is specified, append it: /graph/{graph}/edges/{source_type}/{source_id}/{edge_type}
        if edge_type:
            endpoint = f"/graph/{graph_name}/edges/{source_type}/{quote(str(source_id))}/{edge_type}"
        else:
            endpoint = (
                f"/graph/{graph_name}/edges/{source_type}/{quote(str(source_id))}"
            )

        result = self._call_restpp_api(endpoint, method="GET")

        # Parse REST++ API response format
        # Response format: {"version": {...}, "error": false, "message": "", "results": [...]}
        if isinstance(result, dict):
            # Check for error first
            if result.get("error") is True:
                error_msg = result.get("message", "Unknown error")
                logger.error(f"Error fetching edges: {error_msg}")
                return []

            # Extract results array
            if "results" in result:
                edges = result["results"]
            else:
                logger.debug(
                    f"Unexpected response format from edges endpoint: {result.keys()}"
                )
                return []
        elif isinstance(result, list):
            edges = result
        else:
            logger.debug(
                f"Unexpected response type from edges endpoint: {type(result)}"
            )
            return []

        # Filter by edge_type if specified (client-side filtering)
        # REST API endpoint doesn't support edge_type filtering directly
        if edge_type and isinstance(edges, list):
            edges = [
                e for e in edges if isinstance(e, dict) and e.get("e_type") == edge_type
            ]

        return edges

    def _get_vertices_by_id(
        self, vertex_type: str, vertex_id: str, graph_name: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """
        Get vertex by ID using REST API.

        Args:
            vertex_type: Vertex type name
            vertex_id: Vertex ID
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            Dictionary mapping vertex_id to vertex data
        """
        graph_name = graph_name or self._conn.graphname
        endpoint = f"/graph/{graph_name}/vertices/{vertex_type}/{quote(str(vertex_id))}"
        result = self._call_restpp_api(endpoint, method="GET")
        # Parse response format to match expected format
        # Returns {vertex_id: {"attributes": {...}}}
        if isinstance(result, dict):
            if "results" in result:
                # REST API format
                results = result["results"]
                if results and isinstance(results, list) and len(results) > 0:
                    vertex_data = results[0]
                    return {
                        vertex_id: {"attributes": vertex_data.get("attributes", {})}
                    }
            elif vertex_id in result:
                return {vertex_id: result[vertex_id]}
            else:
                # Try to extract vertex data
                return {vertex_id: {"attributes": result.get("attributes", {})}}
        return {}

    def _get_vertex_count(self, vertex_type: str, graph_name: str | None = None) -> int:
        """
        Get vertex count using REST API.

        Args:
            vertex_type: Vertex type name
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            Number of vertices
        """
        graph_name = graph_name or self._conn.graphname
        endpoint = f"/graph/{graph_name}/vertices/{vertex_type}"
        params = {"limit": "1", "count": "true"}
        result = self._call_restpp_api(endpoint, method="GET", params=params)
        # Parse count from response
        if isinstance(result, dict):
            return result.get("count", 0)
        return 0

    def _delete_vertices(
        self, vertex_type: str, where: str | None = None, graph_name: str | None = None
    ) -> dict[str, Any] | list[dict]:
        """
        Delete vertices using REST API.

        Args:
            vertex_type: Vertex type name
            where: WHERE clause for filtering (optional)
            graph_name: Name of the graph (defaults to self._conn.graphname)

        Returns:
            Response from API
        """
        graph_name = graph_name or self._conn.graphname
        endpoint = f"/graph/{graph_name}/vertices/{vertex_type}"
        params = {}
        if where:
            params["filter"] = where
        return self._call_restpp_api(endpoint, method="DELETE", params=params)

    def _call_restpp_api(
        self,
        endpoint: str,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        *,
        use_basic_auth: bool = False,
    ) -> dict[str, Any] | list[dict]:
        """Call TigerGraph REST++ API endpoint.

        Args:
            endpoint: REST++ API endpoint (e.g., "/graph/{graph_name}/vertices/{vertex_type}")
            method: HTTP method (GET, POST, etc.)
            data: Optional data to send in request body (for POST)
            params: Optional query parameters
            use_basic_auth: When True, use username/password instead of Bearer token

        Returns:
            Response data (dict or list)
        """
        url = f"{self._conn.restpp_url}{endpoint}"

        headers = {
            "Content-Type": "application/json",
            **self._conn._get_auth_headers(use_basic_auth=use_basic_auth),
        }

        logger.debug(f"REST++ API call: {method} {url}")

        try:
            if method.upper() == "GET":
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=120,
                    verify=self._conn.ssl_verify,
                )
            elif method.upper() == "POST":
                response = requests.post(
                    url,
                    headers=headers,
                    data=json.dumps(data, default=_json_serializer) if data else None,
                    params=params,
                    timeout=120,
                    verify=self._conn.ssl_verify,
                )
            elif method.upper() == "DELETE":
                response = requests.delete(
                    url,
                    headers=headers,
                    params=params,
                    timeout=120,
                    verify=self._conn.ssl_verify,
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests_exceptions.HTTPError as errh:
            if (
                errh.response.status_code == 401
                and self._conn.api_token
                and self._conn._token_cache_key
            ):
                _TigerGraphTokenCache.instance().invalidate(self._conn._token_cache_key)

            # For TigerGraph 4.2.1, if token auth fails with 401/REST-10018, try Basic Auth fallback
            if (
                errh.response.status_code == 401
                and self._conn.api_token
                and self._conn.config.username
                and self._conn.config.password
                and "REST-10018" in str(errh)
            ):
                logger.warning(
                    "Token authentication failed with REST-10018, "
                    "falling back to Basic Auth for TigerGraph 4.2.1 compatibility"
                )
                # Retry with Basic Auth
                import base64

                credentials = (
                    f"{self._conn.config.username}:{self._conn.config.password}"
                )
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded_credentials}"
                try:
                    if method.upper() == "GET":
                        response = requests.get(
                            url,
                            headers=headers,
                            params=params,
                            timeout=120,
                            verify=self._conn.ssl_verify,
                        )
                    elif method.upper() == "POST":
                        response = requests.post(
                            url,
                            headers=headers,
                            data=json.dumps(data, default=_json_serializer)
                            if data
                            else None,
                            params=params,
                            timeout=120,
                            verify=self._conn.ssl_verify,
                        )
                    elif method.upper() == "DELETE":
                        response = requests.delete(
                            url,
                            headers=headers,
                            params=params,
                            timeout=120,
                            verify=self._conn.ssl_verify,
                        )
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")
                    response.raise_for_status()
                    logger.info("Successfully authenticated using Basic Auth fallback")
                    return response.json()
                except requests_exceptions.HTTPError as errh2:
                    logger.error(f"HTTP Error (after Basic Auth fallback): {errh2}")
                    error_response = {"error": True, "message": str(errh2)}
                    try:
                        error_json = response.json()
                        if isinstance(error_json, dict):
                            error_response.update(error_json)
                        else:
                            error_response["details"] = response.text
                    except Exception:
                        error_response["details"] = response.text
                    return error_response

            logger.error(f"HTTP Error: {errh}")
            error_response = {"error": True, "message": str(errh)}
            err_response = errh.response
            if err_response is not None:
                try:
                    error_json = err_response.json()
                    if isinstance(error_json, dict):
                        error_response.update(error_json)
                    else:
                        error_response["details"] = err_response.text
                except Exception:
                    error_response["details"] = err_response.text
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
