"""TigerGraph authentication helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from graflo.db.tigergraph.conn import TigerGraphConnection

logger = logging.getLogger(__name__)


class TigerGraphAuth:
    def __init__(self, conn: TigerGraphConnection) -> None:
        self._conn = conn

    def _get_auth_headers(self, use_basic_auth: bool = False) -> dict[str, str]:
        """Get authentication headers for REST API calls.

        Args:
            use_basic_auth: If True, always use Basic Auth (required for GSQL endpoints).
                           If False, prioritize token-based auth for REST++ endpoints.

        Prioritizes token-based authentication over Basic Auth for REST++ endpoints:
        1. If API token is available (from secret), use Bearer token (recommended for TG 4+)
        2. Otherwise, fall back to HTTP Basic Auth with username/password

        For GSQL endpoints, always use Basic Auth as they don't support Bearer tokens.

        Returns:
            Dictionary with Authorization header
        """
        headers = {}

        # GSQL endpoints require Basic Auth, not Bearer tokens
        if use_basic_auth or not self._conn.api_token:
            # Use default username "tigergraph" if username is None but password is set
            username = (
                self._conn.config.username
                if self._conn.config.username
                else "tigergraph"
            )
            password = self._conn.config.password

            if password:
                import base64

                credentials = f"{username}:{password}"
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded_credentials}"
            else:
                logger.warning(
                    f"No password configured for Basic Auth. "
                    f"Username: {username}, Password: {password}"
                )
        else:
            # Use Bearer token for REST++ endpoints
            headers["Authorization"] = f"Bearer {self._conn.api_token}"

        return headers

    def _get_token_from_secret(
        self, secret: str, graph_name: str | None = None, lifetime: int = 3600 * 24 * 30
    ) -> tuple[str, str | None]:
        """
        Generate authentication token from secret using TigerGraph REST API.

        Implements robust token generation with fallback logic for different TG 4.x versions:
        - TigerGraph 4.2.2+: POST /gsql/v1/tokens (lifetime in milliseconds)
        - TigerGraph 4.0-4.2.1: POST /gsql/v1/auth/token (lifetime in seconds)

        Based on pyTigerGraph's token generation mechanism with version-specific endpoint handling.

        Args:
            secret: Secret string created via CREATE SECRET in GSQL
            graph_name: Name of the graph (None for global token)
            lifetime: Token lifetime in seconds (default: 30 days)

        Returns:
            Tuple of (token, expiration_timestamp) or (token, None) if expiration not provided

        Raises:
            RuntimeError: If token generation fails after all retry attempts
        """
        auth_headers = self._get_auth_headers(use_basic_auth=True)
        headers = {
            "Content-Type": "application/json",
            **auth_headers,
        }

        # Determine which endpoint to try based on version
        # For TG 4.2.2+, use /gsql/v1/tokens (lifetime in milliseconds)
        # For TG 4.0-4.2.1, use /gsql/v1/auth/token (lifetime in seconds)
        use_new_endpoint = False
        if self._conn.tg_version:
            import re

            version_match = re.search(r"(\d+)\.(\d+)\.(\d+)", self._conn.tg_version)
            if version_match:
                major = int(version_match.group(1))
                minor = int(version_match.group(2))
                patch = int(version_match.group(3))
                # Use new endpoint for 4.2.2+
                use_new_endpoint = (major, minor, patch) >= (4, 2, 2)

        # Try endpoints in order: new endpoint first (if version >= 4.2.2), then fallback
        endpoints_to_try = []
        if use_new_endpoint:
            # Try new endpoint first for 4.2.2+
            endpoints_to_try.append(
                (
                    f"{self._conn.gsql_url}/gsql/v1/tokens",
                    {
                        "secret": secret,
                        "graph": graph_name,
                        "lifetime": lifetime * 1000,  # Convert to milliseconds
                    },
                    True,  # lifetime in milliseconds
                )
            )
            # Fallback to old endpoint if new one fails
            endpoints_to_try.append(
                (
                    f"{self._conn.gsql_url}/gsql/v1/auth/token",
                    {
                        "secret": secret,
                        "graph": graph_name,
                        "lifetime": lifetime,  # In seconds
                    },
                    False,  # lifetime in seconds
                )
            )
        else:
            # For older versions or unknown version, try old endpoint first
            endpoints_to_try.append(
                (
                    f"{self._conn.gsql_url}/gsql/v1/auth/token",
                    {
                        "secret": secret,
                        "graph": graph_name,
                        "lifetime": lifetime,  # In seconds
                    },
                    False,  # lifetime in seconds
                )
            )
            # Fallback to new endpoint (in case version detection was wrong)
            endpoints_to_try.append(
                (
                    f"{self._conn.gsql_url}/gsql/v1/tokens",
                    {
                        "secret": secret,
                        "graph": graph_name,
                        "lifetime": lifetime * 1000,  # Convert to milliseconds
                    },
                    True,  # lifetime in milliseconds
                )
            )

        last_error: Exception | None = None
        all_404_errors = True  # Track if all failures were 404 errors

        for url, payload, _is_milliseconds in endpoints_to_try:
            try:
                # Remove None values from payload
                clean_payload = {k: v for k, v in payload.items() if v is not None}

                response = requests.post(
                    url,
                    headers=headers,
                    json=clean_payload,  # Use json parameter instead of data
                    timeout=30,
                    verify=self._conn.ssl_verify,
                )

                # Check for 404 - might indicate wrong endpoint or port issue
                if response.status_code == 404:
                    # Try port fallback (similar to pyTigerGraph's _req method)
                    # If using wrong port, try GSQL port
                    if (
                        "/gsql" in url
                        and self._conn.config.port is not None
                        and self._conn.config.gs_port is not None
                        and self._conn.config.port != self._conn.config.gs_port
                    ):
                        logger.debug(f"404 on {url}, trying GSQL port fallback...")
                        # Replace port in URL with GSQL port
                        fallback_url = url.replace(
                            f":{self._conn.config.port}",
                            f":{self._conn.config.gs_port}",
                        )
                        try:
                            response = requests.post(
                                fallback_url,
                                headers=headers,
                                json=clean_payload,
                                timeout=30,
                                verify=self._conn.ssl_verify,
                            )
                            if response.status_code == 200:
                                url = fallback_url  # Update URL for logging
                        except Exception:
                            pass  # Continue to next endpoint

                response.raise_for_status()
                result = response.json()

                # Parse response (both endpoints return similar format)
                # Format: {"token": "...", "expiration": "...", "error": false, "message": "..."}
                # or {"token": "..."} for older versions
                if result.get("error") is True:
                    error_msg = result.get("message", "Unknown error")
                    raise RuntimeError(f"Token generation failed: {error_msg}")

                token = result.get("token")
                expiration = result.get("expiration")

                if token:
                    logger.debug(
                        f"Successfully obtained token from {url} "
                        f"(expiration: {expiration or 'not provided'})"
                    )
                    return (token, expiration)
                else:
                    raise ValueError(f"No token in response: {result}")

            except requests.exceptions.HTTPError as e:
                # Track if this was a 404 error
                if e.response.status_code != 404:
                    all_404_errors = False

                # If 404 and we have more endpoints to try, continue
                if e.response.status_code == 404 and len(endpoints_to_try) > 1:
                    logger.debug(
                        f"Endpoint {url} returned 404, trying next endpoint..."
                    )
                    last_error = e
                    continue
                # For other HTTP errors, log and try next endpoint if available
                logger.debug(
                    f"HTTP error {e.response.status_code} on {url}: {e.response.text}"
                )
                last_error = e
                continue
            except Exception as e:
                all_404_errors = False  # Non-HTTP errors are not 404s
                logger.debug(f"Error trying {url}: {e}")
                last_error = e
                continue

        # All graph-specific endpoints failed
        # If all failures were 404 errors and we have a graph_name, try generating a global token
        # This handles cases where the graph doesn't exist yet (e.g., "DefaultGraph" at init time)
        # For TigerGraph 4.2.1, /gsql/v1/tokens requires the graph to exist, but /gsql/v1/auth/token
        # can generate a global token without a graph parameter
        if all_404_errors and graph_name is not None and last_error:
            logger.debug(
                f"All graph-specific token attempts failed with 404. "
                f"Graph '{graph_name}' may not exist yet. "
                f"Trying to generate a global token (without graph parameter)..."
            )

            # Try generating a global token using /gsql/v1/auth/token (works for TG 4.0-4.2.1)
            global_token_endpoints = [
                (
                    f"{self._conn.gsql_url}/gsql/v1/auth/token",
                    {
                        "secret": secret,
                        "lifetime": lifetime,  # In seconds
                        # No graph parameter = global token
                    },
                    False,  # lifetime in seconds
                )
            ]

            # Also try /gsql/v1/tokens without graph parameter (for TG 4.2.2+)
            global_token_endpoints.append(
                (
                    f"{self._conn.gsql_url}/gsql/v1/tokens",
                    {
                        "secret": secret,
                        "lifetime": lifetime * 1000,  # In milliseconds
                        # No graph parameter = global token
                    },
                    True,  # lifetime in milliseconds
                )
            )

            for url, payload, _is_milliseconds in global_token_endpoints:
                try:
                    clean_payload = {k: v for k, v in payload.items() if v is not None}

                    response = requests.post(
                        url,
                        headers=headers,
                        json=clean_payload,
                        timeout=30,
                        verify=self._conn.ssl_verify,
                    )

                    response.raise_for_status()
                    result = response.json()

                    if result.get("error") is True:
                        error_msg = result.get("message", "Unknown error")
                        logger.debug(f"Global token generation failed: {error_msg}")
                        continue

                    token = result.get("token")
                    expiration = result.get("expiration")

                    if token:
                        logger.info(
                            f"Successfully obtained global token from {url} "
                            f"(graph '{graph_name}' may not exist yet, using global token). "
                            f"Expiration: {expiration or 'not provided'}"
                        )
                        return (token, expiration)

                except Exception as e:
                    logger.debug(f"Error trying global token endpoint {url}: {e}")
                    continue

        # All endpoints failed (including global token fallback)
        error_msg = f"Failed to get token from secret after trying {len(endpoints_to_try)} endpoint(s)"
        if all_404_errors and graph_name:
            error_msg += f" (all returned 404, graph '{graph_name}' may not exist yet)"
        if last_error:
            error_msg += f": {last_error}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
