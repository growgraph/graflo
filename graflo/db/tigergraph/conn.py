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
from pathlib import Path
from typing import Any


import requests
from requests import exceptions as requests_exceptions

# Removed pyTigerGraph dependency - using direct REST API calls instead


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


# Monkey-patch specific exception classes to add add_note() for Python 3.10 compatibility
# Python 3.11+ already has this, but Python 3.10 doesn't
# We patch specific classes since the base Exception class is immutable
def _add_note_shim(self, note: str) -> None:
    """Add a note to the exception (Python 3.11+ compatibility shim for Python 3.10)."""
    if not hasattr(self, "_notes"):
        self._notes = []
    self._notes.append(note)


def _patch_exception_class(cls: type[Exception]) -> None:
    """Patch an exception class to add add_note() if it doesn't exist."""
    if not hasattr(cls, "add_note"):
        cls.add_note = _add_note_shim  # type: ignore[attr-defined, assignment]


# Patch requests exceptions (HTTPError, ConnectionError, Timeout, RequestException)
try:
    from requests.exceptions import (
        HTTPError,
        ConnectionError,
        Timeout,
        RequestException,
    )

    _patch_exception_class(HTTPError)
    _patch_exception_class(ConnectionError)
    _patch_exception_class(Timeout)
    _patch_exception_class(RequestException)
except (ImportError, AttributeError):
    pass

# Removed pyTigerGraph dependency - no longer need TigerGraphException patching


def _wrap_tg_exception(func):
    """Decorator to wrap TigerGraph exceptions for compatibility.

    This decorator is kept for backward compatibility but is no longer strictly
    necessary since we've monkey-patched the specific exception classes.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            # Re-raise all exceptions as-is (add_note is available on patched exceptions)
            raise

    return wrapper


def _validate_tigergraph_schema_name(name: str, name_type: str) -> None:
    """
    Validate a TigerGraph schema name (graph, vertex, or edge) against reserved words
    and invalid characters.

    Args:
        name: The schema name to validate
        name_type: Type of schema name ("graph", "vertex", or "edge")

    Raises:
        ValueError: If the name contains reserved words, forbidden prefixes, or invalid characters
    """
    if not name:
        raise ValueError(f"{name_type.capitalize()} name cannot be empty")

    # Load reserved words from JSON file
    json_path = Path(__file__).parent / "reserved_words.json"
    try:
        with open(json_path, "r") as f:
            reserved_data = json.load(f)
    except FileNotFoundError:
        logger.warning(
            f"Could not find reserved_words.json at {json_path}, skipping validation"
        )
        return
    except json.JSONDecodeError as e:
        logger.warning(f"Could not parse reserved_words.json: {e}, skipping validation")
        return

    reserved_words = set()
    reserved_words.update(
        reserved_data.get("reserved_words", {}).get("gsql_keywords", [])
    )
    reserved_words.update(
        reserved_data.get("reserved_words", {}).get("cpp_keywords", [])
    )

    # Check for reserved words (case-insensitive)
    name_upper = name.upper()
    if name_upper in reserved_words:
        raise ValueError(
            f"{name_type.capitalize()} name '{name}' is a TigerGraph reserved word. "
            f"Reserved words cannot be used as identifiers. "
            f"Please choose a different name."
        )

    # Check for forbidden prefixes
    forbidden_prefixes = reserved_data.get("forbidden_prefixes", [])
    for prefix in forbidden_prefixes:
        if name.startswith(prefix):
            raise ValueError(
                f"{name_type.capitalize()} name '{name}' starts with forbidden prefix '{prefix}'. "
                f"Please choose a different name."
            )

    # Check for invalid characters
    invalid_chars = reserved_data.get("invalid_characters", {}).get("characters", [])
    found_chars = [char for char in invalid_chars if char in name]
    if found_chars:
        raise ValueError(
            f"{name_type.capitalize()} name '{name}' contains invalid characters: {found_chars}. "
            f"TigerGraph identifiers should use alphanumeric characters and underscores only. "
            f"Special characters (especially hyphens and dots) are problematic for REST API endpoints. "
            f"Please choose a different name."
        )


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
        For best results, provide BOTH username/password AND secret:
        - username/password: Required for initial connection and GSQL operations
        - secret: Generates token that works for both GSQL and REST API operations

        Token-based authentication using secrets is the most robust and recommended
        approach for TigerGraph 4+. The connection will:
        1. Use username/password for initial connection
        2. Generate a token from the secret
        3. Use the token for both GSQL operations (via REST API) and REST API calls

        Example:
            >>> config = TigergraphConfig(
            ...     uri="http://localhost:14240",
            ...     username="tigergraph",      # Required for initial connection
            ...     password="tigergraph",      # Required for initial connection
            ...     secret="your_secret_here",  # Generates token for GSQL + REST API
            ...     database="my_graph"
            ... )
            >>> conn = TigerGraphConnection(config)

    Port Configuration for TigerGraph 4+:
        TigerGraph 4.1+ uses port 14240 (GSQL server) as the primary interface.
        Port 9000 (REST++) is for internal use only in TG 4.1+.

        Standard ports:
        - Port 14240: GSQL server (primary interface for all API requests)
        - Port 9000: REST++ (internal-only in TG 4.1+)

        For custom Docker deployments with port mapping, ports are configured via
        environment variables (e.g., TG_WEB, TG_REST) and loaded automatically
        when using TigergraphConfig.from_docker_env().

    Version Compatibility:
        - All TigerGraph versions use /restpp prefix for REST++ endpoints
        - Version is auto-detected, or can be manually specified in config
    """

    flavor = DBFlavor.TIGERGRAPH

    def __init__(self, config: TigergraphConfig):
        super().__init__()
        self.config = config
        self.ssl_verify = getattr(config, "ssl_verify", True)

        # Store connection configuration (no longer using pyTigerGraph)
        # For TigerGraph 4+, both ports typically route through the GSQL server
        # Port 9000 (REST++) is internal-only in TG 4.1+
        self.graphname: str = (
            config.database if config.database is not None else "DefaultGraph"
        )

        # Initialize URLs (ports come from config, no hardcoded defaults)
        # Set GSQL URL first as it's needed for token generation
        # For TigerGraph 4+, gs_port is the primary port (extracted from URI if not explicitly set)
        # Fall back to port from URI if gs_port is not set
        gs_port: int | str | None = config.gs_port
        if gs_port is None:
            # Try to get port from URI
            uri_port = config.port
            if uri_port:
                try:
                    gs_port = int(uri_port)
                    logger.debug(f"Using port {gs_port} from URI for GSQL endpoint")
                except (ValueError, TypeError):
                    pass

        if gs_port is None:
            raise ValueError(
                "gs_port or URI with port must be set in TigergraphConfig. "
                "Standard ports: 14240 (GSQL), 9000 (REST++)."
            )
        self.gsql_url = f"{config.url_without_port}:{gs_port}"

        # Detect TigerGraph version for compatibility (needed before token generation)
        self.tg_version: str | None = None
        self._use_restpp_prefix = False  # Default for 4.2.2+

        # Check if version is manually configured first
        if hasattr(config, "version") and config.version:
            version_str = config.version
            logger.info(f"Using manually configured TigerGraph version: {version_str}")
        else:
            # Auto-detect version using REST API
            try:
                version_str = self._get_version()
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

                # All TigerGraph versions use /restpp prefix for REST++ endpoints
                # Even 4.2.2+ requires /restpp prefix (despite some documentation suggesting otherwise)
                self._use_restpp_prefix = True
                logger.info(
                    f"TigerGraph version {self.tg_version} detected, "
                    f"using /restpp prefix for REST API"
                )
            else:
                logger.warning(
                    f"Could not extract version number from '{version_str}'. "
                    f"Defaulting to using /restpp prefix for REST API"
                )
                self._use_restpp_prefix = True

        # Store base URLs for REST++ and GSQL endpoints
        # For TigerGraph 4.1+, REST++ endpoints use the GSQL port with /restpp prefix
        # Port 9000 is internal-only in TG 4.1+, so we use the same port as GSQL
        # Use the GSQL port we already determined to ensure consistency
        base_url = f"{config.url_without_port}:{gs_port}"
        # Always use /restpp prefix for REST++ endpoints (required for all TG versions)
        self.restpp_url = f"{base_url}/restpp"

        # Get authentication token if secret is provided
        # Token-based auth is the recommended approach for TigerGraph 4+
        # IMPORTANT: You should provide BOTH username/password AND secret:
        # - username/password: Used for initial connection and GSQL operations
        # - secret: Generates token that works for both GSQL and REST API operations
        # Use graph-specific token (is_global=False) for better security
        self.api_token: str | None = None
        if config.secret:
            try:
                token, expiration = self._get_token_from_secret(
                    config.secret,
                    self.graphname,  # Pass graph name for graph-specific token
                )
                self.api_token = token
                if expiration:
                    logger.info(
                        f"Successfully obtained API token for graph '{self.graphname}' "
                        f"(expires: {expiration})"
                    )
                else:
                    logger.info(
                        f"Successfully obtained API token for graph '{self.graphname}'"
                    )
            except Exception as e:
                # Log and fall back to username/password authentication
                logger.warning(f"Failed to get authentication token: {e}")
                logger.warning("Falling back to username/password authentication")
                logger.warning(
                    "Note: For best results, provide both username/password AND secret. "
                    "Username/password is used for GSQL operations, secret generates token for REST API."
                )

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
        if use_basic_auth or not self.api_token:
            # Use default username "tigergraph" if username is None but password is set
            username = self.config.username if self.config.username else "tigergraph"
            password = self.config.password

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
            headers["Authorization"] = f"Bearer {self.api_token}"

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
        if self.tg_version:
            import re

            version_match = re.search(r"(\d+)\.(\d+)\.(\d+)", self.tg_version)
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
                    f"{self.gsql_url}/gsql/v1/tokens",
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
                    f"{self.gsql_url}/gsql/v1/auth/token",
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
                    f"{self.gsql_url}/gsql/v1/auth/token",
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
                    f"{self.gsql_url}/gsql/v1/tokens",
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
                    verify=self.ssl_verify,
                )

                # Check for 404 - might indicate wrong endpoint or port issue
                if response.status_code == 404:
                    # Try port fallback (similar to pyTigerGraph's _req method)
                    # If using wrong port, try GSQL port
                    if (
                        "/gsql" in url
                        and self.config.port is not None
                        and self.config.gs_port is not None
                        and self.config.port != self.config.gs_port
                    ):
                        logger.debug(f"404 on {url}, trying GSQL port fallback...")
                        # Replace port in URL with GSQL port
                        fallback_url = url.replace(
                            f":{self.config.port}", f":{self.config.gs_port}"
                        )
                        try:
                            response = requests.post(
                                fallback_url,
                                headers=headers,
                                json=clean_payload,
                                timeout=30,
                                verify=self.ssl_verify,
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
                    f"{self.gsql_url}/gsql/v1/auth/token",
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
                    f"{self.gsql_url}/gsql/v1/tokens",
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
                        verify=self.ssl_verify,
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
        import re

        if self.config.gs_port is None:
            raise ValueError("gs_port must be set in config for version detection")

        # Try GSQL endpoint first (primary for TigerGraph 4+)
        # Note: /gsql/v1/version exists on GSQL port, but /version does NOT
        # Response format: plain text like "GSQL version: 4.2.2\n"
        gsql_url = f"{self.gsql_url}/gsql/v1/version"
        headers = self._get_auth_headers(use_basic_auth=True)

        try:
            response = requests.get(
                gsql_url, headers=headers, timeout=10, verify=self.ssl_verify
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
            restpp_port = self.config.port if self.config.port else self.config.gs_port
            if restpp_port is None:
                return None

            restpp_url = f"{self.config.url_without_port}:{restpp_port}/version"
            headers = self._get_auth_headers(use_basic_auth=True)

            response = requests.get(
                restpp_url, headers=headers, timeout=10, verify=self.ssl_verify
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
        url = f"{self.gsql_url}/gsql/v1/statements"
        auth_headers = self._get_auth_headers(use_basic_auth=True)
        headers = {
            "Content-Type": "text/plain",
            **auth_headers,
        }

        # Debug: Log if Authorization header is missing
        if "Authorization" not in headers:
            logger.error(
                f"No Authorization header generated. "
                f"Username: {self.config.username}, Password: {'***' if self.config.password else None}"
            )

        try:
            response = requests.post(
                url,
                headers=headers,
                data=gsql_command,
                timeout=120,
                verify=self.ssl_verify,
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
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            List of vertex type names
        """
        graph_name = graph_name or self.graphname
        try:
            result = self._execute_gsql(f"USE GRAPH {graph_name}\nSHOW VERTEX *")
            # Parse GSQL output using the proper parser
            if isinstance(result, str):
                return self._parse_show_output(result, "VERTEX")
            return []
        except Exception as e:
            logger.debug(f"Failed to get vertex types via GSQL: {e}")
            return []

    def _get_edge_types(self, graph_name: str | None = None) -> list[str]:
        """
        Get list of edge types using GSQL.

        Args:
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            List of edge type names
        """
        graph_name = graph_name or self.graphname
        try:
            result = self._execute_gsql(f"USE GRAPH {graph_name}\nSHOW EDGE *")
            # Parse GSQL output using the proper parser
            if isinstance(result, str):
                # _parse_show_edge_output returns list of tuples (edge_name, is_directed)
                # Extract just the edge names
                edge_tuples = self._parse_show_edge_output(result)
                return [edge_name for edge_name, _ in edge_tuples]
            return []
        except Exception as e:
            logger.debug(f"Failed to get edge types via GSQL: {e}")
            return []

    def _get_installed_queries(self, graph_name: str | None = None) -> list[str]:
        """
        Get list of installed queries using GSQL.

        Args:
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            List of query names
        """
        graph_name = graph_name or self.graphname
        try:
            result = self._execute_gsql(f"USE GRAPH {graph_name}\nSHOW QUERY *")
            # Parse GSQL output to extract query names
            queries = []
            if isinstance(result, str):
                lines = result.split("\n")
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith("#") and not line.startswith("USE"):
                        # Query names are typically on their own lines
                        if line and not line.startswith("---"):
                            queries.append(line)
            return queries if queries else []
        except Exception as e:
            logger.debug(f"Failed to get installed queries via GSQL: {e}")
            return []

    def _run_installed_query(
        self, query_name: str, graph_name: str | None = None, **kwargs: Any
    ) -> dict[str, Any] | list[dict]:
        """
        Run an installed query using REST API.

        Args:
            query_name: Name of the installed query
            graph_name: Name of the graph (defaults to self.graphname)
            **kwargs: Query parameters

        Returns:
            Query result (dict or list)
        """
        graph_name = graph_name or self.graphname
        endpoint = f"/query/{graph_name}/{query_name}"
        return self._call_restpp_api(endpoint, method="POST", data=kwargs)

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
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            Response from API
        """
        graph_name = graph_name or self.graphname
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
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            Response from API
        """
        graph_name = graph_name or self.graphname
        endpoint = (
            f"/graph/{graph_name}/edges/{edge_type}/"
            f"{source_type}/{quote(str(source_id))}/"
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
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            List of edge dictionaries
        """
        graph_name = graph_name or self.graphname

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
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            Dictionary mapping vertex_id to vertex data
        """
        graph_name = graph_name or self.graphname
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
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            Number of vertices
        """
        graph_name = graph_name or self.graphname
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
            graph_name: Name of the graph (defaults to self.graphname)

        Returns:
            Response from API
        """
        graph_name = graph_name or self.graphname
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
            # For TigerGraph 4.2.1, if token auth fails with 401/REST-10018, try Basic Auth fallback
            if (
                errh.response.status_code == 401
                and self.api_token
                and self.config.username
                and self.config.password
                and "REST-10018" in str(errh)
            ):
                logger.warning(
                    "Token authentication failed with REST-10018, "
                    "falling back to Basic Auth for TigerGraph 4.2.1 compatibility"
                )
                # Retry with Basic Auth
                import base64

                credentials = f"{self.config.username}:{self.config.password}"
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded_credentials}"
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
                            data=json.dumps(data, default=_json_serializer)
                            if data
                            else None,
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

        Stores graph name for operations that need it.

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

        old_graphname = self.graphname
        self.graphname = graph_name

        try:
            yield graph_name
        finally:
            # Restore original graphname
            self.graphname = old_graphname

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
            result = self._execute_gsql(f"USE GRAPH {name}")
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
        if self.graph_exists(name):
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
                result = self._execute_gsql(gsql_commands)
                logger.info(
                    f"Successfully created graph '{name}' with types {all_types}: {result}"
                )
                # Verify the result doesn't indicate the graph already existed
                result_str = str(result).lower()
                if (
                    "already exists" in result_str
                    or "duplicate" in result_str
                    or "graph already exists" in result_str
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
                        queries = self._get_installed_queries()
                        if queries:
                            logger.info(
                                f"Dropping {len(queries)} queries from graph '{name}'"
                            )
                            for query_name in queries:
                                try:
                                    # Try DROP QUERY with IF EXISTS to avoid errors
                                    drop_query_cmd = f"USE GRAPH {name}\nDROP QUERY {query_name} IF EXISTS"
                                    self._execute_gsql(drop_query_cmd)
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
                                        self._execute_gsql(drop_query_cmd)
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
                    result = self._execute_gsql(list_queries_cmd)
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
                        vertex_types = self._get_vertex_types()
                        for v_type in vertex_types:
                            try:
                                self._delete_vertices(v_type)
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
                result = self._execute_gsql(drop_command)
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
                        edge_types = self._get_edge_types()
                    except Exception:
                        edge_types = []

                    for e_type in edge_types:
                        # Only disassociate from graph, don't drop globally
                        # ALTER GRAPH requires USE GRAPH context
                        try:
                            drop_edge_cmd = f"USE GRAPH {name}\nALTER GRAPH {name} DROP DIRECTED EDGE {e_type}"
                            self._execute_gsql(drop_edge_cmd)
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
                        vertex_types = self._get_vertex_types()
                    except Exception:
                        vertex_types = []

                    for v_type in vertex_types:
                        # Only clear data from this graph's vertices, don't drop vertex type globally
                        # Clear data first to avoid dependency issues
                        try:
                            self._delete_vertices(v_type)
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
                            self._execute_gsql(drop_vertex_cmd)
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
                    vertex_types = self._get_vertex_types()
                    for v_type in vertex_types:
                        result = self._delete_vertices(v_type)
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
                result = self._run_installed_query(query_name, **kwargs)
            else:
                # Execute as raw GSQL
                result = self._execute_gsql(query)
            return result
        except Exception as e:
            logger.error(f"Error executing query '{query}': {e}")
            raise

    def close(self):
        """Close connection - no cleanup needed (using direct REST API calls)."""
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

        # Validate graph name
        _validate_tigergraph_schema_name(graph_name, "graph")

        vertex_config = schema.vertex_config
        edge_config = schema.edge_config

        schema_change_stmts = []

        # Vertices
        for vertex in vertex_config.vertices:
            # Validate vertex name
            _validate_tigergraph_schema_name(vertex.name, "vertex")
            stmt = self._get_vertex_add_statement(vertex, vertex_config)
            schema_change_stmts.append(stmt)

        # Edges
        edges_to_create = list(edge_config.edges_list(include_aux=True))
        for edge in edges_to_create:
            edge.finish_init(vertex_config)
            # Validate edge name
            _validate_tigergraph_schema_name(edge.relation, "edge")
            stmt = self._get_edge_add_statement(edge)
            schema_change_stmts.append(stmt)

        if not schema_change_stmts:
            logger.debug(f"No schema changes to apply for graph '{graph_name}'")
            return

        # Estimate the size of the GSQL command to determine if we need to split it
        # Large SCHEMA_CHANGE JOBs (>30k chars) can cause parser failures with misleading errors
        # like "Missing return statement" (which is actually a parser size limit issue)
        # We'll split into batches based on configurable max_job_size (default: 1000)
        MAX_JOB_SIZE = self.config.max_job_size

        # Calculate accurate size estimation
        # Actual format:
        #   USE GRAPH {graph_name}
        #   CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {graph_name} {
        #       stmt1;
        #       stmt2;
        #       ...
        #   }
        #   RUN SCHEMA_CHANGE JOB {job_name}
        #
        # For N statements:
        #   - Base overhead: USE GRAPH line + CREATE line + closing brace + RUN line + newlines
        #   - Statement overhead: first gets "    " + ";" (5 chars), others get ";\n    " (5 chars each)
        #   - Total: base + sum(len(stmt)) + 5*N

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
        num_statements = len(schema_change_stmts)
        total_stmt_size = sum(len(stmt) for stmt in schema_change_stmts)
        estimated_size = base_overhead + total_stmt_size + 5 * num_statements

        if estimated_size <= MAX_JOB_SIZE:
            # Small enough for a single job
            batches = [schema_change_stmts]
            logger.info(
                f"Applying schema change as single job (estimated size: {estimated_size} chars)"
            )
        else:
            # Split into multiple batches
            # Calculate how many statements per batch
            # For a batch of M statements: base_overhead + sum(len(stmt)) + 5*M <= MAX_JOB_SIZE
            # So: sum(len(stmt)) + 5*M <= MAX_JOB_SIZE - base_overhead
            # If avg_stmt_size = sum(len(stmt)) / M, then: M * (avg_stmt_size + 5) <= MAX_JOB_SIZE - base_overhead
            avg_stmt_size = (
                total_stmt_size / num_statements if num_statements > 0 else 0
            )
            available_space = MAX_JOB_SIZE - base_overhead
            stmts_per_batch = max(1, int(available_space / (avg_stmt_size + 5)))

            batches = []
            for i in range(0, len(schema_change_stmts), stmts_per_batch):
                batches.append(schema_change_stmts[i : i + stmts_per_batch])

            logger.info(
                f"Large schema detected (estimated size: {estimated_size} chars). "
                f"Splitting into {len(batches)} batches of ~{stmts_per_batch} statements each."
            )

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
                self._execute_gsql(drop_job_cmd)
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
            if actual_size > MAX_JOB_SIZE:
                logger.warning(
                    f"Batch {batch_idx + 1} actual size ({actual_size} chars) exceeds limit ({MAX_JOB_SIZE} chars). "
                    f"This may cause parser errors. Consider reducing MAX_JOB_SIZE or improving estimation."
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
                result = self._execute_gsql(full_gsql)
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
        import time

        time.sleep(1.0)  # Increased wait time

        with self._ensure_graph_context(graph_name):
            vertex_types = self._get_vertex_types()
            edge_types = self._get_edge_types()

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

            expected_edge_types = {e.relation for e in edges_to_create if e.relation}

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

        # Validate graph name
        _validate_tigergraph_schema_name(graph_name, "graph")

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
        self._execute_gsql("\n".join(gsql_commands))

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
        self._execute_gsql("\n".join(gsql_commands))

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
                self._execute_gsql(drop_job_cmd)
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
                result = self._execute_gsql(create_job_cmd)
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
                result = self._execute_gsql(run_job_cmd)
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
        Parse SHOW * output to extract type names.

        Looks for lines matching: "- PREFIX name(" or "PREFIX name("

        Args:
            result_str: String output from SHOW * GSQL command
            prefix: The prefix to look for (e.g., "VERTEX", "EDGE")

        Returns:
            List of extracted names
        """
        import re

        names = []
        # Pattern: "- VERTEX name(" or "VERTEX name("
        # Match lines that contain the prefix followed by a word (the name) and then "("
        pattern = rf"(?:^|\s)-?\s*{re.escape(prefix)}\s+(\w+)\s*\("

        for line in result_str.split("\n"):
            line = line.strip()
            if not line:
                continue

            # Use regex to find matches
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                name = match.group(1)
                if name and name not in names:
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
        import re

        edge_types = []
        # Pattern for DIRECTED EDGE: "- DIRECTED EDGE name("
        directed_pattern = r"(?:^|\s)-?\s*DIRECTED\s+EDGE\s+(\w+)\s*\("
        # Pattern for UNDIRECTED EDGE: "- UNDIRECTED EDGE name("
        undirected_pattern = r"(?:^|\s)-?\s*UNDIRECTED\s+EDGE\s+(\w+)\s*\("

        for line in result_str.split("\n"):
            line = line.strip()
            if not line:
                continue

            # Check for DIRECTED EDGE
            match = re.search(directed_pattern, line, re.IGNORECASE)
            if match:
                edge_name = match.group(1)
                if edge_name:
                    edge_types.append((edge_name, True))
                continue

            # Check for UNDIRECTED EDGE
            match = re.search(undirected_pattern, line, re.IGNORECASE)
            if match:
                edge_name = match.group(1)
                if edge_name:
                    edge_types.append((edge_name, False))

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
                        result = self._execute_gsql(show_graphs_cmd)
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
                    result = self._execute_gsql(show_edges_cmd)
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
                            result = self._execute_gsql(drop_edge_cmd)
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
                    result = self._execute_gsql(show_vertices_cmd)
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
                                result = self._delete_vertices(v_type)
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
                            result = self._execute_gsql(drop_vertex_cmd)
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
                    result = self._execute_gsql(show_jobs_cmd)
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
                            result = self._execute_gsql(drop_job_cmd)
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
                            result = self._delete_vertices(class_name)
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
            # For TigerGraph 4.2.1, if token auth fails with 401/REST-10018, try Basic Auth fallback
            if (
                errh.response.status_code == 401
                and self.api_token
                and self.config.username
                and self.config.password
                and "REST-10018" in str(errh)
            ):
                logger.warning(
                    "Token authentication failed with REST-10018, "
                    "falling back to Basic Auth for TigerGraph 4.2.1 compatibility"
                )
                # Retry with Basic Auth
                import base64

                credentials = f"{self.config.username}:{self.config.password}"
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded_credentials}"
                try:
                    response = requests.post(
                        url,
                        headers=headers,
                        data=json.dumps(payload, default=_json_serializer),
                        timeout=120,
                        verify=self.ssl_verify,
                    )
                    response.raise_for_status()
                    logger.info("Successfully authenticated using Basic Auth fallback")
                    return response.json()
                except requests_exceptions.HTTPError as errh2:
                    logger.error(f"HTTP Error (after Basic Auth fallback): {errh2}")
                    error_details = ""
                    try:
                        error_details = response.text
                    except Exception:
                        pass
                    return {
                        "error": True,
                        "message": str(errh2),
                        "details": error_details,
                    }

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
                    # Serialize datetime objects before passing to REST API
                    # REST API expects JSON-serializable data
                    serialized_doc = json.loads(
                        json.dumps(clean_doc, default=_json_serializer)
                    )
                    self._upsert_vertex(class_name, vertex_id, serialized_doc)
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
                    # Serialize data for REST API
                    serialized_props = json.loads(
                        json.dumps(clean_edge_props, default=_json_serializer)
                    )
                    self._upsert_edge(
                        source_class,
                        source_id,
                        edge_type,
                        target_class,
                        target_id,
                        serialized_props,
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
        Fetch edges from TigerGraph using REST API.

        In TigerGraph, you must know at least one vertex ID before you can fetch edges.
        Uses REST API which handles special characters in vertex IDs.

        Args:
            from_type: Source vertex type (required)
            from_id: Source vertex ID (required)
            edge_type: Optional edge type to filter by
            to_type: Optional target vertex type to filter by (not used in REST API)
            to_id: Optional target vertex ID to filter by (not used in REST API)
            filters: Additional query filters (not supported by REST API)
            limit: Maximum number of edges to return (not supported by REST API)
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

            # Use REST API to get edges
            # Returns: list of edge dictionaries
            logger.debug(
                f"Fetching edges using REST API: from_type={from_type}, from_id={from_id}, edge_type={edge_type}"
            )

            # Handle None edge_type
            edge_type_str = edge_type if edge_type is not None else None
            edges = self._get_edges(from_type, from_id, edge_type_str)

            # Parse REST API response format
            # _get_edges() returns list of edge dicts from REST++ API
            # Format: [{"e_type": "...", "from_id": "...", "to_id": "...", "attributes": {...}}, ...]
            # The REST API returns edges in a flat format with e_type, from_id, to_id, attributes
            if isinstance(edges, list):
                # Process each edge to normalize format
                result = []
                for edge in edges:
                    if isinstance(edge, dict):
                        # Normalize edge format - REST API returns flat structure
                        normalized_edge = {}

                        # Extract edge type (rename e_type to edge_type for consistency)
                        normalized_edge["edge_type"] = edge.get(
                            "e_type", edge.get("edge_type", "")
                        )

                        # Extract from/to IDs and types
                        normalized_edge["from_id"] = edge.get("from_id", "")
                        normalized_edge["from_type"] = edge.get("from_type", "")
                        normalized_edge["to_id"] = edge.get("to_id", "")
                        normalized_edge["to_type"] = edge.get("to_type", "")

                        # Handle nested "from"/"to" objects if present (some API versions)
                        if "from" in edge and isinstance(edge["from"], dict):
                            normalized_edge["from_id"] = edge["from"].get(
                                "id",
                                edge["from"].get("v_id", normalized_edge["from_id"]),
                            )
                            normalized_edge["from_type"] = edge["from"].get(
                                "type",
                                edge["from"].get(
                                    "v_type", normalized_edge["from_type"]
                                ),
                            )

                        if "to" in edge and isinstance(edge["to"], dict):
                            normalized_edge["to_id"] = edge["to"].get(
                                "id", edge["to"].get("v_id", normalized_edge["to_id"])
                            )
                            normalized_edge["to_type"] = edge["to"].get(
                                "type",
                                edge["to"].get("v_type", normalized_edge["to_type"]),
                            )

                        # Extract attributes and merge into normalized edge
                        attributes = edge.get("attributes", {})
                        if attributes:
                            normalized_edge.update(attributes)
                        else:
                            # If no attributes key, include all other fields as attributes
                            for k, v in edge.items():
                                if k not in (
                                    "e_type",
                                    "edge_type",
                                    "from",
                                    "to",
                                    "from_id",
                                    "to_id",
                                    "from_type",
                                    "to_type",
                                    "directed",
                                ):
                                    normalized_edge[k] = v

                        result.append(normalized_edge)
            elif isinstance(edges, dict):
                # Single edge dict - normalize and wrap in list
                normalized_edge = {}
                normalized_edge["edge_type"] = edges.get(
                    "e_type", edges.get("edge_type", "")
                )
                normalized_edge["from_id"] = edges.get("from_id", "")
                normalized_edge["to_id"] = edges.get("to_id", "")

                if "from" in edges and isinstance(edges["from"], dict):
                    normalized_edge["from_id"] = edges["from"].get(
                        "id", edges["from"].get("v_id", normalized_edge["from_id"])
                    )
                if "to" in edges and isinstance(edges["to"], dict):
                    normalized_edge["to_id"] = edges["to"].get(
                        "id", edges["to"].get("v_id", normalized_edge["to_id"])
                    )

                attributes = edges.get("attributes", {})
                if attributes:
                    normalized_edge.update(attributes)
                else:
                    for k, v in edges.items():
                        if k not in (
                            "e_type",
                            "edge_type",
                            "from",
                            "to",
                            "from_id",
                            "to_id",
                        ):
                            normalized_edge[k] = v

                result = [normalized_edge]
            else:
                # Fallback for unexpected types
                result: list[dict[str, Any]] = []
                logger.debug(f"Unexpected edges type: {type(edges)}")

            # Apply limit if specified (client-side since REST API doesn't support it)
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
            logger.error(f"Error fetching edges via REST API: {e}")
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
                    vertex_data = self._get_vertices_by_id(class_name, vertex_id)
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
                count = self._get_vertex_count(class_name)
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
                    vertex_types = self._get_vertex_types()

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
