"""REST API data source implementation.

This module provides a data source for REST API endpoints, supporting
full HTTP configuration including authentication, headers, pagination,
and retry logic.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from urllib3.util.retry import Retry

from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class PaginationConfig(BaseDataclass):
    """Configuration for API pagination.

    Supports multiple pagination strategies:
    - offset: Offset-based pagination (offset, limit)
    - cursor: Cursor-based pagination (cursor parameter)
    - page: Page-based pagination (page, per_page)

    Attributes:
        strategy: Pagination strategy ('offset', 'cursor', 'page')
        offset_param: Parameter name for offset (default: 'offset')
        limit_param: Parameter name for limit (default: 'limit')
        cursor_param: Parameter name for cursor (default: 'cursor')
        page_param: Parameter name for page (default: 'page')
        per_page_param: Parameter name for per_page (default: 'per_page')
        initial_offset: Initial offset value (default: 0)
        initial_page: Initial page value (default: 1)
        page_size: Number of items per page (default: 100)
        cursor_path: JSON path to cursor in response (for cursor-based)
        has_more_path: JSON path to has_more flag in response
        data_path: JSON path to data array in response (default: root)
    """

    strategy: str = "offset"  # 'offset', 'cursor', 'page'
    offset_param: str = "offset"
    limit_param: str = "limit"
    cursor_param: str = "cursor"
    page_param: str = "page"
    per_page_param: str = "per_page"
    initial_offset: int = 0
    initial_page: int = 1
    page_size: int = 100
    cursor_path: str | None = None  # JSON path like "next_cursor"
    has_more_path: str | None = None  # JSON path like "has_more"
    data_path: str | None = None  # JSON path to data array, None means root


@dataclasses.dataclass
class APIConfig(BaseDataclass):
    """Configuration for REST API data source.

    Attributes:
        url: API endpoint URL
        method: HTTP method (default: 'GET')
        headers: HTTP headers as dictionary
        auth: Authentication configuration
            - For Basic auth: {'type': 'basic', 'username': '...', 'password': '...'}
            - For Bearer token: {'type': 'bearer', 'token': '...'}
            - For Digest auth: {'type': 'digest', 'username': '...', 'password': '...'}
        params: Query parameters as dictionary
        timeout: Request timeout in seconds (default: None for no timeout)
        retries: Number of retry attempts (default: 0)
        retry_backoff_factor: Backoff factor for retries (default: 0.1)
        retry_status_forcelist: HTTP status codes to retry on (default: [500, 502, 503, 504])
        verify: Verify SSL certificates (default: True)
        pagination: Pagination configuration (default: None)
    """

    url: str
    method: str = "GET"
    headers: dict[str, str] = dataclasses.field(default_factory=dict)
    auth: dict[str, Any] | None = None
    params: dict[str, Any] = dataclasses.field(default_factory=dict)
    timeout: float | None = None
    retries: int = 0
    retry_backoff_factor: float = 0.1
    retry_status_forcelist: list[int] = dataclasses.field(
        default_factory=lambda: [500, 502, 503, 504]
    )
    verify: bool = True
    pagination: PaginationConfig | None = None


@dataclasses.dataclass
class APIDataSource(AbstractDataSource):
    """Data source for REST API endpoints.

    This class provides a data source for REST API endpoints, supporting
    full HTTP configuration, authentication, pagination, and retry logic.
    Returns JSON responses as hierarchical dictionaries, similar to JSON files.

    Attributes:
        config: API configuration
    """

    config: APIConfig

    def __post_init__(self):
        """Initialize the API data source."""
        self.source_type = DataSourceType.API

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration.

        Returns:
            Configured requests session
        """
        session = requests.Session()

        # Configure retries
        if self.config.retries > 0:
            retry_strategy = Retry(
                total=self.config.retries,
                backoff_factor=self.config.retry_backoff_factor,
                status_forcelist=self.config.retry_status_forcelist,
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

        # Configure authentication
        if self.config.auth:
            auth_type = self.config.auth.get("type", "").lower()
            if auth_type == "basic":
                session.auth = HTTPBasicAuth(
                    self.config.auth.get("username", ""),
                    self.config.auth.get("password", ""),
                )
            elif auth_type == "digest":
                session.auth = HTTPDigestAuth(
                    self.config.auth.get("username", ""),
                    self.config.auth.get("password", ""),
                )
            elif auth_type == "bearer":
                token = self.config.auth.get("token", "")
                session.headers["Authorization"] = f"Bearer {token}"

        # Set headers
        session.headers.update(self.config.headers)

        return session

    def _extract_data(self, response: dict | list) -> list[dict]:
        """Extract data array from API response.

        Args:
            response: API response as dictionary or list

        Returns:
            List of data items
        """
        if self.config.pagination and self.config.pagination.data_path:
            # Navigate JSON path
            parts = self.config.pagination.data_path.split(".")
            data = response
            for part in parts:
                if isinstance(data, dict):
                    data = data.get(part)
                elif isinstance(data, list):
                    data = data[int(part)]
                else:
                    return []
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                return []
        else:
            # Root level data
            if isinstance(response, list):
                return response
            elif isinstance(response, dict):
                return [response]
            else:
                return []

    def _has_more(self, response: dict) -> bool:
        """Check if there are more pages to fetch.

        Args:
            response: API response as dictionary

        Returns:
            True if there are more pages
        """
        if not self.config.pagination:
            return False

        if self.config.pagination.has_more_path:
            parts = self.config.pagination.has_more_path.split(".")
            value = response
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return False
            return bool(value)

        # Default: check if data array is not empty
        data = self._extract_data(response)
        return len(data) > 0

    def _get_next_cursor(self, response: dict) -> str | None:
        """Get next cursor from response.

        Args:
            response: API response as dictionary

        Returns:
            Next cursor value or None
        """
        if not self.config.pagination or not self.config.pagination.cursor_path:
            return None

        parts = self.config.pagination.cursor_path.split(".")
        value = response
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return str(value) if value is not None else None

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Iterate over API data in batches.

        Args:
            batch_size: Number of items per batch
            limit: Maximum number of items to retrieve

        Yields:
            list[dict]: Batches of documents as dictionaries
        """
        session = self._create_session()
        total_items = 0

        try:
            # Initialize pagination state
            offset = (
                self.config.pagination.initial_offset if self.config.pagination else 0
            )
            page = self.config.pagination.initial_page if self.config.pagination else 1
            cursor: str | None = None

            while True:
                # Build request parameters
                params = self.config.params.copy()

                # Add pagination parameters
                if self.config.pagination:
                    if self.config.pagination.strategy == "offset":
                        params[self.config.pagination.offset_param] = offset
                        params[self.config.pagination.limit_param] = (
                            self.config.pagination.page_size
                        )
                    elif self.config.pagination.strategy == "page":
                        params[self.config.pagination.page_param] = page
                        params[self.config.pagination.per_page_param] = (
                            self.config.pagination.page_size
                        )
                    elif self.config.pagination.strategy == "cursor" and cursor:
                        params[self.config.pagination.cursor_param] = cursor

                # Make request
                try:
                    response = session.request(
                        method=self.config.method,
                        url=self.config.url,
                        params=params,
                        timeout=self.config.timeout,
                        verify=self.config.verify,
                    )
                    response.raise_for_status()
                    data = response.json()
                except requests.RequestException as e:
                    logger.error(f"API request failed: {e}")
                    break

                # Extract data from response
                items = self._extract_data(data)

                # Process items in batches
                batch = []
                for item in items:
                    if limit and total_items >= limit:
                        break
                    batch.append(item)
                    total_items += 1

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

                # Yield remaining items
                if batch:
                    yield batch

                # Check if we should continue
                if limit and total_items >= limit:
                    break

                # Update pagination state
                if self.config.pagination:
                    if self.config.pagination.strategy == "offset":
                        if not self._has_more(data):
                            break
                        offset += self.config.pagination.page_size
                    elif self.config.pagination.strategy == "page":
                        if not self._has_more(data):
                            break
                        page += 1
                    elif self.config.pagination.strategy == "cursor":
                        cursor = self._get_next_cursor(data)
                        if not cursor:
                            break
                else:
                    # No pagination, single request
                    break

        finally:
            session.close()
