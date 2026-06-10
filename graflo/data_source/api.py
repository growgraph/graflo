"""REST API data source implementation.

Runtime HTTP executor for API connectors. Configuration is built by
:class:`~graflo.architecture.contract.bindings.APIConnector.build_api_config`
from contract fields plus runtime credentials from a connection provider.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from urllib3.util.retry import Retry

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.bindings import PaginationConfig
from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.connection_models import ApiAuth

logger = logging.getLogger(__name__)


class APIConfig(ConfigBaseModel):
    """Merged runtime configuration for REST API requests.

    Built exclusively via :meth:`APIConnector.build_api_config`; not intended
    for direct construction in manifests or factory helpers.
    """

    url: str
    method: str = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    auth: ApiAuth | None = None
    params: dict[str, object] = Field(default_factory=dict)
    timeout: float | None = None
    retries: int = 0
    retry_backoff_factor: float = 0.1
    retry_status_forcelist: list[int] = Field(
        default_factory=lambda: [500, 502, 503, 504]
    )
    verify: bool = True
    pagination: PaginationConfig | None = None
    row_annotations: dict[str, Any] = Field(default_factory=dict)


class APIDataSource(AbstractDataSource):
    """Data source for REST API endpoints."""

    config: APIConfig
    source_type: DataSourceType = DataSourceType.API

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        if self.config.retries > 0:
            retry_strategy = Retry(
                total=self.config.retries,
                backoff_factor=self.config.retry_backoff_factor,
                status_forcelist=self.config.retry_status_forcelist,
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

        auth = self.config.auth
        if auth is not None:
            if auth.auth_type == "basic":
                session.auth = HTTPBasicAuth(
                    auth.username or "",
                    auth.password or "",
                )
            elif auth.auth_type == "digest":
                session.auth = HTTPDigestAuth(
                    auth.username or "",
                    auth.password or "",
                )
            elif auth.auth_type == "bearer":
                token = auth.token or ""
                session.headers[auth.header_name] = f"{auth.prefix} {token}".strip()
            elif auth.auth_type == "api_key":
                session.headers[auth.header_name] = auth.token or ""

        session.headers.update(self.config.headers)
        return session

    def _extract_data(self, response: dict | list) -> list[dict]:
        if self.config.pagination and self.config.pagination.data_path:
            parts = self.config.pagination.data_path.split(".")
            data: object = response
            for part in parts:
                if isinstance(data, dict):
                    data = data.get(part)
                elif isinstance(data, list):
                    data = data[int(part)]
                else:
                    return []
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []

        if isinstance(response, list):
            return response
        if isinstance(response, dict):
            return [response]
        return []

    def _has_more(self, response: dict) -> bool:
        if not self.config.pagination:
            return False

        if self.config.pagination.has_more_path:
            parts = self.config.pagination.has_more_path.split(".")
            value: object = response
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return False
            return bool(value)

        return len(self._extract_data(response)) > 0

    def _get_next_cursor(self, response: dict) -> str | None:
        if not self.config.pagination or not self.config.pagination.cursor_path:
            return None

        parts = self.config.pagination.cursor_path.split(".")
        value: object = response
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return str(value) if value is not None else None

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        session = self._create_session()
        total_items = 0

        try:
            pagination = self.config.pagination
            offset = pagination.initial_offset if pagination else 0
            page = pagination.initial_page if pagination else 1
            cursor: str | None = pagination.initial_cursor if pagination else None

            while True:
                if limit is not None and total_items >= limit:
                    break

                params = dict(self.config.params)

                page_limit = pagination.page_size if pagination else 0
                if pagination is not None and limit is not None:
                    page_limit = min(page_limit, limit - total_items)

                if pagination:
                    if pagination.strategy == "offset":
                        params[pagination.offset_param] = offset
                        params[pagination.limit_param] = page_limit
                    elif pagination.strategy == "page":
                        params[pagination.page_param] = page
                        params[pagination.per_page_param] = page_limit
                    elif pagination.strategy == "cursor" and cursor is not None:
                        params[pagination.cursor_param] = cursor

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

                items = self._extract_data(data)

                batch: list[dict] = []
                for item in items:
                    if limit is not None and total_items >= limit:
                        break
                    batch.append({**self.config.row_annotations, **item})
                    total_items += 1

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

                if batch:
                    yield batch

                if limit is not None and total_items >= limit:
                    break

                if pagination:
                    if pagination.strategy == "offset":
                        if not self._has_more(data):
                            break
                        offset += page_limit
                    elif pagination.strategy == "page":
                        if not self._has_more(data):
                            break
                        page += 1
                    elif pagination.strategy == "cursor":
                        cursor = self._get_next_cursor(data)
                        if not cursor:
                            break
                else:
                    break

        finally:
            session.close()
