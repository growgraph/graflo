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
from graflo.data_source.api_response import (
    ResolvedApiResponse,
    extract_records,
    get_batch_metadata,
    has_more_pages,
    next_cursor_value,
    next_offset_value,
)
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
    params: dict[str, Any] = Field(default_factory=dict)
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

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        session = self._create_session()
        total_items = 0
        resolved_response: ResolvedApiResponse | None = None

        try:
            pagination = self.config.pagination
            request = pagination.request if pagination else None
            offset = request.initial_offset if request else 0
            page = request.initial_page if request else 1
            cursor: str | None = request.initial_cursor if request else None

            while True:
                if limit is not None and total_items >= limit:
                    break

                params = dict(self.config.params)

                page_limit = request.page_size if request else 0
                if request is not None and limit is not None:
                    page_limit = min(page_limit, limit - total_items)

                if request is not None:
                    if request.strategy == "offset":
                        params[request.offset_param] = offset
                        params[request.limit_param] = page_limit
                    elif request.strategy == "page":
                        params[request.page_param] = page
                        params[request.per_page_param] = page_limit
                    elif request.strategy == "cursor" and cursor is not None:
                        params[request.cursor_param] = cursor

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

                if pagination is not None and resolved_response is None:
                    resolved_response = ResolvedApiResponse.resolve(
                        pagination.response,
                        data,
                    )

                response_shape = (
                    resolved_response
                    if resolved_response is not None
                    else ResolvedApiResponse()
                )
                items = extract_records(data, response_shape)
                batch_metadata = get_batch_metadata(data, response_shape)

                batch: list[dict] = []
                for item in items:
                    if limit is not None and total_items >= limit:
                        break
                    batch.append(
                        {**self.config.row_annotations, **batch_metadata, **item}
                    )
                    total_items += 1

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

                if batch:
                    yield batch

                if limit is not None and total_items >= limit:
                    break

                if request is None:
                    break

                if not has_more_pages(
                    data,
                    response_shape,
                    items,
                    strategy=request.strategy,
                ):
                    break

                if request.strategy == "offset":
                    server_offset = next_offset_value(data, response_shape)
                    if server_offset is not None:
                        offset = server_offset
                    else:
                        offset += page_limit
                elif request.strategy == "page":
                    page += 1
                elif request.strategy == "cursor":
                    cursor = next_cursor_value(data, response_shape)
                    if not cursor:
                        break

        finally:
            session.close()
