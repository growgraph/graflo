"""APIDataSource against a local HTTP server: credentials, failed pages, rate limits."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from graflo.architecture.contract.bindings import APIConnector
from graflo.connections.sources import ApiAuth
from graflo.data_source.api import APIConfig, APIDataSource

#: What one request gets back: status, headers, JSON body.
Reply = tuple[int, dict[str, str], Any]
Responder = Callable[[dict[str, list[str]], int], Reply]

PAGED = {
    "request": {"strategy": "offset", "page_size": 2},
    "response": {"records_path": "items", "has_more_path": "more"},
}


class _Server:
    def __init__(self, responder: Responder) -> None:
        self.calls = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                owner.calls += 1
                query = parse_qs(urlparse(self.path).query)
                status, headers, body = responder(query, owner.calls)
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                for name, value in headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._httpd.server_address[1]}/items"
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()


@pytest.fixture
def serve() -> Iterator[Callable[[Responder], _Server]]:
    servers: list[_Server] = []

    def start(responder: Responder) -> _Server:
        server = _Server(responder)
        server.start()
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.stop()


def _source(url: str, **config: Any) -> APIDataSource:
    return APIDataSource(config=APIConfig.model_validate({"url": url, **config}))


def _rows(source: APIDataSource) -> list[dict]:
    return [row for batch in source.iter_batches(batch_size=10) for row in batch]


class TestCredentialHeaders:
    @pytest.mark.parametrize("auth_type", ["bearer", "api_key"])
    def test_no_token_sends_no_header(self, auth_type: str) -> None:
        source = _source(
            "http://api.invalid/items",
            auth=ApiAuth.model_validate({"auth_type": auth_type}),
        )
        assert "Authorization" not in source._create_session().headers

    def test_token_is_sent(self) -> None:
        source = _source("http://api.invalid/items", auth=ApiAuth(token="t0k3n"))
        assert source._create_session().headers["Authorization"] == "Bearer t0k3n"


class TestFailedPage:
    def test_failure_on_a_later_page_raises(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        def responder(query: dict[str, list[str]], call: int) -> Reply:
            if query["offset"] == ["0"]:
                return 200, {}, {"items": [{"id": 1}, {"id": 2}], "more": True}
            return 500, {}, {"error": "boom"}

        source = _source(serve(responder).url, pagination=PAGED)
        batches = source.iter_batches(batch_size=10)
        assert next(batches) == [{"id": 1}, {"id": 2}]
        with pytest.raises(requests.HTTPError):
            next(batches)

    def test_failure_on_the_first_page_raises(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        source = _source(serve(lambda query, call: (503, {}, {})).url)
        with pytest.raises(requests.HTTPError):
            _rows(source)


class TestRateLimit:
    def test_429_is_retried_by_default(self) -> None:
        assert 429 in APIConfig(url="http://api.invalid").retry_status_forcelist
        assert 429 in APIConnector(path="/items").retry_status_forcelist

    def test_rate_limited_request_is_repeated(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        def responder(query: dict[str, list[str]], call: int) -> Reply:
            if call == 1:
                return 429, {}, {}
            return 200, {}, [{"id": 1}]

        server = serve(responder)
        source = _source(server.url, retries=2, retry_backoff_factor=0)
        assert _rows(source) == [{"id": 1}]
        assert server.calls == 2

    def test_rate_limit_outlasting_the_retries_raises(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        server = serve(lambda query, call: (429, {}, {}))
        source = _source(server.url, retries=1, retry_backoff_factor=0)
        with pytest.raises(requests.RequestException):
            _rows(source)
        assert server.calls == 2
