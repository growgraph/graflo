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

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    ConnectorUpdate,
    WatermarkConfig,
)
from graflo.connections.sources import ApiAuth
from graflo.data_source.api import APIConfig, APIDataSource
from graflo.data_source.api_response import APIBodyError

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


SLACK_PAGED = {
    "request": {"strategy": "cursor", "cursor_param": "cursor", "page_size": 2},
    "response": {
        "records_path": "messages",
        "cursor_path": "response_metadata.next_cursor",
        "ok_path": "ok",
        "error_path": "error",
    },
}


class TestBodyLevelErrors:
    def test_ok_false_raises_with_the_body_code(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        reply = {"ok": False, "error": "channel_not_found"}
        source = _source(
            serve(lambda query, call: (200, {}, reply)).url, pagination=SLACK_PAGED
        )
        with pytest.raises(APIBodyError, match="channel_not_found") as raised:
            _rows(source)
        assert raised.value.code == "channel_not_found"

    def test_ok_false_on_a_later_page_raises(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        def responder(query: dict[str, list[str]], call: int) -> Reply:
            if "cursor" not in query:
                return (
                    200,
                    {},
                    {
                        "ok": True,
                        "messages": [{"ts": "1"}],
                        "response_metadata": {"next_cursor": "c2"},
                    },
                )
            return 200, {}, {"ok": False, "error": "missing_scope"}

        source = _source(serve(responder).url, pagination=SLACK_PAGED)
        batches = source.iter_batches(batch_size=10)
        assert next(batches) == [{"ts": "1"}]
        with pytest.raises(APIBodyError, match="missing_scope"):
            next(batches)

    def test_ok_true_yields_rows(self, serve: Callable[[Responder], _Server]) -> None:
        reply = {"ok": True, "messages": [{"ts": "1"}, {"ts": "2"}]}
        source = _source(
            serve(lambda query, call: (200, {}, reply)).url, pagination=SLACK_PAGED
        )
        assert _rows(source) == [{"ts": "1"}, {"ts": "2"}]

    def test_missing_ok_flag_raises(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        reply = {"messages": [{"ts": "1"}]}
        source = _source(
            serve(lambda query, call: (200, {}, reply)).url, pagination=SLACK_PAGED
        )
        with pytest.raises(APIBodyError):
            _rows(source)

    def test_error_path_alone_raises_when_set(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        pagination = {
            "request": {"strategy": "offset", "page_size": 2},
            "response": {"records_path": "items", "error_path": "error.code"},
        }
        reply = {"items": [], "error": {"code": "rate_limited"}}
        source = _source(
            serve(lambda query, call: (200, {}, reply)).url, pagination=pagination
        )
        with pytest.raises(APIBodyError, match="rate_limited"):
            _rows(source)

    def test_no_paths_declared_keeps_bodies_unchecked(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        reply = {"ok": False, "items": [{"id": 1}], "more": False}
        source = _source(
            serve(lambda query, call: (200, {}, reply)).url, pagination=PAGED
        )
        assert _rows(source) == [{"id": 1}]


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


JIRA_WATERMARK = {
    "field": "fields.updated",
    "param": "jql",
    "template": "project = SUP AND updated >= '{value}' ORDER BY updated",
}


class TestWatermark:
    def test_an_unset_watermark_leaves_the_connector_hash_alone(self) -> None:
        connector = APIConnector(
            path="/rest/api/2/search", params={"jql": "project = SUP"}
        )
        assert "watermark" not in connector._hash_payload()
        assert (
            connector.hash
            == "b03c99a2b43a5cc0332ed08da21a52314b1799a644038ce0ca40c0414c93a0f9"
        )

    def test_a_watermark_is_part_of_the_connector_hash(self) -> None:
        plain = APIConnector(path="/search")
        marked = APIConnector(path="/search", watermark=JIRA_WATERMARK)
        assert plain.hash != marked.hash

    def test_render_fills_the_template(self) -> None:
        mark = WatermarkConfig.model_validate(JIRA_WATERMARK)
        assert mark.render("2026-10-01 09:00") == {
            "jql": "project = SUP AND updated >= '2026-10-01 09:00' ORDER BY updated"
        }

    def test_render_without_a_template_sends_the_value(self) -> None:
        mark = WatermarkConfig(field="ts", param="oldest")
        assert mark.render("1700000000.000100") == {"oldest": "1700000000.000100"}

    def test_the_connector_hands_its_watermark_to_the_source(self) -> None:
        connector = APIConnector(path="/search", watermark=JIRA_WATERMARK)
        config = connector.build_api_config(base_url="http://api.invalid")
        assert config.watermark == connector.watermark

    def test_no_watermark_means_no_high_water(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        source = _source(serve(lambda query, call: (200, {}, [{"id": 1}])).url)
        _rows(source)
        assert source.high_water is None

    def test_high_water_is_the_largest_value_over_every_page(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        def responder(query: dict[str, list[str]], call: int) -> Reply:
            if query["offset"] == ["0"]:
                items = [
                    {"id": 1, "fields": {"updated": "2026-10-03T08:00:00Z"}},
                    {"id": 2, "fields": {"updated": "2026-10-01T08:00:00Z"}},
                ]
                return 200, {}, {"items": items, "more": True}
            items = [
                {"id": 3, "fields": {"updated": "2026-10-02T08:00:00Z"}},
                {"id": 4, "fields": {}},
            ]
            return 200, {}, {"items": items, "more": False}

        source = _source(
            serve(responder).url,
            pagination=PAGED,
            watermark={"field": "fields.updated", "param": "jql"},
        )
        assert len(_rows(source)) == 4
        assert source.high_water == "2026-10-03T08:00:00Z"

    def test_numeric_values_compare_as_numbers(
        self, serve: Callable[[Responder], _Server]
    ) -> None:
        rows = [{"ts": "9.5"}, {"ts": "10.25"}, {"ts": "10.0"}]
        source = _source(
            serve(lambda query, call: (200, {}, rows)).url,
            watermark={"field": "ts", "param": "oldest"},
        )
        _rows(source)
        assert source.high_water == "10.25"

    def test_the_rendered_value_patches_the_next_run(self) -> None:
        bindings = Bindings.model_validate(
            {
                "connectors": [
                    {
                        "name": "issues",
                        "path": "/search",
                        "params": {"jql": "project = SUP ORDER BY updated"},
                        "watermark": JIRA_WATERMARK,
                    }
                ]
            }
        )
        (connector,) = bindings.connectors
        assert isinstance(connector, APIConnector) and connector.watermark
        bindings.apply_connector_update(
            ConnectorUpdate(
                connector="issues",
                params={
                    **connector.params,
                    **connector.watermark.render("2026-10-03 08:00"),
                },
            )
        )
        (patched,) = bindings.connectors
        assert isinstance(patched, APIConnector)
        assert patched.params["jql"].startswith("project = SUP AND updated >=")
